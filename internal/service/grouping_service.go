package service

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"go-reconciliation/internal/domain"
	"go-reconciliation/internal/repository/mysql"
	"go-reconciliation/pkg/formatter"
	"go-reconciliation/pkg/stringhelper"
)

type GroupingService struct {
	fileRepo    *mysql.FileRepository
	projectRepo *mysql.ProjectRepository
	extractPath string
	nasHomePath string
}

func NewGroupingService(
	fileRepo *mysql.FileRepository,
	projectRepo *mysql.ProjectRepository,
	extractPath string,
	nasHomePath string,
) *GroupingService {
	return &GroupingService{
		fileRepo:    fileRepo,
		projectRepo: projectRepo,
		extractPath: extractPath,
		nasHomePath: nasHomePath,
	}
}

// ArchiveFileOnError moves an invalid/failed file to the NAS archive folder and logs the error to DB
func (s *GroupingService) ArchiveFileOnError(ctx context.Context, file *domain.ScannedFile, vp *domain.VendorProject, status int, errMsg string, dates []string) {
	log.Printf("[Archive File] Status: %d, File: %s, Error: %s", status, file.Filename, errMsg)

	archiveFolder, ok := domain.FolderNameStatuses[status]
	if !ok {
		archiveFolder = "FAILED_FILES"
	}

	archivePath := stringhelper.JoinPaths(s.nasHomePath, vp.RelativePath, archiveFolder)
	_ = os.MkdirAll(archivePath, 0755)

	destFile := filepath.Join(archivePath, file.CleanFilename)
	_ = os.Rename(file.FullPath, destFile)

	// Save or update FileCheckHistory with error status
	fchID := uuid.New().String()
	trxType := file.Type
	fch := &domain.FileCheckHistory{
		ID:        fchID,
		ProjectID: vp.ID,
		VendorID:  vp.VendorID,
		Filename:  file.CleanFilename,
		MD5:       file.MD5,
		Date:      file.Date,
		Type:      &trxType,
		Path:      vp.RelativePath,
		Status:    status,
		Message:   &errMsg,
		Total:     0,
	}
	_ = s.fileRepo.UpsertFileCheckHistory(ctx, fch)

	// Record to file_error_logs
	if len(dates) == 0 {
		dates = []string{file.Date}
	}
	for _, d := range dates {
		_ = s.fileRepo.RecordFileErrorLog(ctx, fchID, vp.ID, vp.VendorID, trxType, "csv", file.CleanFilename, d, errMsg, status)
	}
}

// ProcessCsvFile parses a raw CSV file and groups its content into date-partitioned files
func (s *GroupingService) ProcessCsvFile(ctx context.Context, file *domain.ScannedFile, vp *domain.VendorProject, customProfiles map[string]map[string]map[string]string) ([]string, error) {
	log.Printf("[Grouping] Processing CSV: %s for Project: %s (Vendor: %s)", file.Filename, vp.ProjectName, vp.VendorPath)

	// 1. Check file existence and empty check
	fileInfo, err := os.Stat(file.FullPath)
	if err != nil || os.IsNotExist(err) {
		s.ArchiveFileOnError(ctx, file, vp, domain.FileHistoryNotFound, fmt.Sprintf("File not found: %s", file.Filename), nil)
		return nil, fmt.Errorf("file not found: %s", file.Filename)
	}

	if fileInfo.Size() < 1 {
		s.ArchiveFileOnError(ctx, file, vp, domain.FileHistoryDataEmpty, fmt.Sprintf("Empty file: %s", file.Filename), nil)
		return nil, fmt.Errorf("empty file: %s", file.Filename)
	}

	// 2. Validate filename date & prefix
	if file.Date == "" {
		s.ArchiveFileOnError(ctx, file, vp, domain.FileHistoryFilenameInvalid, fmt.Sprintf("Missing date in filename: %s", file.Filename), nil)
		return nil, fmt.Errorf("missing date in filename: %s", file.Filename)
	}

	if file.Type != domain.TrxTypePaid && file.Type != domain.TrxTypePayout {
		s.ArchiveFileOnError(ctx, file, vp, domain.FileHistoryFilenameInvalid, fmt.Sprintf("Filename prefix invalid (expected paid/payout): %s", file.Filename), nil)
		return nil, fmt.Errorf("invalid transaction type for %s", file.Filename)
	}

	f, err := os.Open(file.FullPath)
	if err != nil {
		s.ArchiveFileOnError(ctx, file, vp, domain.FileHistoryFailed, fmt.Sprintf("Open file failed: %v", err), nil)
		return nil, fmt.Errorf("open file failed: %w", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	buf := make([]byte, 64*1024)
	scanner.Buffer(buf, 10*1024*1024)

	// 3. Read first line to detect delimiter
	if !scanner.Scan() {
		s.ArchiveFileOnError(ctx, file, vp, domain.FileHistoryDataEmpty, fmt.Sprintf("Empty file content: %s", file.Filename), nil)
		return nil, fmt.Errorf("empty file: %s", file.Filename)
	}
	firstLine := scanner.Text()
	delimiter := stringhelper.GetDelimiter(firstLine)
	if delimiter == "" {
		s.ArchiveFileOnError(ctx, file, vp, domain.FileHistorySeparatorInvalid, fmt.Sprintf("Cannot detect separator for: %s", file.Filename), nil)
		return nil, fmt.Errorf("cannot detect delimiter for %s", file.Filename)
	}

	// 4. Parse Header
	headerReader := csv.NewReader(strings.NewReader(firstLine))
	headerReader.Comma = []rune(delimiter)[0]
	headerReader.LazyQuotes = true
	headerFields, err := headerReader.Read()
	if err != nil {
		s.ArchiveFileOnError(ctx, file, vp, domain.FileHistorySeparatorInvalid, fmt.Sprintf("Parse header failed: %v", err), nil)
		return nil, fmt.Errorf("parse header failed: %w", err)
	}

	// Normalize header names
	var cleanHeaders []string
	headerMap := make(map[string]int)
	for i, h := range headerFields {
		clean := formatter.ConvertToColumnName(h)
		cleanHeaders = append(cleanHeaders, clean)
		headerMap[clean] = i
	}

	createdAtIdx, hasCreatedAt := headerMap[domain.ColCreatedAt]
	refNoIdx, hasRefNo := headerMap[domain.ColReferenceNo]

	if !hasCreatedAt || !hasRefNo {
		s.ArchiveFileOnError(ctx, file, vp, domain.FileHistoryColumnMismatch, fmt.Sprintf("Missing created_at or reference_no in header: %v", cleanHeaders), nil)
		return nil, fmt.Errorf("missing created_at or reference_no in header: %v", cleanHeaders)
	}

	// 5. Process records into date partitions in memory
	partitions := make(map[string]map[string][]string)
	totalRows := 0
	var detectedDates []string

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		r := csv.NewReader(strings.NewReader(line))
		r.Comma = []rune(delimiter)[0]
		r.LazyQuotes = true
		record, err := r.Read()
		if err != nil || len(record) != len(headerFields) {
			continue
		}

		for i := range record {
			record[i] = stringhelper.RemoveTrailingQuotes(stringhelper.RemoveEqualSign(record[i]))
		}

		refNo := record[refNoIdx]
		rawCreatedAt := record[createdAtIdx]
		dateOnly := formatter.ConvertToDateOnly(rawCreatedAt)
		if dateOnly == "" {
			continue
		}

		if _, ok := partitions[dateOnly]; !ok {
			partitions[dateOnly] = make(map[string][]string)
			detectedDates = append(detectedDates, dateOnly)
		}
		partitions[dateOnly][refNo] = record
		totalRows++
	}

	if totalRows == 0 {
		s.ArchiveFileOnError(ctx, file, vp, domain.FileHistoryDataEmpty, fmt.Sprintf("No valid transaction rows found in: %s", file.Filename), detectedDates)
		return nil, fmt.Errorf("no valid transaction rows found in: %s", file.Filename)
	}

	// 6. Check transaction movements (filter out references already moved)
	movedMap, err := s.fileRepo.FetchMovedTransactions(ctx, vp.ID, vp.VendorID, file.Type, detectedDates)
	if err == nil && len(movedMap) > 0 {
		for date, rows := range partitions {
			if movedRefs, ok := movedMap[date]; ok {
				for ref := range movedRefs {
					delete(rows, ref)
				}
			}
		}
	}

	// 7. Save FileCheckHistory record
	fchID := uuid.New().String()
	tableColsJson, _ := json.Marshal(cleanHeaders)
	csvColsJson, _ := json.Marshal(headerFields)
	trxType := file.Type

	fch := &domain.FileCheckHistory{
		ID:           fchID,
		ProjectID:    vp.ID,
		VendorID:     vp.VendorID,
		Filename:     file.CleanFilename,
		MD5:          file.MD5,
		Date:         file.Date,
		Type:         &trxType,
		Path:         vp.RelativePath,
		Status:       domain.FileHistoryCompleted,
		TableColumns: string(tableColsJson),
		CsvColumns:   string(csvColsJson),
		Total:        totalRows,
	}

	// 8. Write partitioned CSV files to extract storage
	var distinctDates []string
	var details []*domain.FileHistoryDetail

	for date, rows := range partitions {
		if len(rows) == 0 {
			continue
		}
		distinctDates = append(distinctDates, date)

		storageRelPath := stringhelper.JoinPaths(s.extractPath, vp.ProjectPath, date, trxType)
		_ = os.MkdirAll(storageRelPath, 0755)

		outputFilename := fmt.Sprintf("%s_%s.csv", vp.VendorPath, file.MD5)
		outputPath := filepath.Join(storageRelPath, outputFilename)

		outFile, err := os.Create(outputPath)
		if err != nil {
			log.Printf("[Error] Failed to create extract file %s: %v", outputPath, err)
			continue
		}

		writer := bufio.NewWriter(outFile)
		writer.WriteString(strings.Join(cleanHeaders, ";") + "\n")

		for _, row := range rows {
			writer.WriteString(strings.Join(row, ";") + "\n")
		}
		writer.Flush()
		outFile.Close()

		details = append(details, &domain.FileHistoryDetail{
			ID:            uuid.New().String(),
			FileHistoryID: fchID,
			ProjectID:     vp.ID,
			VendorID:      vp.VendorID,
			Date:          date,
			Type:          trxType,
			Path:          storageRelPath,
			Filename:      outputFilename,
			Total:         len(rows),
		})
	}

	// 9. Persist records to MySQL
	_ = s.fileRepo.UpsertFileCheckHistory(ctx, fch)
	_ = s.fileRepo.SaveFileHistoryDetails(ctx, details)
	_ = s.fileRepo.ResolveFileErrorLogs(ctx, fchID)

	// 10. Move processed raw file to date directory on NAS
	newNasDir := stringhelper.JoinPaths(file.NasPath, file.Date)
	_ = os.MkdirAll(newNasDir, 0755)
	newNasFile := filepath.Join(newNasDir, file.CleanFilename)
	_ = os.Rename(file.FullPath, newNasFile)

	log.Printf("[Grouping Success] File %s partitioned into %d dates (%d total rows)", file.Filename, len(distinctDates), totalRows)
	return distinctDates, nil
}
