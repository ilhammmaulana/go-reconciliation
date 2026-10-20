package reconcile

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"go-reconciliation/internal/config"
	"go-reconciliation/internal/db"
	"go-reconciliation/internal/logger"
	"go-reconciliation/internal/queue"
)

// Status constants matching App\Constants\Statuses
const (
	StatusWaiting    = 0
	StatusOnProgress = 1
	StatusFailed     = 2
	StatusCompleted  = 3
	StatusSkipped    = 13

	FileHistoryCompleted = 3

	CashIn  = "IN"
	CashOut = "OUT"
	Paid    = "paid"
	Payout  = "payout"

	ColReferenceNo       = "reference_no"
	ColParentReferenceNo = "parent_reference_no"
	ColVendorRefNo       = "vendor_reference_no"
	ColAmount            = "amount"
	ColFee               = "fee"
	ColTotalAmount       = "total_amount"
	ColPaymentAmount     = "payment_amount"
	ColInvoiceAmount     = "invoice_amount"
	ColStatus            = "status"
	ColCreatedAt         = "created_at"
	ColPaidAt            = "paid_at"
	ColPayoutAt          = "payout_at"
	ColPaymentMethod     = "payment_method"

	MaxInsertBatch = 5000
)

// Service handles the complete reconciliation pipeline equivalent to ReconciliationServiceV2.php
type Service struct {
	db     *db.DB
	config *config.Config
}

// New creates a new reconcile Service
func New(database *db.DB, cfg *config.Config) *Service {
	return &Service{
		db:     database,
		config: cfg,
	}
}

// Handle processes a ReconcileJob payload dispatched by Laravel
func (s *Service) Handle(ctx context.Context, payload *queue.ReconcileJobPayload) error {
	logger.Info.Printf("[RECONCILE] Starting job UUID=%s projectId=%s date=%s type=%s",
		payload.JobUUID, payload.ProjectID, payload.TargetDate, payload.TargetType)

	lockName := s.buildLockName(payload)
	locked, err := s.acquireLock(ctx, lockName, 1800)
	if err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}
	if !locked {
		logger.Info.Printf("[RECONCILE SKIP] Another process holds lock: %s", lockName)
		return nil
	}
	defer s.releaseLock(ctx, lockName)

	processed, err := s.reconcile(ctx, payload)
	if err != nil {
		s.markReconciliationFailed(ctx, payload, err.Error())
		return err
	}

	logger.Info.Printf("[RECONCILE] Completed projectId=%s processed=%d", payload.ProjectID, processed)
	runtime.GC()
	return nil
}

type fileDetailItem struct {
	ID                  string
	FileHistoryID       string
	ProjectID           string
	VendorID            string
	Date                string
	Type                string
	Path                string
	Filename            string
	Total               int
	FileCheckHistoryID  string
	CustomProfileID     sql.NullString
	FileHistoryCreateAt sql.NullTime
}

type vendorInfo struct {
	ID                    string
	Name                  string
	Path                  string
	IsInternal            bool
	IsAggregator          bool
	SplitTransactionRuleID sql.NullString
}

type projectInfo struct {
	ID          string
	ProjectPath string
	ProjectName string
	VendorID    string
}

type profileColumn struct {
	Name  string `json:"name"`
	Type  string `json:"type"` // reference, compare, column
	Label string `json:"label"`
}

type toleranceSetting struct {
	MaskingTolerance        map[string]float64
	MaskingTolerancePercent map[string]float64
}

// reconcile runs the complete reconciliation matching logic identically to ReconciliationServiceV2
func (s *Service) reconcile(ctx context.Context, payload *queue.ReconcileJobPayload) (int, error) {
	// Step 1: Find candidate file_history_details
	query := `
		SELECT fhd.id, fhd.file_history_id, fhd.project_id, fhd.vendor_id, fhd.date, fhd.type, fhd.path, fhd.filename, fhd.total
		FROM file_history_details fhd
		INNER JOIN file_check_histories fh ON fhd.file_history_id = fh.id
		WHERE fhd.is_reconciled = 0
		  AND fh.status = ?
		  AND fhd.project_id = ?
	`
	args := []interface{}{FileHistoryCompleted, payload.ProjectID}

	if payload.TargetDate != "" {
		query += " AND fhd.date = ?"
		args = append(args, payload.TargetDate)
	}
	if payload.TargetType != "" {
		query += " AND fhd.type = ?"
		args = append(args, payload.TargetType)
	} else {
		// Exclude in-progress partitions
		inProgRows, err := s.db.QueryContext(ctx,
			"SELECT date, recon_type FROM reconciliations WHERE project_id = ? AND status = ?",
			payload.ProjectID, StatusOnProgress,
		)
		if err == nil {
			defer inProgRows.Close()
			for inProgRows.Next() {
				var d, rt string
				if inProgRows.Scan(&d, &rt) == nil {
					t := Paid
					if rt == CashOut {
						t = Payout
					}
					query += " AND NOT (fhd.date = ? AND fhd.type = ?)"
					args = append(args, d[:10], t)
				}
			}
		}
	}

	query += " ORDER BY fhd.id ASC LIMIT 1"

	row := s.db.QueryRowContext(ctx, query, args...)
	var candidate fileDetailItem
	err := row.Scan(
		&candidate.ID, &candidate.FileHistoryID, &candidate.ProjectID,
		&candidate.VendorID, &candidate.Date, &candidate.Type,
		&candidate.Path, &candidate.Filename, &candidate.Total,
	)
	if err == sql.ErrNoRows {
		logger.Info.Printf("[RECONCILE] No pending files found for projectId: %s", payload.ProjectID)
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("error finding file_history_detail: %w", err)
	}

	reconType := CashIn
	if candidate.Type == Payout {
		reconType = CashOut
	}

	// Step 2: Check if already on progress
	var onProgCount int
	s.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM reconciliations WHERE status = ? AND project_id = ? AND date = ? AND recon_type = ?",
		StatusOnProgress, candidate.ProjectID, candidate.Date, reconType,
	).Scan(&onProgCount)
	if onProgCount > 0 {
		logger.Info.Printf("[RECONCILE SKIP] Already on progress for project=%s date=%s type=%s",
			candidate.ProjectID, candidate.Date, reconType)
		return 0, nil
	}

	// Step 3: Fetch all file_history_details for this partition
	filesQuery := `
		SELECT fhd.id, fhd.file_history_id, fhd.project_id, fhd.vendor_id, fhd.date, fhd.type, fhd.path, fhd.filename, fhd.total,
		       fh.custom_profile_id, fh.created_at
		FROM file_history_details fhd
		INNER JOIN file_check_histories fh ON fhd.file_history_id = fh.id
		WHERE fh.status = ?
		  AND fhd.project_id = ?
		  AND fhd.path = ?
		  AND fhd.date = ?
		  AND fhd.type = ?
		ORDER BY fh.created_at DESC
	`
	fRows, err := s.db.QueryContext(ctx, filesQuery,
		FileHistoryCompleted, candidate.ProjectID, candidate.Path, candidate.Date, candidate.Type,
	)
	if err != nil {
		return 0, fmt.Errorf("error querying files: %w", err)
	}
	defer fRows.Close()

	var validFiles []fileDetailItem
	var fhdIDs []string
	for fRows.Next() {
		var f fileDetailItem
		if err := fRows.Scan(
			&f.ID, &f.FileHistoryID, &f.ProjectID, &f.VendorID, &f.Date,
			&f.Type, &f.Path, &f.Filename, &f.Total, &f.CustomProfileID, &f.FileHistoryCreateAt,
		); err != nil {
			continue
		}

		fullFilePath := filepath.Join(s.config.StoragePath, f.Path, f.Filename)
		if _, statErr := os.Stat(fullFilePath); os.IsNotExist(statErr) {
			logger.Warn.Printf("[FILE NOT FOUND] Deleting missing file record: %s", fullFilePath)
			s.db.ExecContext(ctx, "DELETE FROM file_history_details WHERE id = ?", f.ID)
			continue
		}

		validFiles = append(validFiles, f)
		fhdIDs = append(fhdIDs, f.ID)
	}

	if len(validFiles) == 0 {
		logger.Warn.Printf("[RECONCILE] No valid files on disk for date=%s type=%s", candidate.Date, candidate.Type)
		return 0, nil
	}

	// Step 4: Load project info
	project, err := s.loadProject(ctx, candidate.ProjectID)
	if err != nil {
		return 0, err
	}

	// Step 5: Upsert reconciliation record to onprogress
	reconciliationID, err := s.initReconciliationRecord(ctx, project, candidate.Date, reconType, validFiles[0].FileHistoryID)
	if err != nil {
		return 0, fmt.Errorf("failed to initialize reconciliation record: %w", err)
	}

	// Step 6: Load Split Transaction Rules
	splitRules := s.loadSplitRules(ctx)

	// Step 7: Load Tolerances
	tolerance := s.loadTolerances(ctx)

	// Step 8: Parse CSV files and build vendor data in memory
	vendorsData := make(map[string]map[string]map[string]string) // vendor_id -> reference_no -> row map
	vendorCols := make(map[string][]string)
	profileCompareCols := make([]string, 0)
	var vendorsID []string
	vendorsMap := make(map[string]*vendorInfo)

	for _, file := range validFiles {
		vInfo, err := s.loadVendor(ctx, file.VendorID)
		if err == nil {
			vendorsMap[file.VendorID] = vInfo
		}

		// Read custom profile
		var profileCols []profileColumn
		if file.CustomProfileID.Valid && file.CustomProfileID.String != "" {
			profileCols = s.loadProfileColumns(ctx, file.CustomProfileID.String, candidate.Type)
			for _, pc := range profileCols {
				if pc.Type == "compare" && !contains(profileCompareCols, pc.Name) {
					profileCompareCols = append(profileCompareCols, pc.Name)
				}
			}
		}

		// Parse CSV
		csvPath := filepath.Join(s.config.StoragePath, file.Path, file.Filename)
		records, cols, err := s.readCSVFile(csvPath)
		if err != nil {
			logger.Error.Printf("[CSV PARSE ERROR] %s: %v", csvPath, err)
			continue
		}

		if _, exists := vendorsData[file.VendorID]; !exists {
			vendorsData[file.VendorID] = make(map[string]map[string]string)
			vendorsID = append(vendorsID, file.VendorID)
		}
		vendorCols[file.VendorID] = uniqueStrings(append(vendorCols[file.VendorID], cols...))

		// Aggregate transactions
		for _, rec := range records {
			refNo := rec[ColReferenceNo]
			if refNo == "" {
				continue
			}

			// Check split transaction
			parentRef := ""
			if vInfo != nil && vInfo.SplitTransactionRuleID.Valid {
				if pattern, ok := splitRules[vInfo.SplitTransactionRuleID.String]; ok {
					parentRef = extractSubRef(refNo, pattern)
				}
			}

			targetRef := refNo
			if parentRef != "" {
				targetRef = parentRef
			}

			if existing, ok := vendorsData[file.VendorID][targetRef]; !ok {
				vendorsData[file.VendorID][targetRef] = rec
			} else {
				// Keep newest by created_at if duplicate
				if rec[ColCreatedAt] > existing[ColCreatedAt] {
					vendorsData[file.VendorID][targetRef] = rec
				}
			}
		}
	}

	// Collect all distinct columns across all vendors
	var allColumns []string
	for _, cols := range vendorCols {
		allColumns = uniqueStrings(append(allColumns, cols...))
	}

	// Normalize rows: ensure each row has all keys
	for vid := range vendorsData {
		for ref, rec := range vendorsData[vid] {
			for _, c := range allColumns {
				if _, ok := rec[c]; !ok {
					rec[c] = ""
				}
			}
			vendorsData[vid][ref] = rec
		}
	}

	// Step 9: Compare data across vendors
	diffMap := make(map[string][]string)      // ref -> list of diff columns
	notExistsMap := make(map[string][]string)  // ref -> list of vendor IDs where missing
	matchList := make([]string, 0)
	diffList := make([]string, 0)
	notExistsList := make([]string, 0)

	if len(vendorsID) == 1 {
		// Single vendor: all records considered unmatched / not exists in other vendors
		v0 := vendorsID[0]
		for ref := range vendorsData[v0] {
			notExistsList = append(notExistsList, ref)
		}
	} else if len(vendorsID) > 1 {
		// Pairwise compare
		for i := 0; i < len(vendorsID)-1; i++ {
			for j := i + 1; j < len(vendorsID); j++ {
				v1 := vendorsID[i]
				v2 := vendorsID[j]

				arr1 := vendorsData[v1]
				arr2 := vendorsData[v2]

				for ref, row1 := range arr1 {
					row2, ok := arr2[ref]
					if !ok {
						notExistsMap[ref] = uniqueStrings(append(notExistsMap[ref], v2))
						if !contains(notExistsList, ref) {
							notExistsList = append(notExistsList, ref)
						}
						continue
					}

					var rowDiffs []string
					for _, col := range profileCompareCols {
						if isDifferent(col, row1[col], row2[col], tolerance) {
							rowDiffs = append(rowDiffs, col)
						}
					}

					if len(rowDiffs) > 0 {
						diffMap[ref] = uniqueStrings(append(diffMap[ref], rowDiffs...))
						if !contains(diffList, ref) {
							diffList = append(diffList, ref)
						}
					} else {
						if !contains(matchList, ref) && !contains(diffList, ref) {
							matchList = append(matchList, ref)
						}
					}
				}

				// Check items in arr2 but not in arr1
				for ref := range arr2 {
					if _, ok := arr1[ref]; !ok {
						notExistsMap[ref] = uniqueStrings(append(notExistsMap[ref], v1))
						if !contains(notExistsList, ref) {
							notExistsList = append(notExistsList, ref)
						}
					}
				}
			}
		}
	}

	// Step 10: Compute vendor statistics & save VendorSummary
	summaryVendorsJSON := make([]map[string]interface{}, 0)
	for vid, vRows := range vendorsData {
		var sumAmount, sumFee, sumTotal int64
		statusGroups := make(map[string]map[string]int64)

		for _, r := range vRows {
			amt, _ := strconv.ParseInt(r[ColAmount], 10, 64)
			fee, _ := strconv.ParseInt(r[ColFee], 10, 64)
			tot, _ := strconv.ParseInt(r[ColTotalAmount], 10, 64)

			sumAmount += amt
			sumFee += fee
			sumTotal += tot

			st := strings.ToLower(r[ColStatus])
			if st == "" {
				st = "unknown"
			}
			if _, ok := statusGroups[st]; !ok {
				statusGroups[st] = map[string]int64{"entries": 0, "amount": 0, "fee": 0, "total": 0}
			}
			statusGroups[st]["entries"]++
			statusGroups[st]["amount"] += amt
			statusGroups[st]["fee"] += fee
			statusGroups[st]["total"] += tot
		}

		// Delete old VendorSummary
		s.db.ExecContext(ctx, `
			DELETE FROM vendor_summaries
			WHERE project_id = ? AND vendor_id = ? AND type = ? AND date = ?
		`, project.ID, vid, candidate.Type, candidate.Date)

		// Insert VendorSummary per status
		vInfo := vendorsMap[vid]
		isAgg := 0
		if vInfo != nil && vInfo.IsAggregator {
			isAgg = 1
		}

		for st, grp := range statusGroups {
			s.db.ExecContext(ctx, `
				INSERT INTO vendor_summaries (
					id, project_id, vendor_id, type, total_entries, sum_amount, sum_fee, sum_total_amount,
					date, is_aggregator, status, created_at, updated_at
				) VALUES (UUID(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
			`, project.ID, vid, candidate.Type, grp["entries"], grp["amount"], grp["fee"], grp["total"],
				candidate.Date, isAgg, st,
			)
		}

		summaryVendorsJSON = append(summaryVendorsJSON, map[string]interface{}{
			"vendor_id":        vid,
			"total_entries":    len(vRows),
			"sum_amount":       sumAmount,
			"sum_fee":          sumFee,
			"sum_total_amount": sumTotal,
		})

		// Step 11: Create vendor partition table & insert records
		vendorPath := "vendor"
		if vInfo != nil && vInfo.Path != "" {
			vendorPath = vInfo.Path
		}
		tableVendor := s.buildVendorTableName(candidate.Type, project.ProjectPath, vendorPath, candidate.Date)
		s.recreateDynamicTable(ctx, tableVendor, allColumns, false)
		s.bulkInsertMap(ctx, tableVendor, allColumns, vRows, diffMap, notExistsMap, matchList, false)
	}

	// Step 12: Build merge_all records & insert into main partition table
	mergeAll := make(map[string]map[string]string)
	// Priority 1: Main vendor / project owner
	if mainRows, ok := vendorsData[project.VendorID]; ok {
		for ref, r := range mainRows {
			mergeAll[ref] = r
		}
	}
	// Fill rest from other vendors
	for _, vRows := range vendorsData {
		for ref, r := range vRows {
			if _, ok := mergeAll[ref]; !ok {
				mergeAll[ref] = r
			}
		}
	}

	mainTable := s.buildMainTableName(candidate.Type, project.ProjectPath, candidate.Date)
	s.recreateDynamicTable(ctx, mainTable, allColumns, true)
	s.bulkInsertMap(ctx, mainTable, allColumns, mergeAll, diffMap, notExistsMap, matchList, true)

	// Step 13: Update Reconciliation record with final summaries
	sumVendorsRaw, _ := json.Marshal(summaryVendorsJSON)
	totalEntries := len(mergeAll)
	totalMatch := len(matchList)
	totalDiff := len(diffList)
	totalEmpty := len(notExistsList)

	s.db.ExecContext(ctx, `
		UPDATE reconciliations SET
			status = ?,
			total = ?,
			total_match = ?,
			total_differences = ?,
			total_differences_empty = ?,
			total_vendors = ?,
			sum_amount_vendors = ?,
			message = NULL,
			updated_at = NOW()
		WHERE id = ?
	`, StatusCompleted, totalEntries, totalMatch, totalDiff, totalEmpty, len(vendorsID), string(sumVendorsRaw), reconciliationID)

	// Step 14: Mark file_history_details as reconciled
	for _, id := range fhdIDs {
		s.db.ExecContext(ctx, "UPDATE file_history_details SET is_reconciled = 1, updated_at = NOW() WHERE id = ?", id)
	}

	// Step 15: Update reconcile_histories
	for _, f := range validFiles {
		var exists int
		s.db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM reconcile_histories WHERE reconciliation_id = ? AND file_check_history_id = ?",
			reconciliationID, f.FileHistoryID,
		).Scan(&exists)

		if exists > 0 {
			s.db.ExecContext(ctx, `
				UPDATE reconcile_histories SET
					file_updated_at = ?,
					status = ?,
					updated_at = NOW()
				WHERE reconciliation_id = ? AND file_check_history_id = ?
			`, f.FileHistoryCreateAt, FileHistoryCompleted, reconciliationID, f.FileHistoryID)
		} else {
			s.db.ExecContext(ctx, `
				INSERT INTO reconcile_histories (
					id, reconciliation_id, file_check_history_id, file_updated_at, status, created_at, updated_at
				) VALUES (UUID(), ?, ?, ?, ?, NOW(), NOW())
			`, reconciliationID, f.FileHistoryID, f.FileHistoryCreateAt, FileHistoryCompleted)
		}
	}

	return totalEntries, nil
}

// -----------------------------------------------------------------------
// Database Helpers & Schema Generation
// -----------------------------------------------------------------------

func (s *Service) recreateDynamicTable(ctx context.Context, tableName string, columns []string, isMain bool) error {
	s.db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS `%s`", tableName))

	var colDefs []string
	colDefs = append(colDefs, "`id` CHAR(36) NOT NULL PRIMARY KEY")

	for _, c := range columns {
		switch c {
		case ColReferenceNo, ColParentReferenceNo:
			colDefs = append(colDefs, fmt.Sprintf("`%s` VARCHAR(64) NULL, INDEX idx_%s (`%s`)", c, c, c))
		case ColVendorRefNo:
			colDefs = append(colDefs, fmt.Sprintf("`%s` VARCHAR(64) NULL", c))
		case ColAmount, ColTotalAmount, ColFee, ColPaymentAmount, ColInvoiceAmount:
			colDefs = append(colDefs, fmt.Sprintf("`%s` BIGINT NULL DEFAULT 0", c))
		case ColCreatedAt, ColPaidAt, ColPayoutAt:
			colDefs = append(colDefs, fmt.Sprintf("`%s` TIMESTAMP NULL, INDEX idx_%s (`%s`)", c, c, c))
		case ColStatus:
			colDefs = append(colDefs, fmt.Sprintf("`%s` VARCHAR(32) NULL, INDEX idx_%s (`%s`)", c, c, c))
		case ColPaymentMethod:
			colDefs = append(colDefs, fmt.Sprintf("`%s` VARCHAR(64) NULL, INDEX idx_%s (`%s`)", c, c, c))
		default:
			colDefs = append(colDefs, fmt.Sprintf("`%s` VARCHAR(255) NULL", c))
		}
	}

	colDefs = append(colDefs, "`_diff_` JSON NULL")
	colDefs = append(colDefs, "`_not_exists_` JSON NULL")
	colDefs = append(colDefs, "`_match_` JSON NULL")
	if isMain {
		colDefs = append(colDefs, "`_split_transactions_` JSON NULL")
	}

	ddl := fmt.Sprintf(
		"CREATE TABLE `%s` (%s) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci",
		tableName, strings.Join(colDefs, ", "),
	)
	_, err := s.db.ExecContext(ctx, ddl)
	return err
}

func (s *Service) bulkInsertMap(
	ctx context.Context,
	tableName string,
	columns []string,
	rows map[string]map[string]string,
	diffMap map[string][]string,
	notExistsMap map[string][]string,
	matchList []string,
	isMain bool,
) {
	if len(rows) == 0 {
		return
	}

	insertCols := append([]string{"id"}, columns...)
	insertCols = append(insertCols, "_diff_", "_not_exists_", "_match_")
	if isMain {
		insertCols = append(insertCols, "_split_transactions_")
	}

	chunkRows := make([][]interface{}, 0, MaxInsertBatch)
	for ref, r := range rows {
		rowVals := make([]interface{}, 0, len(insertCols))
		rowVals = append(rowVals, generateUUID())

		for _, col := range columns {
			val := r[col]
			if val == "" {
				rowVals = append(rowVals, nil)
			} else {
				rowVals = append(rowVals, val)
			}
		}

		// JSON metadata
		diffRaw, _ := json.Marshal(diffMap[ref])
		notExistsRaw, _ := json.Marshal(notExistsMap[ref])
		matchRaw := "[]"
		if contains(matchList, ref) {
			mBytes, _ := json.Marshal([]string{"matched"})
			matchRaw = string(mBytes)
		}

		rowVals = append(rowVals, string(diffRaw))
		rowVals = append(rowVals, string(notExistsRaw))
		rowVals = append(rowVals, string(matchRaw))
		if isMain {
			rowVals = append(rowVals, "[]")
		}

		chunkRows = append(chunkRows, rowVals)

		if len(chunkRows) >= MaxInsertBatch {
			s.db.BulkInsert(tableName, insertCols, chunkRows)
			chunkRows = chunkRows[:0]
		}
	}

	if len(chunkRows) > 0 {
		s.db.BulkInsert(tableName, insertCols, chunkRows)
	}
}

// -----------------------------------------------------------------------
// CSV Reader
// -----------------------------------------------------------------------

func (s *Service) readCSVFile(path string) ([]map[string]string, []string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.Comma = ';'
	reader.LazyQuotes = true
	reader.FieldsPerRecord = -1

	header, err := reader.Read()
	if err != nil {
		return nil, nil, fmt.Errorf("empty or invalid header: %w", err)
	}

	// Clean headers
	cols := make([]string, len(header))
	for i, h := range header {
		cols[i] = strings.TrimSpace(strings.ToLower(h))
	}

	var records []map[string]string
	for {
		rec, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}
		rowMap := make(map[string]string)
		for i, c := range cols {
			if i < len(rec) {
				rowMap[c] = strings.TrimSpace(rec[i])
			} else {
				rowMap[c] = ""
			}
		}
		records = append(records, rowMap)
	}

	return records, cols, nil
}

// -----------------------------------------------------------------------
// Profile & Tolerance Loaders
// -----------------------------------------------------------------------

func (s *Service) loadProfileColumns(ctx context.Context, profileID, fType string) []profileColumn {
	colField := "paid_columns"
	if fType == Payout {
		colField = "payout_columns"
	}

	var rawJSON sql.NullString
	s.db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT %s FROM custom_profile_mapings WHERE id = ?", colField),
		profileID,
	).Scan(&rawJSON)

	if !rawJSON.Valid || rawJSON.String == "" {
		return nil
	}

	var cols []profileColumn
	_ = json.Unmarshal([]byte(rawJSON.String), &cols)
	return cols
}

func (s *Service) loadSplitRules(ctx context.Context) map[string]string {
	rules := make(map[string]string)
	rows, err := s.db.QueryContext(ctx, "SELECT id, pattern FROM split_transaction_rules")
	if err != nil {
		return rules
	}
	defer rows.Close()

	for rows.Next() {
		var id, pat string
		if rows.Scan(&id, &pat) == nil {
			rules[id] = pat
		}
	}
	return rules
}

func (s *Service) loadTolerances(ctx context.Context) *toleranceSetting {
	ts := &toleranceSetting{
		MaskingTolerance:        map[string]float64{"created_at": 10, "paid_at": 180, "payout_at": 180, "amount": 1000, "total_amount": 1000},
		MaskingTolerancePercent: map[string]float64{"amount": 0, "total_amount": 0},
	}

	var maskRaw, pctRaw sql.NullString
	s.db.QueryRowContext(ctx, "SELECT masking_tolerance, masking_tolerance_percent FROM reconciliation_settings LIMIT 1").Scan(&maskRaw, &pctRaw)

	if maskRaw.Valid && maskRaw.String != "" {
		var m map[string]float64
		if json.Unmarshal([]byte(maskRaw.String), &m) == nil {
			for k, v := range m {
				ts.MaskingTolerance[k] = v
			}
		}
	}
	if pctRaw.Valid && pctRaw.String != "" {
		var m map[string]float64
		if json.Unmarshal([]byte(pctRaw.String), &m) == nil {
			for k, v := range m {
				ts.MaskingTolerancePercent[k] = v
			}
		}
	}

	return ts
}

func (s *Service) loadProject(ctx context.Context, projectID string) (*projectInfo, error) {
	p := &projectInfo{}
	err := s.db.QueryRowContext(ctx,
		"SELECT id, project_path, project_name, vendor_id FROM vendor_projects WHERE id = ?",
		projectID,
	).Scan(&p.ID, &p.ProjectPath, &p.ProjectName, &p.VendorID)
	return p, err
}

func (s *Service) loadVendor(ctx context.Context, vendorID string) (*vendorInfo, error) {
	v := &vendorInfo{}
	var isInt, isAgg int
	err := s.db.QueryRowContext(ctx,
		"SELECT id, name, path, is_internal, is_aggregator, split_transaction_rule_id FROM vendors WHERE id = ?",
		vendorID,
	).Scan(&v.ID, &v.Name, &v.Path, &isInt, &isAgg, &v.SplitTransactionRuleID)
	v.IsInternal = isInt == 1
	v.IsAggregator = isAgg == 1
	return v, err
}

func (s *Service) initReconciliationRecord(ctx context.Context, p *projectInfo, date, reconType, fileHistoryID string) (string, error) {
	var reconID string
	err := s.db.QueryRowContext(ctx, `
		SELECT id FROM reconciliations
		WHERE project_id = ? AND date = ? AND recon_type = ?
	`, p.ID, date, reconType).Scan(&reconID)

	if err == sql.ErrNoRows {
		reconID = generateUUID()
		_, insErr := s.db.ExecContext(ctx, `
			INSERT INTO reconciliations (
				id, project_id, file_check_history_id, recon_type, status, name, date,
				lifecycle_status, total, total_match, total_differences, total_differences_empty,
				total_vendors, sum_amount_all, sum_amount_all_highest, sum_amount_all_lowest,
				created_at, updated_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, 1, 0, 0, 0, 0, 0, 0, 0, 0, NOW(), NOW())
		`, reconID, p.ID, fileHistoryID, reconType, StatusOnProgress, p.ProjectName, date)
		return reconID, insErr
	}

	_, updErr := s.db.ExecContext(ctx, `
		UPDATE reconciliations SET status = ?, updated_at = NOW() WHERE id = ?
	`, StatusOnProgress, reconID)
	return reconID, updErr
}

// -----------------------------------------------------------------------
// Comparison logic & utilities
// -----------------------------------------------------------------------

func isDifferent(col, v1, v2 string, tol *toleranceSetting) bool {
	if v1 == "" && v2 == "" {
		return false
	}
	if v1 == "" || v2 == "" {
		return true
	}

	switch col {
	case ColCreatedAt, ColPaidAt, ColPayoutAt:
		t1, err1 := parseDateTime(v1)
		t2, err2 := parseDateTime(v2)
		if err1 == nil && err2 == nil {
			maxSecs := tol.MaskingTolerance[col]
			diffSecs := math.Abs(t1.Sub(t2).Seconds())
			return diffSecs > maxSecs
		}
		return v1 != v2

	case ColAmount, ColTotalAmount, ColFee:
		n1, err1 := strconv.ParseFloat(v1, 64)
		n2, err2 := strconv.ParseFloat(v2, 64)
		if err1 == nil && err2 == nil {
			absDiff := math.Abs(n1 - n2)
			allowedTol := tol.MaskingTolerance[col]
			pct := tol.MaskingTolerancePercent[col]
			if pct > 0 {
				base := math.Max(math.Abs(n1), math.Abs(n2))
				allowedTol = (base * pct) / 100.0
			}
			return absDiff > allowedTol
		}
		return v1 != v2

	case ColStatus, ColPaymentMethod:
		return !strings.EqualFold(v1, v2)

	default:
		return v1 != v2
	}
}

func parseDateTime(s string) (time.Time, error) {
	formats := []string{
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04",
		"2006-01-02",
	}
	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unable to parse date: %s", s)
}

func extractSubRef(refNumber, phpPattern string) string {
	// Strip PHP delimiters (e.g. /^...$/i -> (?i)^...$)
	cleanPat := phpPattern
	if strings.HasPrefix(cleanPat, "/") && strings.LastIndex(cleanPat, "/") > 0 {
		lastSlash := strings.LastIndex(cleanPat, "/")
		flags := cleanPat[lastSlash+1:]
		cleanPat = cleanPat[1:lastSlash]
		if strings.Contains(flags, "i") {
			cleanPat = "(?i)" + cleanPat
		}
	}
	re, err := regexp.Compile(cleanPat)
	if err != nil {
		return ""
	}
	matches := re.FindStringSubmatch(refNumber)
	if len(matches) >= 2 {
		return matches[1]
	}
	return ""
}

func (s *Service) buildMainTableName(fType, projectPath, date string) string {
	d := strings.ReplaceAll(date[:10], "-", "")
	return fmt.Sprintf("trx_%s_%s_%s", fType, cleanString(projectPath), d)
}

func (s *Service) buildVendorTableName(fType, projectPath, vendorPath, date string) string {
	d := strings.ReplaceAll(date[:10], "-", "")
	return fmt.Sprintf("trx_%s_%s_%s_%s", fType, cleanString(projectPath), cleanString(vendorPath), d)
}

func cleanString(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		} else if r == '/' || r == '-' || r == ' ' {
			b.WriteRune('_')
		}
	}
	return b.String()
}

func generateUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func (s *Service) buildLockName(payload *queue.ReconcileJobPayload) string {
	if payload.TargetDate != "" && payload.TargetType != "" {
		return fmt.Sprintf("reconcile:run:lock:%s:%s:%s", payload.ProjectID, payload.TargetDate, payload.TargetType)
	}
	return fmt.Sprintf("reconcile:run:lock:regular:%s", payload.ProjectID)
}

func (s *Service) acquireLock(ctx context.Context, lockName string, timeout int) (bool, error) {
	var result sql.NullInt64
	err := s.db.QueryRowContext(ctx, "SELECT GET_LOCK(?, ?)", lockName, timeout).Scan(&result)
	if err != nil {
		return false, err
	}
	return result.Valid && result.Int64 == 1, nil
}

func (s *Service) releaseLock(ctx context.Context, lockName string) {
	s.db.ExecContext(ctx, "SELECT RELEASE_LOCK(?)", lockName)
}

func (s *Service) markReconciliationFailed(ctx context.Context, payload *queue.ReconcileJobPayload, errMsg string) {
	q := `UPDATE reconciliations SET status = ?, message = ?, updated_at = NOW()
	      WHERE project_id = ? AND status = ?`
	args := []interface{}{StatusFailed, errMsg, payload.ProjectID, StatusOnProgress}
	if payload.TargetDate != "" {
		q += " AND date = ?"
		args = append(args, payload.TargetDate)
	}
	s.db.ExecContext(ctx, q, args...)
}

func contains(arr []string, val string) bool {
	for _, v := range arr {
		if v == val {
			return true
		}
	}
	return false
}

func uniqueStrings(arr []string) []string {
	seen := make(map[string]bool)
	var out []string
	for _, v := range arr {
		if !seen[v] && v != "" {
			seen[v] = true
			out = append(out, v)
		}
	}
	return out
}
