package service

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"go-reconciliation/internal/domain"
	"go-reconciliation/pkg/stringhelper"
)

type ScannerService struct {
	nasHomePath string
}

func NewScannerService(nasHomePath string) *ScannerService {
	return &ScannerService{nasHomePath: nasHomePath}
}

// ScanVendorDirectory scans a single vendor folder on the NAS
func (s *ScannerService) ScanVendorDirectory(vp *domain.VendorProject, maxFiles int) ([]*domain.ScannedFile, error) {
	nasPath := stringhelper.JoinPaths(s.nasHomePath, vp.RelativePath)

	if _, err := os.Stat(nasPath); os.IsNotExist(err) {
		_ = os.MkdirAll(nasPath, 0755)
		return nil, nil
	}

	entries, err := os.ReadDir(nasPath)
	if err != nil {
		return nil, fmt.Errorf("read dir %s failed: %w", nasPath, err)
	}

	var rawFiles []string
	for _, entry := range entries {
		if !entry.IsDir() {
			rawFiles = append(rawFiles, entry.Name())
		}
	}

	sort.Strings(rawFiles)

	var result []*domain.ScannedFile
	excelExts := map[string]bool{".xlsx": true, ".xls": true, ".xlsm": true}

	for _, filename := range rawFiles {
		fullPath := filepath.Join(nasPath, filename)
		ext := strings.ToLower(filepath.Ext(filename))
		cleanName := stringhelper.CleanFilename(filename)
		fileDate := stringhelper.ExtractDateFromFilename(filename)
		trxType := stringhelper.ExtractTransactionTypeFromFilename(filename)

		if ext == ".csv" || excelExts[ext] {
			fileType := "csv"
			if excelExts[ext] {
				fileType = "excel"
			}

			md5Hash, _ := stringhelper.ComputeMD5(fullPath)

			result = append(result, &domain.ScannedFile{
				FullPath:      fullPath,
				Filename:      filename,
				CleanFilename: cleanName,
				Extension:     fileType,
				Type:          trxType,
				Date:          fileDate,
				MD5:           md5Hash,
				ProjectID:     vp.ID,
				VendorID:      vp.VendorID,
				VendorPath:    vp.VendorPath,
				RelativePath:  vp.RelativePath,
				NasPath:       nasPath,
			})

			if len(result) >= maxFiles {
				break
			}
		}
	}

	return result, nil
}
