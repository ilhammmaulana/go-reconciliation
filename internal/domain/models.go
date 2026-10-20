package domain

import (
	"time"
)

// Vendor represents partner or internal ledger
type Vendor struct {
	ID                     string  `json:"id"`
	Name                   string  `json:"name"`
	Path                   string  `json:"path"`
	IsInternal             bool    `json:"is_internal"`
	IsAggregator           bool    `json:"is_aggregator"`
	CustomProfileMappingID *string `json:"custom_profile_maping_id"`
}

// VendorProject represents project reconciliation configuration
type VendorProject struct {
	ID                     string  `json:"id"`
	VendorID               string  `json:"vendor_id"`
	ProjectName            string  `json:"project_name"`
	ProjectPath            string  `json:"project_path"`
	Status                 int     `json:"status"`
	AutoReconcileVendorID  *string `json:"auto_reconcile_by_vendor_id"`
	CustomProfileMappingID *string `json:"custom_profile_maping_id"`
	IsInternal             bool    `json:"is_internal"`
	IsAggregator           bool    `json:"is_aggregator"`
	VendorPath             string  `json:"vendor_path"`
	RelativePath           string  `json:"relative_path"`
}

// CustomProfile represents CSV header translation rules
type CustomProfile struct {
	ID       string            `json:"id"`
	Name     string            `json:"name"`
	Type     string            `json:"type"` // paid or payout
	Mappings map[string]string `json:"mappings"` // raw column -> standardized column
}

// FileCheckHistory maps to file_check_histories table
type FileCheckHistory struct {
	ID              string     `json:"id"`
	ProjectID       string     `json:"project_id"`
	VendorID        string     `json:"vendor_id"`
	Filename        string     `json:"filename"`
	MD5             string     `json:"md5"`
	Date            string     `json:"date"`
	Type            *string    `json:"type"`
	Path            string     `json:"path"`
	Status          int        `json:"status"`
	TableColumns    string     `json:"table_columns"`
	CsvColumns      string     `json:"csv_columns"`
	CustomProfileID *string    `json:"custom_profile_id"`
	FileGroup       string     `json:"file_group"`
	Total           int        `json:"total"`
	Message         *string    `json:"message"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
}

// FileHistoryDetail maps to file_history_details table
type FileHistoryDetail struct {
	ID                string    `json:"id"`
	FileHistoryID     string    `json:"file_history_id"`
	ProjectID         string    `json:"project_id"`
	VendorID          string    `json:"vendor_id"`
	Date              string    `json:"date"`
	Type              string    `json:"type"`
	Path              string    `json:"path"`
	Filename          string    `json:"filename"`
	Total             int       `json:"total"`
	IsReconciled      bool      `json:"is_reconciled"`
	IsMoveTransaction bool      `json:"is_move_transaction"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
}

// ScannedFile represents a raw file discovered on NAS
type ScannedFile struct {
	FullPath      string `json:"full_path"`
	Filename      string `json:"filename"`
	CleanFilename string `json:"clean_filename"`
	Extension     string `json:"extension"`
	Type          string `json:"type"` // paid, payout, summary, mutation
	Date          string `json:"date"`
	MD5           string `json:"md5"`
	ProjectID     string `json:"project_id"`
	VendorID      string `json:"vendor_id"`
	VendorPath    string `json:"vendor_path"`
	RelativePath  string `json:"relative_path"`
	NasPath       string `json:"nas_path"`
}

// JobPayload represents message received from Laravel via Redis or HTTP
type JobPayload struct {
	Action     string   `json:"action"` // "RERUN_FILE", "GROUP_PROJECT", etc.
	FileID     string   `json:"file_id,omitempty"`
	ProjectID  string   `json:"project_id,omitempty"`
	ProjectIDs []string `json:"project_ids,omitempty"`
	VendorID   string   `json:"vendor_id,omitempty"`
	Filename   string   `json:"filename,omitempty"`
	IsHighLoad bool     `json:"is_high_load,omitempty"`
	TargetDate string   `json:"target_date,omitempty"`
	Timestamp  int64    `json:"timestamp,omitempty"`
}

// GroupingResult summarizes outcome of date-splitting run
type GroupingResult struct {
	ProjectID             string              `json:"project_id"`
	AutoReconcileVendorID *string             `json:"auto_reconcile_by_vendor_id"`
	TotalCsvFiles         int                 `json:"total_csv_files"`
	TotalExcelFiles       int                 `json:"total_excel_files"`
	ReconcileDates        map[string][]string `json:"reconcile_dates"` // type -> dates
	Status                string              `json:"status"`
}
