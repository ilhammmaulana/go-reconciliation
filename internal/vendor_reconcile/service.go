package vendor_reconcile

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"go-reconciliation/internal/config"
	"go-reconciliation/internal/db"
	"go-reconciliation/internal/logger"
	"go-reconciliation/internal/queue"
)

const (
	StatusWaiting    = 0
	StatusOnProgress = 1
	StatusFailed     = 2
	StatusCompleted  = 3

	ColReferenceNo   = "reference_no"
	ColAmount        = "amount"
	ColFee           = "fee"
	ColTotalAmount   = "total_amount"
	ColStatus        = "status"
	ColCreatedAt     = "created_at"
	ColPaidAt        = "paid_at"
	ColPayoutAt      = "payout_at"
	ColPaymentMethod = "payment_method"
)

// Service handles cross-vendor reconciliation mirroring ReconcileVendorService.php
type Service struct {
	db     *db.DB
	config *config.Config
}

// New creates a new vendor reconcile Service
func New(database *db.DB, cfg *config.Config) *Service {
	return &Service{
		db:     database,
		config: cfg,
	}
}

// Handle processes a RunReconcileVendorJob payload dispatched by Laravel
func (s *Service) Handle(ctx context.Context, payload *queue.ReconcileVendorJobPayload) error {
	logger.Info.Printf("[RECON-VENDOR] Starting job UUID=%s projectId=%s reconcileVendorId=%s",
		payload.JobUUID, payload.ProjectID, payload.ReconcileVendorID)

	lockKey := s.buildLockKey(payload)
	locked, err := s.acquireLock(ctx, lockKey, 1800)
	if err != nil {
		return fmt.Errorf("lock error: %w", err)
	}
	if !locked {
		logger.Info.Printf("[RECON-VENDOR SKIP] Lock held: %s", lockKey)
		return nil
	}
	defer s.releaseLock(ctx, lockKey)

	result, err := s.reconcileByVendor(ctx, payload)
	if err != nil {
		return err
	}

	logger.Info.Printf("[RECON-VENDOR] Completed result=%s", result)
	runtime.GC()
	return nil
}

type reconcileVendorRecord struct {
	ID        string
	ProjectID string
	VendorID  string
	Name      string
	Date      string
	Type      string
	Status    int
}

type vendorDetailRecord struct {
	ID                string
	ReconcileVendorID string
	VendorID          string
	IsMain            bool
	IsExternal        bool
	IsAggregator      bool
	IsInternal        bool
	FilesRaw          string
}

type toleranceConfig struct {
	MaskingTolerance        map[string]float64
	MaskingTolerancePercent map[string]float64
}

// reconcileByVendor mirrors ReconcileVendorService::reconcileByVendorV2()
func (s *Service) reconcileByVendor(ctx context.Context, payload *queue.ReconcileVendorJobPayload) (string, error) {
	// Step 1: Pick waiting record and lock with transaction
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return "", err
	}

	query := `
		SELECT id, project_id, vendor_id, name, date, type, status
		FROM reconcile_vendors
		WHERE status = ?
	`
	args := []interface{}{StatusWaiting}

	if payload.ReconcileVendorID != "" {
		query += " AND id = ?"
		args = append(args, payload.ReconcileVendorID)
	} else if payload.ProjectID != "" {
		query += " AND project_id = ?"
		args = append(args, payload.ProjectID)
	}
	query += " ORDER BY date ASC LIMIT 1 FOR UPDATE"

	var rv reconcileVendorRecord
	err = tx.QueryRowContext(ctx, query, args...).Scan(
		&rv.ID, &rv.ProjectID, &rv.VendorID, &rv.Name, &rv.Date, &rv.Type, &rv.Status,
	)
	if err == sql.ErrNoRows {
		tx.Rollback()
		return "EMPTY", nil
	}
	if err != nil {
		tx.Rollback()
		return "", fmt.Errorf("select reconcile_vendors error: %w", err)
	}

	// Update to onprogress
	if _, err := tx.ExecContext(ctx, "UPDATE reconcile_vendors SET status = ?, updated_at = NOW() WHERE id = ?", StatusOnProgress, rv.ID); err != nil {
		tx.Rollback()
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}

	logger.Info.Printf("[RECON-VENDOR] Processing rv_id=%s date=%s type=%s", rv.ID, rv.Date, rv.Type)

	// Step 2: Fetch details
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, reconcile_vendor_id, vendor_id, is_main, is_external, is_aggregator, is_internal, files
		FROM reconcile_vendor_details
		WHERE reconcile_vendor_id = ?
	`, rv.ID)
	if err != nil {
		s.failRecord(ctx, rv.ID, err.Error())
		return "", err
	}
	defer rows.Close()

	var details []vendorDetailRecord
	for rows.Next() {
		var d vendorDetailRecord
		var isM, isExt, isAgg, isInt int
		if err := rows.Scan(&d.ID, &d.ReconcileVendorID, &d.VendorID, &isM, &isExt, &isAgg, &isInt, &d.FilesRaw); err == nil {
			d.IsMain = isM == 1
			d.IsExternal = isExt == 1
			d.IsAggregator = isAgg == 1
			d.IsInternal = isInt == 1
			details = append(details, d)
		}
	}

	// Step 3: Load tolerances
	tol := s.loadTolerances(ctx)

	// Step 4: Load files & data for main vendor vs other vendors
	mainTransactions := make(map[string]map[string]string)
	vendorTransactions := make(map[string]map[string]map[string]string) // vid -> ref -> row

	for _, d := range details {
		var filenames []string
		_ = json.Unmarshal([]byte(d.FilesRaw), &filenames)

		for _, fn := range filenames {
			// Find file path in file_history_details
			var relPath string
			s.db.QueryRowContext(ctx,
				"SELECT path FROM file_history_details WHERE filename = ? AND project_id = ? LIMIT 1",
				fn, rv.ProjectID,
			).Scan(&relPath)

			if relPath == "" {
				continue
			}

			fullPath := filepath.Join(s.config.StoragePath, relPath, fn)
			recs := s.readCSV(fullPath)

			if d.IsMain {
				for _, r := range recs {
					if ref := r[ColReferenceNo]; ref != "" {
						mainTransactions[ref] = r
					}
				}
			} else {
				if _, ok := vendorTransactions[d.VendorID]; !ok {
					vendorTransactions[d.VendorID] = make(map[string]map[string]string)
				}
				for _, r := range recs {
					if ref := r[ColReferenceNo]; ref != "" {
						vendorTransactions[d.VendorID][ref] = r
					}
				}
			}
		}
	}

	// Step 5: Perform cross-vendor matching against main
	var totalMatch, totalDiff, totalEmpty int
	for _, mainRow := range mainTransactions {
		ref := mainRow[ColReferenceNo]

		hasDiff := false
		hasEmpty := false
		for _, vRows := range vendorTransactions {
			vRow, ok := vRows[ref]
			if !ok {
				hasEmpty = true
				continue
			}

			// Compare amount
			if !compareAmount(mainRow[ColAmount], vRow[ColAmount], tol) {
				hasDiff = true
			}
			// Compare status
			if !strings.EqualFold(mainRow[ColStatus], vRow[ColStatus]) {
				hasDiff = true
			}
		}

		if hasDiff {
			totalDiff++
		} else if hasEmpty {
			totalEmpty++
		} else {
			totalMatch++
		}
	}

	totalEntries := len(mainTransactions)

	// Step 6: Update reconcile_vendor record
	_, err = s.db.ExecContext(ctx, `
		UPDATE reconcile_vendors SET
			status = ?,
			total = ?,
			total_match = ?,
			total_differences = ?,
			total_empty = ?,
			message = NULL,
			updated_at = NOW()
		WHERE id = ?
	`, StatusCompleted, totalEntries, totalMatch, totalDiff, totalEmpty, rv.ID)
	if err != nil {
		return "", err
	}

	// Step 7: Check if more waiting jobs exist
	var waitingCount int
	s.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM reconcile_vendors WHERE project_id = ? AND status = ?",
		rv.ProjectID, StatusWaiting,
	).Scan(&waitingCount)

	if waitingCount > 0 {
		return "NEXT", nil
	}
	return "DONE", nil
}

func (s *Service) readCSV(path string) []map[string]string {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.Comma = ';'
	reader.LazyQuotes = true
	reader.FieldsPerRecord = -1

	header, err := reader.Read()
	if err != nil {
		return nil
	}

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
			}
		}
		records = append(records, rowMap)
	}
	return records
}

func compareAmount(v1, v2 string, tol *toleranceConfig) bool {
	n1, err1 := strconv.ParseFloat(v1, 64)
	n2, err2 := strconv.ParseFloat(v2, 64)
	if err1 != nil || err2 != nil {
		return v1 == v2
	}
	allowed := tol.MaskingTolerance["amount"]
	return math.Abs(n1-n2) <= allowed
}

func (s *Service) loadTolerances(ctx context.Context) *toleranceConfig {
	tc := &toleranceConfig{
		MaskingTolerance:        map[string]float64{"amount": 1000, "total_amount": 1000},
		MaskingTolerancePercent: map[string]float64{"amount": 0},
	}
	var maskRaw sql.NullString
	s.db.QueryRowContext(ctx, "SELECT masking_tolerance FROM reconciliation_settings LIMIT 1").Scan(&maskRaw)
	if maskRaw.Valid && maskRaw.String != "" {
		var m map[string]float64
		_ = json.Unmarshal([]byte(maskRaw.String), &m)
		for k, v := range m {
			tc.MaskingTolerance[k] = v
		}
	}
	return tc
}

func (s *Service) failRecord(ctx context.Context, id, msg string) {
	s.db.ExecContext(ctx,
		"UPDATE reconcile_vendors SET status = ?, message = ?, updated_at = NOW() WHERE id = ?",
		StatusFailed, msg, id,
	)
}

func (s *Service) buildLockKey(payload *queue.ReconcileVendorJobPayload) string {
	if payload.ReconcileVendorID != "" {
		return fmt.Sprintf("reconcile:run:vendor:lock:id:%s", payload.ReconcileVendorID)
	}
	return fmt.Sprintf("reconcile:run:vendor:lock:proj:%s", payload.ProjectID)
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
