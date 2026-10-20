package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/google/uuid"
	"go-reconciliation/internal/domain"
)

type FileRepository struct {
	db *sql.DB
}

func NewFileRepository(db *sql.DB) *FileRepository {
	return &FileRepository{db: db}
}

// CheckIsOnProgress checks if there's any file currently processing
func (r *FileRepository) CheckIsOnProgress(ctx context.Context, projectIDs []string) (bool, error) {
	query := `SELECT COUNT(*) FROM file_check_histories WHERE status = ?`
	args := []interface{}{domain.FileHistoryOnProgress}

	if len(projectIDs) > 0 {
		placeholders := strings.Repeat("?,", len(projectIDs))
		placeholders = strings.TrimSuffix(placeholders, ",")
		query += fmt.Sprintf(` AND project_id IN (%s)`, placeholders)
		for _, id := range projectIDs {
			args = append(args, id)
		}
	}

	var count int
	err := r.db.QueryRowContext(ctx, query, args...).Scan(&count)
	return count > 0, err
}

// FetchPendingOrInterruptedFiles loads all files with WAITING or ONPROGRESS status (for Startup Recovery)
func (r *FileRepository) FetchPendingOrInterruptedFiles(ctx context.Context) ([]*domain.FileCheckHistory, error) {
	query := `
		SELECT id, project_id, vendor_id, filename, md5, date, type, path, status,
		       table_columns, csv_columns, custom_profile_id, file_group, total, message, created_at, updated_at
		FROM file_check_histories
		WHERE status IN (?, ?)
		ORDER BY created_at ASC
	`
	rows, err := r.db.QueryContext(ctx, query, domain.FileHistoryWaiting, domain.FileHistoryOnProgress)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []*domain.FileCheckHistory
	for rows.Next() {
		var fch domain.FileCheckHistory
		var fType sql.NullString
		var customProfile sql.NullInt64
		var message sql.NullString

		err := rows.Scan(
			&fch.ID,
			&fch.ProjectID,
			&fch.VendorID,
			&fch.Filename,
			&fch.MD5,
			&fch.Date,
			&fType,
			&fch.Path,
			&fch.Status,
			&fch.TableColumns,
			&fch.CsvColumns,
			&customProfile,
			&fch.FileGroup,
			&fch.Total,
			&message,
			&fch.CreatedAt,
			&fch.UpdatedAt,
		)
		if err != nil {
			continue
		}

		if fType.Valid {
			fch.Type = &fType.String
		}
		if customProfile.Valid {
			fch.CustomProfileID = &customProfile.Int64
		}
		if message.Valid {
			fch.Message = &message.String
		}

		list = append(list, &fch)
	}

	return list, nil
}

// FetchFileByID retrieves a single FileCheckHistory by ID
func (r *FileRepository) FetchFileByID(ctx context.Context, id string) (*domain.FileCheckHistory, error) {
	query := `
		SELECT id, project_id, vendor_id, filename, md5, date, type, path, status,
		       table_columns, csv_columns, custom_profile_id, file_group, total, message, created_at, updated_at
		FROM file_check_histories
		WHERE id = ?
	`
	row := r.db.QueryRowContext(ctx, query, id)

	var fch domain.FileCheckHistory
	var fType sql.NullString
	var customProfile sql.NullInt64
	var message sql.NullString

	err := row.Scan(
		&fch.ID,
		&fch.ProjectID,
		&fch.VendorID,
		&fch.Filename,
		&fch.MD5,
		&fch.Date,
		&fType,
		&fch.Path,
		&fch.Status,
		&fch.TableColumns,
		&fch.CsvColumns,
		&customProfile,
		&fch.FileGroup,
		&fch.Total,
		&message,
		&fch.CreatedAt,
		&fch.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	if fType.Valid {
		fch.Type = &fType.String
	}
	if customProfile.Valid {
		fch.CustomProfileID = &customProfile.Int64
	}
	if message.Valid {
		fch.Message = &message.String
	}

	return &fch, nil
}

// UpdateFileStatus updates the status and message of a FileCheckHistory
func (r *FileRepository) UpdateFileStatus(ctx context.Context, id string, status int, message string) error {
	query := `UPDATE file_check_histories SET status = ?, message = ?, updated_at = NOW() WHERE id = ?`
	_, err := r.db.ExecContext(ctx, query, status, message, id)
	return err
}

// UpsertFileCheckHistory creates or updates file check history
func (r *FileRepository) UpsertFileCheckHistory(ctx context.Context, fch *domain.FileCheckHistory) error {
	if fch.ID == "" {
		fch.ID = uuid.New().String()
	}

	query := `
		INSERT INTO file_check_histories (
			id, project_id, vendor_id, filename, md5, date, type, path, status,
			table_columns, csv_columns, custom_profile_id, file_group, total, message, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
		ON DUPLICATE KEY UPDATE
			status = VALUES(status),
			md5 = VALUES(md5),
			type = VALUES(type),
			table_columns = VALUES(table_columns),
			csv_columns = VALUES(csv_columns),
			custom_profile_id = VALUES(custom_profile_id),
			file_group = VALUES(file_group),
			total = VALUES(total),
			message = VALUES(message),
			updated_at = NOW()
	`

	_, err := r.db.ExecContext(ctx, query,
		fch.ID,
		fch.ProjectID,
		fch.VendorID,
		fch.Filename,
		fch.MD5,
		fch.Date,
		fch.Type,
		fch.Path,
		fch.Status,
		fch.TableColumns,
		fch.CsvColumns,
		fch.CustomProfileID,
		fch.FileGroup,
		fch.Total,
		fch.Message,
	)
	return err
}

// SaveFileHistoryDetails batch inserts/updates details per date partition
func (r *FileRepository) SaveFileHistoryDetails(ctx context.Context, details []*domain.FileHistoryDetail) error {
	if len(details) == 0 {
		return nil
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	query := `
		INSERT INTO file_history_details (
			id, file_history_id, project_id, vendor_id, date, type, path, filename, total, is_reconciled, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NOW(), NOW())
		ON DUPLICATE KEY UPDATE
			total = VALUES(total),
			path = VALUES(path),
			filename = VALUES(filename),
			updated_at = NOW()
	`

	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, d := range details {
		if d.ID == "" {
			d.ID = uuid.New().String()
		}
		_, err := stmt.ExecContext(ctx,
			d.ID,
			d.FileHistoryID,
			d.ProjectID,
			d.VendorID,
			d.Date,
			d.Type,
			d.Path,
			d.Filename,
			d.Total,
		)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// FetchMovedTransactions gets references moved between dates
func (r *FileRepository) FetchMovedTransactions(ctx context.Context, projectID, vendorID string, trxType string, dates []string) (map[string]map[string]bool, error) {
	if len(dates) == 0 {
		return make(map[string]map[string]bool), nil
	}

	placeholders := strings.Repeat("?,", len(dates))
	placeholders = strings.TrimSuffix(placeholders, ",")

	query := fmt.Sprintf(`
		SELECT reference_no, from_date
		FROM transaction_movements
		WHERE project_id = ? AND vendor_id = ? AND type = ? AND from_date IN (%s)
	`, placeholders)

	args := []interface{}{projectID, vendorID, trxType}
	for _, d := range dates {
		args = append(args, d)
	}

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// map: from_date -> reference_no -> true
	result := make(map[string]map[string]bool)
	for rows.Next() {
		var refNo, fromDate string
		if err := rows.Scan(&refNo, &fromDate); err != nil {
			continue
		}
		if _, ok := result[fromDate]; !ok {
			result[fromDate] = make(map[string]bool)
		}
		result[fromDate][refNo] = true
	}

	return result, nil
}

// RecordFileErrorLog records or updates file error audit log
func (r *FileRepository) RecordFileErrorLog(ctx context.Context, fileID string, projectID, vendorID string, trxType, fileType, filename, date, message string, status int) error {
	query := `
		INSERT INTO file_error_logs (
			id, file_id, project_id, vendor_id, type, file_type, date, filename, status, file_status, message, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
		ON DUPLICATE KEY UPDATE
			status = VALUES(status),
			file_status = VALUES(file_status),
			message = VALUES(message),
			resolved_at = NULL,
			updated_at = NOW()
	`
	_, err := r.db.ExecContext(ctx, query,
		uuid.New().String(),
		fileID,
		projectID,
		vendorID,
		trxType,
		fileType,
		date,
		filename,
		domain.FileErrorLogActive,
		status,
		message,
	)
	return err
}

// ResolveFileErrorLogs marks error logs as resolved
func (r *FileRepository) ResolveFileErrorLogs(ctx context.Context, fileID string) error {
	query := `UPDATE file_error_logs SET status = ?, resolved_at = NOW() WHERE file_id = ? AND status = ?`
	_, err := r.db.ExecContext(ctx, query, domain.FileErrorLogResolved, fileID, domain.FileErrorLogActive)
	return err
}

// UpdateReconcileVendorsWaiting sets reconcile_vendors status to WAITING for specific dates and returns count of affected rows
func (r *FileRepository) UpdateReconcileVendorsWaiting(ctx context.Context, projectID string, trxType string, dates []string) (int64, error) {
	if len(dates) == 0 {
		return 0, nil
	}

	placeholders := strings.Repeat("?,", len(dates))
	placeholders = strings.TrimSuffix(placeholders, ",")

	query := fmt.Sprintf(`
		UPDATE reconcile_vendors
		SET status = ?, updated_at = NOW()
		WHERE project_id = ? AND type = ? AND date IN (%s)
	`, placeholders)

	args := []interface{}{domain.ReconcileStatusWaiting, projectID, trxType}
	for _, d := range dates {
		args = append(args, d)
	}

	res, err := r.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// AutoCreateReconByVendor automatically registers reconcile_vendors and reconcile_vendor_details when all vendors uploaded
func (r *FileRepository) AutoCreateReconByVendor(ctx context.Context, projectID string, mainVendorID string, projectName string, dates []string, trxType string, allVendorProjects []*domain.VendorProject) (bool, error) {
	if mainVendorID == "" || len(dates) == 0 || len(allVendorProjects) == 0 {
		return false, nil
	}

	placeholders := strings.Repeat("?,", len(dates))
	placeholders = strings.TrimSuffix(placeholders, ",")

	queryDetails := fmt.Sprintf(`
		SELECT date, vendor_id, filename
		FROM file_history_details
		WHERE project_id = ? AND type = ? AND date IN (%s)
	`, placeholders)

	args := []interface{}{projectID, trxType}
	for _, d := range dates {
		args = append(args, d)
	}

	rows, err := r.db.QueryContext(ctx, queryDetails, args...)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	vendorFilesMap := make(map[string]map[string][]string)
	for rows.Next() {
		var date string
		var vendorID string
		var filename string
		if err := rows.Scan(&date, &vendorID, &filename); err != nil {
			continue
		}
		if _, ok := vendorFilesMap[date]; !ok {
			vendorFilesMap[date] = make(map[string][]string)
		}
		vendorFilesMap[date][vendorID] = append(vendorFilesMap[date][vendorID], filename)
	}

	queryRecon := fmt.Sprintf(`
		SELECT date
		FROM reconcile_vendors
		WHERE vendor_id = ? AND project_id = ? AND type = ? AND date IN (%s)
	`, placeholders)

	argsRecon := []interface{}{mainVendorID, projectID, trxType}
	for _, d := range dates {
		argsRecon = append(argsRecon, d)
	}

	reconRows, err := r.db.QueryContext(ctx, queryRecon, argsRecon...)
	if err != nil {
		return false, err
	}
	defer reconRows.Close()

	existingRecons := make(map[string]bool)
	for reconRows.Next() {
		var date string
		if err := reconRows.Scan(&date); err == nil {
			existingRecons[date] = true
		}
	}

	createdAny := false
	for _, date := range dates {
		if existingRecons[date] {
			continue
		}

		uploadedVendors := vendorFilesMap[date]
		if uploadedVendors == nil {
			continue
		}

		allUploaded := true
		for _, vp := range allVendorProjects {
			if len(uploadedVendors[vp.VendorID]) == 0 {
				allUploaded = false
				break
			}
		}

		if !allUploaded {
			continue
		}

		tx, err := r.db.BeginTx(ctx, nil)
		if err != nil {
			continue
		}

		reconcileVendorID := uuid.New().String()
		insertReconQuery := `
			INSERT INTO reconcile_vendors (id, project_id, vendor_id, name, date, type, status, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
		`
		_, err = tx.ExecContext(ctx, insertReconQuery, reconcileVendorID, projectID, mainVendorID, projectName, date, trxType, domain.ReconcileStatusWaiting)
		if err != nil {
			tx.Rollback()
			continue
		}

		for _, vp := range allVendorProjects {
			filesList := uploadedVendors[vp.VendorID]
			filesJSON, _ := json.Marshal(filesList)
			isMain := 0
			if vp.VendorID == mainVendorID {
				isMain = 1
			}

			isInternal := 0
			if vp.IsInternal && !vp.IsAggregator && vp.VendorID != mainVendorID {
				isInternal = 1
			}

			isAggregator := 0
			if vp.IsAggregator && !vp.IsInternal && vp.VendorID != mainVendorID {
				isAggregator = 1
			}

			isExternal := 0
			if !vp.IsInternal && !vp.IsAggregator && vp.VendorID != mainVendorID {
				isExternal = 1
			}

			detailQuery := `
				INSERT INTO reconcile_vendor_details (
					id, vendor_id, reconcile_vendor_id, is_main, is_internal, is_aggregator, is_external, files, summary, created_at, updated_at
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, '[]', NOW(), NOW())
			`
			_, err = tx.ExecContext(ctx, detailQuery,
				uuid.New().String(),
				vp.VendorID,
				reconcileVendorID,
				isMain,
				isInternal,
				isAggregator,
				isExternal,
				string(filesJSON),
			)
			if err != nil {
				tx.Rollback()
				allUploaded = false
				break
			}
		}

		if allUploaded {
			if err := tx.Commit(); err == nil {
				createdAny = true
				log.Printf("[ReconcileVendor] Automatic ReconcileByVendor created for %s#%s#%s#%s", projectName, date, trxType, mainVendorID)
			}
		}
	}

	return createdAny, nil
}
