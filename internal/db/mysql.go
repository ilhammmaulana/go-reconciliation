package db

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"go-reconciliation/internal/config"
	"go-reconciliation/internal/logger"
)

// DB wraps the standard sql.DB with helper methods
type DB struct {
	*sql.DB
}

// Connect opens a MySQL connection pool using the provided config
func Connect(cfg *config.Config) (*DB, error) {
	db, err := sql.Open("mysql", cfg.DBDsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open DB: %w", err)
	}

	// Tune connection pool — important for long-running Go workers
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetConnMaxIdleTime(2 * time.Minute)

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping DB: %w", err)
	}

	logger.Info.Printf("Connected to MySQL: %s/%s", cfg.DBHost, cfg.DBName)
	return &DB{db}, nil
}

// BulkInsert runs a single INSERT ... VALUES(...),(...) query for efficiency.
// tableName must be pre-sanitized (no user input). columns and rows must align.
func (db *DB) BulkInsert(tableName string, columns []string, rows [][]interface{}) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	// Build: INSERT INTO `table` (`col1`,`col2`) VALUES (?,?),(?,?)
	colParts := make([]string, len(columns))
	for i, c := range columns {
		colParts[i] = "`" + c + "`"
	}

	placeholderRow := "(" + strings.Repeat("?,", len(columns))
	placeholderRow = placeholderRow[:len(placeholderRow)-1] + ")"

	allPlaceholders := make([]string, len(rows))
	args := make([]interface{}, 0, len(rows)*len(columns))

	for i, row := range rows {
		allPlaceholders[i] = placeholderRow
		args = append(args, row...)
	}

	query := fmt.Sprintf(
		"INSERT INTO `%s` (%s) VALUES %s",
		tableName,
		strings.Join(colParts, ","),
		strings.Join(allPlaceholders, ","),
	)

	result, err := db.Exec(query, args...)
	if err != nil {
		return 0, fmt.Errorf("bulk insert into %s failed: %w", tableName, err)
	}

	return result.RowsAffected()
}

// BulkUpsert runs INSERT ... ON DUPLICATE KEY UPDATE for upsert semantics.
func (db *DB) BulkUpsert(tableName string, columns []string, updateColumns []string, rows [][]interface{}) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	colParts := make([]string, len(columns))
	for i, c := range columns {
		colParts[i] = "`" + c + "`"
	}

	placeholderRow := "(" + strings.Repeat("?,", len(columns))
	placeholderRow = placeholderRow[:len(placeholderRow)-1] + ")"

	allPlaceholders := make([]string, len(rows))
	args := make([]interface{}, 0, len(rows)*len(columns))
	for i, row := range rows {
		allPlaceholders[i] = placeholderRow
		args = append(args, row...)
	}

	updateParts := make([]string, len(updateColumns))
	for i, c := range updateColumns {
		updateParts[i] = fmt.Sprintf("`%s`=VALUES(`%s`)", c, c)
	}

	query := fmt.Sprintf(
		"INSERT INTO `%s` (%s) VALUES %s ON DUPLICATE KEY UPDATE %s",
		tableName,
		strings.Join(colParts, ","),
		strings.Join(allPlaceholders, ","),
		strings.Join(updateParts, ","),
	)

	result, err := db.Exec(query, args...)
	if err != nil {
		return 0, fmt.Errorf("bulk upsert into %s failed: %w", tableName, err)
	}

	return result.RowsAffected()
}
