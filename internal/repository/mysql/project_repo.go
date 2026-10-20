package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"go-reconciliation/internal/domain"
)

type ProjectRepository struct {
	db *sql.DB
}

func NewProjectRepository(db *sql.DB) *ProjectRepository {
	return &ProjectRepository{db: db}
}

// FetchProjectVendors retrieves vendor configurations for projects
func (r *ProjectRepository) FetchProjectVendors(ctx context.Context, projectIDs []string) ([]*domain.VendorProject, error) {
	var query string
	var args []interface{}

	if len(projectIDs) > 0 {
		placeholders := strings.Repeat("?,", len(projectIDs))
		placeholders = strings.TrimSuffix(placeholders, ",")

		query = fmt.Sprintf(`
			SELECT
				vp.id as project_id,
				vp.vendor_id,
				vp.status,
				vp.auto_reconcile_by_vendor_id,
				v.path as vendor_path,
				TRUE as is_internal,
				v.is_aggregator as is_aggregator,
				vp.project_path,
				CONCAT(v.path, '/INTERNAL/', vp.project_path) as relative_path,
				v.custom_profile_maping_id,
				vp.project_name
			FROM vendor_projects vp
			INNER JOIN vendors v ON v.id = vp.vendor_id
			WHERE vp.id IN (%s)
			UNION ALL
			SELECT
				vp.id as project_id,
				vpr.vendor_id,
				vp.status,
				vp.auto_reconcile_by_vendor_id,
				v.path as vendor_path,
				FALSE as is_internal,
				v.is_aggregator as is_aggregator,
				vp.project_path,
				CONCAT(v.path, '/PROJECTS/', vp.project_path) as relative_path,
				v.custom_profile_maping_id,
				vp.project_name
			FROM vendor_project_relations vpr
			INNER JOIN vendors v ON v.id = vpr.vendor_id
			INNER JOIN vendor_projects vp ON vp.id = vpr.vendor_projects_id
			WHERE vp.id IN (%s)
		`, placeholders, placeholders)

		for _, id := range projectIDs {
			args = append(args, id)
		}
		for _, id := range projectIDs {
			args = append(args, id)
		}
	} else {
		query = `
			SELECT
				vp.id as project_id,
				vp.vendor_id,
				vp.status,
				vp.auto_reconcile_by_vendor_id,
				v.path as vendor_path,
				TRUE as is_internal,
				v.is_aggregator as is_aggregator,
				vp.project_path,
				CONCAT(v.path, '/INTERNAL/', vp.project_path) as relative_path,
				v.custom_profile_maping_id,
				vp.project_name
			FROM vendor_projects vp
			INNER JOIN vendors v ON v.id = vp.vendor_id
			UNION ALL
			SELECT
				vp.id as project_id,
				vpr.vendor_id,
				vp.status,
				vp.auto_reconcile_by_vendor_id,
				v.path as vendor_path,
				FALSE as is_internal,
				v.is_aggregator as is_aggregator,
				vp.project_path,
				CONCAT(v.path, '/PROJECTS/', vp.project_path) as relative_path,
				v.custom_profile_maping_id,
				vp.project_name
			FROM vendor_project_relations vpr
			INNER JOIN vendors v ON v.id = vpr.vendor_id
			INNER JOIN vendor_projects vp ON vp.id = vpr.vendor_projects_id
		`
	}

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query project vendors failed: %w", err)
	}
	defer rows.Close()

	var result []*domain.VendorProject
	for rows.Next() {
		var vp domain.VendorProject
		var autoRecon sql.NullString
		var customProfile sql.NullString

		err := rows.Scan(
			&vp.ID,
			&vp.VendorID,
			&vp.Status,
			&autoRecon,
			&vp.VendorPath,
			&vp.IsInternal,
			&vp.IsAggregator,
			&vp.ProjectPath,
			&vp.RelativePath,
			&customProfile,
			&vp.ProjectName,
		)
		if err != nil {
			return nil, fmt.Errorf("scan vendor project failed: %w", err)
		}

		if autoRecon.Valid && autoRecon.String != "" {
			val := autoRecon.String
			vp.AutoReconcileVendorID = &val
		}
		if customProfile.Valid && customProfile.String != "" {
			val := customProfile.String
			vp.CustomProfileMappingID = &val
		}

		result = append(result, &vp)
	}

	return result, nil
}

type profileColumnItem struct {
	Name  string `json:"name"`
	Type  string `json:"type"`
	Label string `json:"label"`
}

// FetchCustomProfiles loads all custom profile mapping headers
func (r *ProjectRepository) FetchCustomProfiles(ctx context.Context) (map[string]map[string]map[string]string, error) {
	// Returns map: ProfileID -> ("paid" / "payout") -> (RawCol -> StandardCol)
	query := `SELECT id, name, payout_columns, paid_columns FROM custom_profile_mapings`
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]map[string]map[string]string)
	for rows.Next() {
		var id, name string
		var payoutRaw, paidRaw sql.NullString
		if err := rows.Scan(&id, &name, &payoutRaw, &paidRaw); err != nil {
			continue
		}

		result[id] = make(map[string]map[string]string)
		result[id]["paid"] = make(map[string]string)
		result[id]["payout"] = make(map[string]string)

		if paidRaw.Valid && paidRaw.String != "" {
			var cols []profileColumnItem
			if err := json.Unmarshal([]byte(paidRaw.String), &cols); err == nil {
				for _, col := range cols {
					if col.Label != "" && col.Name != "" {
						result[id]["paid"][col.Label] = col.Name
					}
				}
			}
		}

		if payoutRaw.Valid && payoutRaw.String != "" {
			var cols []profileColumnItem
			if err := json.Unmarshal([]byte(payoutRaw.String), &cols); err == nil {
				for _, col := range cols {
					if col.Label != "" && col.Name != "" {
						result[id]["payout"][col.Label] = col.Name
					}
				}
			}
		}
	}

	return result, nil
}
