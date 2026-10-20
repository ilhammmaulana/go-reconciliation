package mysql

import (
	"context"
	"testing"
	"go-reconciliation/internal/config"
)

func TestLiveFetchProjectVendors(t *testing.T) {
	cfg := config.Load("/mount/Workspace/Projects/go-reconciliation/.env")
	db, err := InitDB(cfg)
	if err != nil {
		t.Skipf("DB not accessible: %v", err)
	}
	defer db.Close()

	repo := NewProjectRepository(db)
	vps, err := repo.FetchProjectVendors(context.Background(), nil)
	t.Logf("FetchProjectVendors: err=%v, count=%d", err, len(vps))
	if err != nil {
		t.Errorf("FetchProjectVendors failed: %v", err)
	}
}
