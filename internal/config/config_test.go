package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestConfigLoad(t *testing.T) {
	tempDir := t.TempDir()
	envPath := filepath.Join(tempDir, ".env")

	content := `APP_ENV=testing
APP_PORT=9999
API_KEY=test-secret-key-123
DB_HOST=127.0.0.1
DB_PORT=3306
DB_USERNAME=root
DB_PASSWORD=secret
DB_DATABASE=test_db
HIGH_LOAD_PROJECT_IDS=101,102,103
`
	if err := os.WriteFile(envPath, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write mock env: %v", err)
	}

	cfg := Load(envPath)
	if cfg.AppEnv != "testing" {
		t.Errorf("expected AppEnv=testing, got %s", cfg.AppEnv)
	}
	if cfg.AppPort != "9999" {
		t.Errorf("expected AppPort=9999, got %s", cfg.AppPort)
	}
	if cfg.ApiKey != "test-secret-key-123" {
		t.Errorf("expected ApiKey=test-secret-key-123, got %s", cfg.ApiKey)
	}
	if len(cfg.HighLoadProjectIDs) != 3 {
		t.Errorf("expected 3 high load ids, got %d", len(cfg.HighLoadProjectIDs))
	}
}
