package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/gin-gonic/gin"
	"go-reconciliation/internal/config"
	"go-reconciliation/internal/logger"
)

func setupTestRouter(apiKey string) (*gin.Engine, string) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	tempLogDir := filepath.Join(os.TempDir(), "test_go_rec_logs")
	_ = os.MkdirAll(tempLogDir, 0755)
	logger.InitWithName("test-server", tempLogDir)

	cfg := &config.Config{
		ApiKey: apiKey,
	}

	handler := NewHandler(nil, cfg)
	handler.RegisterRoutes(r)

	return r, tempLogDir
}

func TestHealthCheck(t *testing.T) {
	r, _ := setupTestRouter("valid-key")

	req, _ := http.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", w.Code)
	}

	var resp map[string]string
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["status"] != "healthy" {
		t.Errorf("expected healthy status, got %v", resp)
	}
}

func TestClearLogsUnauthorized(t *testing.T) {
	r, _ := setupTestRouter("secret-api-key")

	// Call without key
	req, _ := http.NewRequest(http.MethodPost, "/api/v1/logs/clear", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized, got %d", w.Code)
	}

	// Call with wrong key
	reqWrong, _ := http.NewRequest(http.MethodPost, "/api/v1/logs/clear", nil)
	reqWrong.Header.Set("X-API-KEY", "wrong-key")
	wWrong := httptest.NewRecorder()
	r.ServeHTTP(wWrong, reqWrong)

	if wWrong.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized, got %d", wWrong.Code)
	}
}

func TestClearLogsSuccess(t *testing.T) {
	r, _ := setupTestRouter("secret-api-key")

	req, _ := http.NewRequest(http.MethodPost, "/api/v1/logs/clear", nil)
	req.Header.Set("X-API-KEY", "secret-api-key")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d, body: %s", w.Code, w.Body.String())
	}

	var resp map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["status"] != "success" {
		t.Errorf("expected success status, got %v", resp)
	}
}
