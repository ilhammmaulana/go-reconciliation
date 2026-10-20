package logger

import (
	"os"
	"path/filepath"
	"testing"
)

func TestInitWithNameAndClearLogs(t *testing.T) {
	tempLogDir := filepath.Join(t.TempDir(), "logs")

	// 1. Initialize custom worker log
	InitWithName("worker-test-reconcile", tempLogDir)
	Info.Println("Testing worker log output")
	Error.Println("Testing worker error output")

	logFilePath := filepath.Join(tempLogDir, "worker-test-reconcile.log")
	data, err := os.ReadFile(logFilePath)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}
	if len(data) == 0 {
		t.Fatalf("Log file is empty, expected content")
	}

	// 2. Clear logs
	cleared, err := ClearLogs(tempLogDir)
	if err != nil {
		t.Fatalf("ClearLogs failed: %v", err)
	}
	if len(cleared) != 1 || cleared[0] != "worker-test-reconcile.log" {
		t.Errorf("Unexpected cleared files list: %v", cleared)
	}

	dataAfter, err := os.ReadFile(logFilePath)
	if err != nil {
		t.Fatalf("Failed to read log file after clear: %v", err)
	}
	if len(dataAfter) != 0 {
		t.Errorf("Expected truncated 0-byte file, got size %d", len(dataAfter))
	}
}
