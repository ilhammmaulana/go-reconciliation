package logger

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
)

var (
	Info   *log.Logger
	Warn   *log.Logger
	Error  *log.Logger
	Debug  *log.Logger
	activeFile *os.File
	mu     sync.Mutex
	logDirPath = "logs"
)

// Init initializes the default console logger
func Init() {
	InitWithName("default")
}

// InitWithName initializes logger outputting to both console and a specific log file in logsDir
func InitWithName(name string, customLogDir ...string) {
	mu.Lock()
	defer mu.Unlock()

	dir := logDirPath
	if len(customLogDir) > 0 && customLogDir[0] != "" {
		dir = customLogDir[0]
	}
	logDirPath = dir

	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Printf("[WARN] Failed to create log directory '%s': %v", dir, err)
	}

	flags := log.Ldate | log.Ltime | log.Lshortfile

	logFilePath := filepath.Join(dir, fmt.Sprintf("%s.log", name))
	f, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Printf("[WARN] Failed to open log file '%s': %v (logging to console only)", logFilePath, err)
		Info = log.New(os.Stdout, "[INFO]  ", flags)
		Warn = log.New(os.Stdout, "[WARN]  ", flags)
		Error = log.New(os.Stderr, "[ERROR] ", flags)
		Debug = log.New(os.Stdout, "[DEBUG] ", flags)
		return
	}

	if activeFile != nil {
		_ = activeFile.Close()
	}
	activeFile = f

	outWriter := io.MultiWriter(os.Stdout, f)
	errWriter := io.MultiWriter(os.Stderr, f)

	Info = log.New(outWriter, "[INFO]  ", flags)
	Warn = log.New(outWriter, "[WARN]  ", flags)
	Error = log.New(errWriter, "[ERROR] ", flags)
	Debug = log.New(outWriter, "[DEBUG] ", flags)

	// Also redirect standard log output
	log.SetOutput(outWriter)
	log.SetFlags(flags)
}

// ClearLogs truncates or removes all log files in the configured log directory
func ClearLogs(customLogDir ...string) ([]string, error) {
	mu.Lock()
	defer mu.Unlock()

	dir := logDirPath
	if len(customLogDir) > 0 && customLogDir[0] != "" {
		dir = customLogDir[0]
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read log dir %s: %w", dir, err)
	}

	var cleared []string
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".log" {
			continue
		}

		fullPath := filepath.Join(dir, entry.Name())
		// Truncate file so any active file descriptors remain valid
		if err := os.Truncate(fullPath, 0); err != nil {
			log.Printf("[WARN] Failed to truncate log file %s: %v", fullPath, err)
			continue
		}
		cleared = append(cleared, entry.Name())
	}

	return cleared, nil
}
