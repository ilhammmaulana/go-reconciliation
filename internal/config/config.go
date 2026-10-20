package config

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

type Config struct {
	// App & Server
	AppEnv  string
	AppPort string
	ApiKey  string

	// Database
	DBHost     string
	DBPort     string
	DBUser     string
	DBPass     string
	DBPassword string
	DBName     string
	DBDsn      string

	// Redis
	RedisHost     string
	RedisPort     string
	RedisPassword string
	RedisDB       int

	// Storage & Paths
	NasHomePath string
	ExtractPath string
	StoragePath string

	// Worker & Concurrency
	WorkerConcurrency       int
	MaxConcurrentFiles      int
	PollIntervalSeconds     int
	HighLoadProjectIDs      []string
	EnableRedisQueue        bool
	EnableMySQLJobsPolling  bool
	QueueDriver             string // "database", "redis", or "both"
	MySQLJobsQueueName      string
	RedisQueueName          string
	JobsPollIntervalSeconds int
}

// Load reads .env from optional envPath or defaults to .env in current/parent dir
func Load(envPaths ...string) *Config {
	envPath := ".env"
	if len(envPaths) > 0 && envPaths[0] != "" {
		envPath = envPaths[0]
	}

	if err := godotenv.Load(envPath); err != nil {
		log.Printf("[INFO] godotenv.Load from '%s': %v (using OS env / defaults)", envPath, err)
	}

	queueDriver := getEnv("QUEUE_DRIVER", "both")
	enableRedis := getEnvAsBool("ENABLE_REDIS_QUEUE", false) || queueDriver == "redis" || queueDriver == "both"
	enableMySQLJobs := getEnvAsBool("ENABLE_MYSQL_JOBS_POLLING", true) || queueDriver == "database" || queueDriver == "both"

	dbUser := getEnv("DB_USERNAME", "root")
	dbPass := getEnv("DB_PASSWORD", "")
	if dbPass == "null" {
		dbPass = ""
	}
	dbHost := getEnv("DB_HOST", "127.0.0.1")
	dbPort := getEnv("DB_PORT", "3306")
	dbName := getEnv("DB_DATABASE", "anihost_reconcile_test_2")

	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s?parseTime=true&loc=Asia%%2FJakarta&charset=utf8mb4",
		dbUser, dbPass, dbHost, dbPort, dbName,
	)

	redisPass := getEnv("REDIS_PASSWORD", "")
	if redisPass == "null" {
		redisPass = ""
	}

	projectRoot := filepath.Dir(envPath)
	storagePath := getEnv("STORAGE_PATH", filepath.Join(projectRoot, "storage"))

	concurrency := getEnvAsInt("GO_WORKER_CONCURRENCY", 5)

	cfg := &Config{
		AppEnv:                  getEnv("APP_ENV", "production"),
		AppPort:                 getEnv("APP_PORT", "8080"),
		ApiKey:                  getEnv("API_KEY", "secret-reconcile-key"),
		DBHost:                  dbHost,
		DBPort:                  dbPort,
		DBUser:                  dbUser,
		DBPass:                  dbPass,
		DBPassword:              dbPass,
		DBName:                  dbName,
		DBDsn:                   dsn,
		RedisHost:               getEnv("REDIS_HOST", "127.0.0.1"),
		RedisPort:               getEnv("REDIS_PORT", "6379"),
		RedisPassword:           redisPass,
		RedisDB:                 getEnvAsInt("REDIS_DB", 0),
		NasHomePath:             getEnv("NAS_HOME_PATH", "/mount/nas"),
		ExtractPath:             getEnv("EXTRACT_PATH", "storage/app/extract"),
		StoragePath:             storagePath,
		WorkerConcurrency:       concurrency,
		MaxConcurrentFiles:      getEnvAsInt("MAX_CONCURRENT_FILES", 8),
		PollIntervalSeconds:     getEnvAsInt("POLL_INTERVAL_SECONDS", 5),
		EnableRedisQueue:        enableRedis,
		EnableMySQLJobsPolling:  enableMySQLJobs,
		QueueDriver:             queueDriver,
		MySQLJobsQueueName:      getEnv("MYSQL_JOBS_QUEUE_NAME", "go_grouping"),
		RedisQueueName:          getEnv("REDIS_QUEUE_NAME", "go_grouping"),
		JobsPollIntervalSeconds: getEnvAsInt("JOBS_POLL_INTERVAL_SECONDS", 5),
	}

	highLoadStr := getEnv("HIGH_LOAD_PROJECT_IDS", "")
	if highLoadStr != "" {
		for _, idStr := range strings.Split(highLoadStr, ",") {
			if trimmed := strings.TrimSpace(idStr); trimmed != "" {
				cfg.HighLoadProjectIDs = append(cfg.HighLoadProjectIDs, trimmed)
			}
		}
	}

	return cfg
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func getEnvAsInt(key string, fallback int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return fallback
}

func getEnvAsBool(key string, fallback bool) bool {
	if val := os.Getenv(key); val != "" {
		return strings.ToLower(val) == "true" || val == "1"
	}
	return fallback
}
