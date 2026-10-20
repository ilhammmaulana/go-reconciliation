package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"go-reconciliation/internal/api"
	"go-reconciliation/internal/config"
	"go-reconciliation/internal/logger"
	"go-reconciliation/internal/repository/mysql"
	"go-reconciliation/internal/service"
	"go-reconciliation/internal/worker"
)

func main() {
	// 0. Initialize Server Logger
	logger.InitWithName("server")

	log.Println("==================================================")
	log.Println(" Starting Go Reconciliation Ingestion Engine...")
	log.Println("==================================================")

	// 1. Load Configurations
	cfg := config.Load()
	if cfg.AppEnv == "production" {
		gin.SetMode(gin.ReleaseMode)
	}

	// 2. Initialize Database Connection
	db, err := mysql.InitDB(cfg)
	if err != nil {
		log.Fatalf("[Fatal] Failed to connect to MySQL: %v", err)
	}
	defer db.Close()
	log.Println("[MySQL] Connected successfully to database:", cfg.DBName)

	// 3. Optional Redis Client
	var redisCli *redis.Client
	if cfg.EnableRedisQueue {
		redisCli = redis.NewClient(&redis.Options{
			Addr:     fmt.Sprintf("%s:%s", cfg.RedisHost, cfg.RedisPort),
			Password: cfg.RedisPassword,
			DB:       cfg.RedisDB,
		})
		log.Println("[Redis] Initialized Redis connection for queue:", cfg.RedisQueueName)
	}

	// 4. Initialize Repositories & Services
	projectRepo := mysql.NewProjectRepository(db)
	fileRepo := mysql.NewFileRepository(db)

	scannerService := service.NewScannerService(cfg.NasHomePath)
	groupingService := service.NewGroupingService(fileRepo, projectRepo, cfg.ExtractPath, cfg.NasHomePath)
	dispatcher := service.NewJobDispatcher(db, redisCli, cfg.EnableRedisQueue)

	// 5. Initialize Worker Pool
	pool := worker.NewWorkerPool(
		scannerService,
		groupingService,
		dispatcher,
		projectRepo,
		fileRepo,
		cfg.MaxConcurrentFiles,
		cfg.PollIntervalSeconds,
		cfg.HighLoadProjectIDs,
		cfg.NasHomePath,
	)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	// Start Worker Pool (NAS Background Poller & In-Memory Job Queue)
	pool.Start(ctx, &wg)

	// 6. Initialize Queue Consumer (Dual Mode: Redis + MySQL Jobs Table Poller)
	queueConsumer := worker.NewQueueConsumer(
		db,
		redisCli,
		pool,
		cfg.MySQLJobsQueueName,
		cfg.RedisQueueName,
		cfg.JobsPollIntervalSeconds,
	)

	// Start MySQL Jobs Table Poller (every 5 seconds)
	if cfg.EnableMySQLJobsPolling {
		wg.Add(1)
		go queueConsumer.StartMySQLJobsPoller(ctx, &wg)
	}

	// Start Redis Queue Consumer (if Redis enabled)
	if cfg.EnableRedisQueue && redisCli != nil {
		wg.Add(1)
		go queueConsumer.StartRedisConsumer(ctx, &wg)
	}

	// 7. Setup Internal REST HTTP Server
	router := gin.Default()
	handler := api.NewHandler(pool, cfg)
	handler.RegisterRoutes(router)

	srv := &http.Server{
		Addr:    ":" + cfg.AppPort,
		Handler: router,
	}

	go func() {
		log.Printf("[HTTP Server] Listening on http://0.0.0.0:%s\n", cfg.AppPort)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[HTTP Error] %v", err)
		}
	}()

	// 8. Graceful Shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("[Shutdown] Received termination signal. Shutting down gracefully...")
	cancel() // Signal all goroutines to stop

	// Shutdown HTTP Server
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("[HTTP Server Shutdown Error] %v", err)
	}

	wg.Wait() // Wait for all running goroutines to finish current jobs
	log.Println("[Shutdown] Go Reconciliation Engine exited cleanly.")
}
