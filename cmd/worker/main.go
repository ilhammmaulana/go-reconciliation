package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"go-reconciliation/internal/config"
	"go-reconciliation/internal/db"
	"go-reconciliation/internal/logger"
	"go-reconciliation/internal/queue"
	"go-reconciliation/internal/reconcile"
	vendor_reconcile "go-reconciliation/internal/vendor_reconcile"
)

func main() {
	// -----------------------------------------------------------------------
	// CLI Flags
	// -----------------------------------------------------------------------
	var (
		mode        = flag.String("mode", "reconcile", "Worker mode: reconcile | reconcile-vendor | reconcile-high-load | reconcile-vendor-high-load")
		envFile     = flag.String("env", ".env", "Path to .env file (default .env in current directory or relative to binary)")
		concurrency = flag.Int("concurrency", 0, "Number of concurrent goroutine workers (0 = use GO_WORKER_CONCURRENCY from .env)")
	)
	flag.Parse()

	// -----------------------------------------------------------------------
	// Init
	// -----------------------------------------------------------------------
	logger.InitWithName("worker-" + *mode)
	logger.Info.Printf("=== Go Reconciliation Engine ===")
	logger.Info.Printf("Mode: %s | PID: %d", *mode, os.Getpid())

	// Resolve .env path:
	// 1. If absolute, use directly.
	// 2. Check current working directory.
	// 3. Check relative to binary location or parent (for bin/ builds).
	envPath := *envFile
	if !filepath.IsAbs(envPath) {
		if _, err := os.Stat(envPath); err != nil {
			// Not found in cwd, try relative to binary directory
			if exe, err := os.Executable(); err == nil {
				binDir := filepath.Dir(exe)
				candidates := []string{
					filepath.Join(binDir, envPath),
					filepath.Join(binDir, "..", envPath),
					filepath.Join(binDir, "../..", envPath),
				}
				for _, candidate := range candidates {
					if _, err := os.Stat(candidate); err == nil {
						envPath = candidate
						break
					}
				}
			}
		}
	}

	// -----------------------------------------------------------------------
	// Config
	// -----------------------------------------------------------------------
	cfg := config.Load(envPath)

	if *concurrency > 0 {
		cfg.WorkerConcurrency = *concurrency
	}
	logger.Info.Printf("Concurrency: %d workers", cfg.WorkerConcurrency)

	// -----------------------------------------------------------------------
	// Database
	// -----------------------------------------------------------------------
	database, err := db.Connect(cfg)
	if err != nil {
		logger.Error.Fatalf("Database connection error: %v", err)
	}
	defer database.Close()

	// -----------------------------------------------------------------------
	// Redis
	// -----------------------------------------------------------------------
	redisClient, err := queue.NewClient(cfg)
	if err != nil {
		logger.Error.Fatalf("Redis connection error: %v", err)
	}
	defer redisClient.Close()

	// -----------------------------------------------------------------------
	// Context with graceful shutdown
	// -----------------------------------------------------------------------
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		s := <-sig
		logger.Info.Printf("Received signal %s — shutting down gracefully...", s)
		cancel()
	}()

	// -----------------------------------------------------------------------
	// Start workers based on mode
	// -----------------------------------------------------------------------
	queueName, isVendor := resolveQueue(*mode)

	logger.Info.Printf("Listening on queue: %s", queueName)

	var wg sync.WaitGroup
	for i := 0; i < cfg.WorkerConcurrency; i++ {
		wg.Add(1)
		workerID := i + 1
		go func() {
			defer wg.Done()
			if isVendor {
				runVendorWorker(ctx, workerID, queueName, redisClient, database, cfg)
			} else {
				runReconcileWorker(ctx, workerID, queueName, redisClient, database, cfg)
			}
		}()
	}

	wg.Wait()
	logger.Info.Printf("All workers stopped. Goodbye.")
}

// -----------------------------------------------------------------------
// Worker loops
// -----------------------------------------------------------------------

func runReconcileWorker(ctx context.Context, id int, queueName string, rdb *queue.Client, database *db.DB, cfg *config.Config) {
	svc := reconcile.New(database, cfg)
	logger.Info.Printf("[WORKER-%d] Reconcile worker started on queue=%s", id, queueName)

	for {
		select {
		case <-ctx.Done():
			logger.Info.Printf("[WORKER-%d] Stopping...", id)
			return
		default:
		}

		payload, rawJob, err := rdb.PopReconcileJob(ctx, queueName, 3*time.Second)
		if err != nil {
			if ctx.Err() != nil {
				return // context cancelled
			}
			logger.Error.Printf("[WORKER-%d] Pop error: %v", id, err)
			time.Sleep(2 * time.Second)
			continue
		}
		if payload == nil {
			continue // BLPOP timeout — no job, loop again
		}

		logger.Info.Printf("[WORKER-%d] Got job uuid=%s projectId=%s", id, payload.JobUUID, payload.ProjectID)

		if err := svc.Handle(ctx, payload); err != nil {
			logger.Error.Printf("[WORKER-%d] Job failed uuid=%s err=%v", id, payload.JobUUID, err)
			// Push to failed queue for inspection — Laravel will see it in Redis
			_ = rdb.PushFailed(ctx, queueName, rawJob, err.Error())
		}
	}
}

func runVendorWorker(ctx context.Context, id int, queueName string, rdb *queue.Client, database *db.DB, cfg *config.Config) {
	svc := vendor_reconcile.New(database, cfg)
	logger.Info.Printf("[WORKER-%d] ReconcileVendor worker started on queue=%s", id, queueName)

	for {
		select {
		case <-ctx.Done():
			logger.Info.Printf("[WORKER-%d] Stopping...", id)
			return
		default:
		}

		payload, rawJob, err := rdb.PopReconcileVendorJob(ctx, queueName, 3*time.Second)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			logger.Error.Printf("[WORKER-%d] Pop error: %v", id, err)
			time.Sleep(2 * time.Second)
			continue
		}
		if payload == nil {
			continue
		}

		logger.Info.Printf("[WORKER-%d] Got vendor job uuid=%s projectId=%s vendorReconcileId=%s",
			id, payload.JobUUID, payload.ProjectID, payload.ReconcileVendorID)

		if err := svc.Handle(ctx, payload); err != nil {
			logger.Error.Printf("[WORKER-%d] Vendor job failed uuid=%s err=%v", id, payload.JobUUID, err)
			_ = rdb.PushFailed(ctx, queueName, rawJob, err.Error())
		}
	}
}

// resolveQueue maps the --mode flag to a queue name and type
func resolveQueue(mode string) (queueName string, isVendor bool) {
	switch mode {
	case "reconcile":
		return queue.QueueReconcile, false
	case "reconcile-high-load":
		return queue.QueueReconcileHighLoad, false
	case "reconcile-vendor":
		return queue.QueueReconcileVendor, true
	case "reconcile-vendor-high-load":
		return queue.QueueReconcileVendorHL, true
	default:
		fmt.Fprintf(os.Stderr, "Unknown mode: %s\n", mode)
		os.Exit(1)
		return "", false
	}
}
