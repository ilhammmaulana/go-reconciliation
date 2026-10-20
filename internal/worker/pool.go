package worker

import (
	"context"
	"log"
	"path/filepath"
	"sync"
	"time"

	"go-reconciliation/internal/domain"
	"go-reconciliation/internal/repository/mysql"
	"go-reconciliation/internal/service"
	"go-reconciliation/pkg/stringhelper"
)

type WorkerPool struct {
	scannerService  *service.ScannerService
	groupingService *service.GroupingService
	dispatcher      *service.JobDispatcher
	projectRepo     *mysql.ProjectRepository
	fileRepo        *mysql.FileRepository
	semaphore       chan struct{}
	pollInterval    time.Duration
	highLoadIDs     map[string]bool
	jobQueue        chan domain.JobPayload
	nasHomePath     string
}

func NewWorkerPool(
	scannerService *service.ScannerService,
	groupingService *service.GroupingService,
	dispatcher *service.JobDispatcher,
	projectRepo *mysql.ProjectRepository,
	fileRepo *mysql.FileRepository,
	maxConcurrent int,
	pollIntervalSec int,
	highLoadIDs []string,
	nasHomePath string,
) *WorkerPool {
	hlMap := make(map[string]bool)
	for _, id := range highLoadIDs {
		hlMap[id] = true
	}

	return &WorkerPool{
		scannerService:  scannerService,
		groupingService: groupingService,
		dispatcher:      dispatcher,
		projectRepo:     projectRepo,
		fileRepo:        fileRepo,
		semaphore:       make(chan struct{}, maxConcurrent),
		pollInterval:    time.Duration(pollIntervalSec) * time.Second,
		highLoadIDs:     hlMap,
		jobQueue:        make(chan domain.JobPayload, 1000),
		nasHomePath:     nasHomePath,
	}
}

// EnqueueJob pushes on-demand jobs into memory channel
func (w *WorkerPool) EnqueueJob(job domain.JobPayload) {
	w.jobQueue <- job
}

// RecoverInterruptedFilesOnStartup recovers any leftover WAITING / ONPROGRESS files from database
func (w *WorkerPool) RecoverInterruptedFilesOnStartup(ctx context.Context) {
	log.Println("[Startup Recovery] Checking MySQL for pending or interrupted jobs...")

	files, err := w.fileRepo.FetchPendingOrInterruptedFiles(ctx)
	if err != nil {
		log.Printf("[Startup Recovery Error] Could not query pending files: %v", err)
		return
	}

	if len(files) == 0 {
		log.Println("[Startup Recovery] No interrupted jobs found. System state is clean.")
		return
	}

	log.Printf("[Startup Recovery] Found %d interrupted/waiting files from previous run. Recovering...", len(files))

	for _, fch := range files {
		log.Printf("[Startup Recovery] Re-queueing file: %s (ID: %s, Status: %d)", fch.Filename, fch.ID, fch.Status)
		w.EnqueueJob(domain.JobPayload{
			Action:    "RERUN_FILE",
			FileID:    fch.ID,
			ProjectID: fch.ProjectID,
			VendorID:  fch.VendorID,
			Filename:  fch.Filename,
		})
	}
}

// Start launches the background polling and worker goroutines
func (w *WorkerPool) Start(ctx context.Context, wg *sync.WaitGroup) {
	// 1. Run Startup Recovery first
	w.RecoverInterruptedFilesOnStartup(ctx)

	// 2. Start Background NAS Poller
	wg.Add(1)
	go w.runNormalPoller(ctx, wg)

	// 3. Start On-Demand Job Consumer
	wg.Add(1)
	go w.runJobQueueConsumer(ctx, wg)
}

// runNormalPoller continuously checks NAS for pending files
func (w *WorkerPool) runNormalPoller(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()

	log.Println("[WorkerPool] Background NAS Poller started")

	for {
		select {
		case <-ctx.Done():
			log.Println("[WorkerPool] Stopping NAS Poller...")
			return
		case <-ticker.C:
			w.pollAndProcess(ctx)
		}
	}
}

// runJobQueueConsumer listens for on-demand jobs (Rerun / HTTP Fast Trigger)
func (w *WorkerPool) runJobQueueConsumer(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Println("[WorkerPool] Fast HTTP / Job Consumer started")

	for {
		select {
		case <-ctx.Done():
			return
		case job := <-w.jobQueue:
			if job.FileID != "" {
				log.Printf("[Fast Trigger] Processing specific File ID: %s", job.FileID)
				w.processSingleFileByID(ctx, job.FileID)
			} else if job.ProjectID > 0 {
				log.Printf("[Fast Trigger] Processing Project ID: %d", job.ProjectID)
				w.processSingleProject(ctx, job.ProjectID)
			}
		}
	}
}

func (w *WorkerPool) processSingleFileByID(ctx context.Context, fileID string) {
	fch, err := w.fileRepo.FetchFileByID(ctx, fileID)
	if err != nil || fch == nil {
		log.Printf("[Fast Trigger Error] File not found for ID %s: %v", fileID, err)
		return
	}

	// Update status to ONPROGRESS in DB
	_ = w.fileRepo.UpdateFileStatus(ctx, fileID, domain.FileHistoryOnProgress, "Processing via Fast Trigger")

	vps, err := w.projectRepo.FetchProjectVendors(ctx, []string{fch.ProjectID})
	if err != nil || len(vps) == 0 {
		return
	}

	var targetVP *domain.VendorProject
	for _, vp := range vps {
		if vp.VendorID == fch.VendorID {
			targetVP = vp
			break
		}
	}
	if targetVP == nil {
		targetVP = vps[0]
	}

	// Determine file full path
	nasPath := stringhelper.JoinPaths(w.nasHomePath, targetVP.RelativePath)
	fullPath := filepath.Join(nasPath, fch.Filename)

	scanned := &domain.ScannedFile{
		FullPath:      fullPath,
		Filename:      fch.Filename,
		CleanFilename: stringhelper.CleanFilename(fch.Filename),
		Extension:     "csv",
		Type:          stringhelper.ExtractTransactionTypeFromFilename(fch.Filename),
		Date:          fch.Date,
		MD5:           fch.MD5,
		ProjectID:     targetVP.ID,
		VendorID:      targetVP.VendorID,
		VendorPath:    targetVP.VendorPath,
		RelativePath:  targetVP.RelativePath,
		NasPath:       nasPath,
	}

	profiles, _ := w.projectRepo.FetchCustomProfiles(ctx)

	dates, err := w.groupingService.ProcessCsvFile(ctx, scanned, targetVP, profiles)
	if err == nil && len(dates) > 0 {
		queueName := domain.QueueReconcile
		if w.highLoadIDs[targetVP.ID] {
			queueName = domain.QueueReconcileHigh
		}
		_ = w.dispatcher.DispatchReconcileJob(ctx, targetVP.ID, queueName)
	}
}

func (w *WorkerPool) pollAndProcess(ctx context.Context) {
	vps, err := w.projectRepo.FetchProjectVendors(ctx, nil)
	if err != nil || len(vps) == 0 {
		return
	}

	profiles, _ := w.projectRepo.FetchCustomProfiles(ctx)

	projectMap := make(map[string][]*domain.VendorProject)
	for _, vp := range vps {
		projectMap[vp.ID] = append(projectMap[vp.ID], vp)
	}

	for projectID, vendors := range projectMap {
		// 1. Check if this project is already being processed
		isOnProgress, _ := w.fileRepo.CheckIsOnProgress(ctx, []string{projectID})
		if isOnProgress {
			log.Printf("[WorkerPool] Project %s already on progress. Skipping this cycle.", projectID)
			continue
		}

		// 2. Scan files for all vendors under this project
		type vendorFilePair struct {
			file   *domain.ScannedFile
			vendor *domain.VendorProject
		}
		var filesToProcess []vendorFilePair

		for _, vp := range vendors {
			files, err := w.scannerService.ScanVendorDirectory(vp, 50)
			if err != nil || len(files) == 0 {
				continue
			}

			for _, file := range files {
				if file.Extension == "csv" {
					filesToProcess = append(filesToProcess, vendorFilePair{
						file:   file,
						vendor: vp,
					})
				}
			}
		}

		if len(filesToProcess) == 0 {
			continue
		}

		log.Printf("[WorkerPool] Found %d CSV files to process for Project %d (%s)", len(filesToProcess), projectID, vendors[0].ProjectName)

		// 3. Process each file
		reconcileDates := make(map[string][]string) // type -> list of dates
		processedCount := 0

		for _, pair := range filesToProcess {
			w.semaphore <- struct{}{} // Acquire concurrency slot

			dates, err := w.groupingService.ProcessCsvFile(ctx, pair.file, pair.vendor, profiles)
			<-w.semaphore // Release slot

			if err == nil && len(dates) > 0 {
				processedCount++
				trxType := pair.file.Type
				for _, d := range dates {
					// Check if already in slice
					found := false
					for _, existing := range reconcileDates[trxType] {
						if existing == d {
							found = true
							break
						}
					}
					if !found {
						reconcileDates[trxType] = append(reconcileDates[trxType], d)
					}
				}
			}
		}

		if processedCount == 0 {
			continue
		}

		// 4. Dispatch ReconcileJob for the project
		queueName := domain.QueueReconcile
		vendorQueueName := domain.QueueReconcileVendor
		if w.highLoadIDs[projectID] {
			queueName = domain.QueueReconcileHigh
			vendorQueueName = domain.QueueReconcileVendorHigh
		}
		_ = w.dispatcher.DispatchReconcileJob(ctx, projectID, queueName)

		// 5. Auto Create & Update Reconcile By Vendor
		shouldRerunReconVendor := false
		for trxType, dates := range reconcileDates {
			if len(dates) == 0 {
				continue
			}

			if vendors[0].AutoReconcileVendorID != nil {
				created, err := w.fileRepo.AutoCreateReconByVendor(ctx, projectID, *vendors[0].AutoReconcileVendorID, vendors[0].ProjectName, dates, trxType, vendors)
				if err == nil && created {
					shouldRerunReconVendor = true
				}
			}

			updated, err := w.fileRepo.UpdateReconcileVendorsWaiting(ctx, projectID, trxType, dates)
			if err == nil && updated > 0 {
				shouldRerunReconVendor = true
				log.Printf("[ReconcileVendor] Updated %d existing reconcile_vendors to WAITING for project %d (%s)", updated, projectID, trxType)
			}
		}

		if shouldRerunReconVendor {
			_ = w.dispatcher.DispatchReconcileVendorJob(ctx, projectID, vendorQueueName)
		}
	}
}

func (w *WorkerPool) processSingleProject(ctx context.Context, projectID string) {
	vps, err := w.projectRepo.FetchProjectVendors(ctx, []string{projectID})
	if err != nil || len(vps) == 0 {
		return
	}

	profiles, _ := w.projectRepo.FetchCustomProfiles(ctx)

	for _, vp := range vps {
		files, err := w.scannerService.ScanVendorDirectory(vp, 100)
		if err != nil {
			continue
		}

		for _, file := range files {
			if file.Extension == "csv" {
				_, _ = w.groupingService.ProcessCsvFile(ctx, file, vp, profiles)
			}
		}
	}
}
