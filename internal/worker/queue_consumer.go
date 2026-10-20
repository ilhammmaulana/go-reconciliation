package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"go-reconciliation/internal/domain"
)

type QueueConsumer struct {
	db              *sql.DB
	redisCli        *redis.Client
	pool            *WorkerPool
	mysqlQueueName  string
	redisQueueName  string
	pollIntervalSec int
}

func NewQueueConsumer(
	db *sql.DB,
	redisCli *redis.Client,
	pool *WorkerPool,
	mysqlQueueName string,
	redisQueueName string,
	pollIntervalSec int,
) *QueueConsumer {
	return &QueueConsumer{
		db:              db,
		redisCli:        redisCli,
		pool:            pool,
		mysqlQueueName:  mysqlQueueName,
		redisQueueName:  redisQueueName,
		pollIntervalSec: pollIntervalSec,
	}
}

// StartMySQLJobsPoller polls Laravel's `jobs` table every 5 seconds
func (c *QueueConsumer) StartMySQLJobsPoller(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(time.Duration(c.pollIntervalSec) * time.Second)
	defer ticker.Stop()

	log.Printf("[MySQL Jobs Poller] Polling table 'jobs' every %d seconds for queue: '%s'", c.pollIntervalSec, c.mysqlQueueName)

	for {
		select {
		case <-ctx.Done():
			log.Println("[MySQL Jobs Poller] Stopping...")
			return
		case <-ticker.C:
			c.pollMySQLJobs(ctx)
		}
	}
}

func (c *QueueConsumer) pollMySQLJobs(ctx context.Context) {
	now := time.Now().Unix()

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return
	}
	defer tx.Rollback()

	// Select and lock pending jobs for this queue
	query := `
		SELECT id, payload
		FROM jobs
		WHERE queue = ? AND (reserved_at IS NULL OR reserved_at <= ?) AND available_at <= ?
		ORDER BY id ASC
		LIMIT 5
		FOR UPDATE
	`

	rows, err := tx.QueryContext(ctx, query, c.mysqlQueueName, now-300, now)
	if err != nil {
		return
	}
	defer rows.Close()

	var jobIDs []int64
	var payloads []string

	for rows.Next() {
		var id int64
		var payload string
		if err := rows.Scan(&id, &payload); err == nil {
			jobIDs = append(jobIDs, id)
			payloads = append(payloads, payload)
		}
	}

	if len(jobIDs) == 0 {
		return
	}

	// Delete claimed jobs from table
	for _, id := range jobIDs {
		_, _ = tx.ExecContext(ctx, "DELETE FROM jobs WHERE id = ?", id)
	}

	if err := tx.Commit(); err != nil {
		log.Printf("[MySQL Jobs Error] Failed to commit job claim: %v", err)
		return
	}

	// Process payloads
	for i, rawPayload := range payloads {
		log.Printf("[MySQL Jobs] Picked up job ID %d: %s", jobIDs[i], rawPayload)
		job := c.parseLaravelPayload(rawPayload)
		c.pool.EnqueueJob(job)
	}
}

// StartRedisConsumer listens for Redis jobs using BLPOP
func (c *QueueConsumer) StartRedisConsumer(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	if c.redisCli == nil {
		return
	}

	queueKeys := []string{
		"queues:" + c.redisQueueName,
		"queue:" + c.redisQueueName,
		c.redisQueueName,
	}

	log.Printf("[Redis Consumer] Listening for jobs on Redis keys: %v", queueKeys)

	for {
		select {
		case <-ctx.Done():
			log.Println("[Redis Consumer] Stopping...")
			return
		default:
			// BLPOP blocks for 5 seconds waiting for incoming jobs
			result, err := c.redisCli.BLPop(ctx, 5*time.Second, queueKeys...).Result()
			if err != nil {
				if err != redis.Nil && ctx.Err() == nil {
					// Transient network error
					time.Sleep(1 * time.Second)
				}
				continue
			}

			if len(result) >= 2 {
				rawPayload := result[1]
				log.Printf("[Redis Consumer] Received job from %s: %s", result[0], rawPayload)
				job := c.parseLaravelPayload(rawPayload)
				c.pool.EnqueueJob(job)
			}
		}
	}
}

// parseLaravelPayload extracts project/file data from Laravel JSON payload
func (c *QueueConsumer) parseLaravelPayload(raw string) domain.JobPayload {
	var job domain.JobPayload

	// Try direct JobPayload unmarshal first
	if err := json.Unmarshal([]byte(raw), &job); err == nil && (job.Action != "" || job.ProjectID != "" || job.FileID != "") {
		return job
	}

	// Try parsing standard Laravel Queue wrapper JSON
	var wrapper struct {
		Job  string `json:"job"`
		Data struct {
			ProjectID string `json:"project_id"`
			FileID    string `json:"file_id"`
			Action    string `json:"action"`
			Filename  string `json:"filename"`
		} `json:"data"`
	}

	if err := json.Unmarshal([]byte(raw), &wrapper); err == nil {
		job.ProjectID = wrapper.Data.ProjectID
		job.FileID = wrapper.Data.FileID
		job.Filename = wrapper.Data.Filename
		job.Action = wrapper.Data.Action
		if job.Action == "" {
			job.Action = "PROCESS_QUEUE_JOB"
		}
		return job
	}

	return domain.JobPayload{
		Action: "UNKNOWN_JOB",
	}
}
