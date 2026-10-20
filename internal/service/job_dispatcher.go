package service

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/redis/go-redis/v9"
	"go-reconciliation/internal/domain"
)

type JobDispatcher struct {
	db       *sql.DB
	redisCli *redis.Client
	useRedis bool
}

func NewJobDispatcher(db *sql.DB, redisCli *redis.Client, useRedis bool) *JobDispatcher {
	return &JobDispatcher{
		db:       db,
		redisCli: redisCli,
		useRedis: useRedis,
	}
}

func (d *JobDispatcher) DispatchReconcileJob(ctx context.Context, projectID string, queueName string) error {
	if queueName == "" {
		queueName = domain.QueueReconcile
	}

	log.Printf("[Dispatcher] Dispatching ReconcileJob for project %s on queue: %s", projectID, queueName)

	if d.useRedis && d.redisCli != nil {
		// Redis dispatch
		payloadJSON := fmt.Sprintf(`{"job":"App\\Jobs\\ReconcileJob","data":{"projectId":%q}}`, projectID)
		return d.redisCli.RPush(ctx, "queues:"+queueName, payloadJSON).Err()
	}

	// MySQL Database Queue dispatch (inserts into jobs table)
	query := `
		INSERT INTO jobs (queue, payload, attempts, reserved_at, available_at, created_at)
		VALUES (?, ?, 0, NULL, UNIX_TIMESTAMP(), UNIX_TIMESTAMP())
	`
	payload := fmt.Sprintf(`{"displayName":"App\\Jobs\\ReconcileJob","job":"Illuminate\\Queue\\CallQueuedHandler@call","maxTries":null,"data":{"commandName":"App\\Jobs\\ReconcileJob","command":"O:20:\"App\\Jobs\\ReconcileJob\":1:{s:9:\"projectId\";s:%d:\"%s\";}"}}`, len(projectID), projectID)

	_, err := d.db.ExecContext(ctx, query, queueName, payload)
	return err
}

// DispatchReconcileVendorJob triggers RunReconcileVendorJob in Laravel
func (d *JobDispatcher) DispatchReconcileVendorJob(ctx context.Context, projectID string, queueName string) error {
	if queueName == "" {
		queueName = domain.QueueReconcileVendor
	}

	log.Printf("[Dispatcher] Dispatching RunReconcileVendorJob for project %s on queue: %s", projectID, queueName)

	if d.useRedis && d.redisCli != nil {
		payloadJSON := fmt.Sprintf(`{"job":"App\\Jobs\\RunReconcileVendorJob","data":{"projectId":%q}}`, projectID)
		return d.redisCli.RPush(ctx, "queues:"+queueName, payloadJSON).Err()
	}

	query := `
		INSERT INTO jobs (queue, payload, attempts, reserved_at, available_at, created_at)
		VALUES (?, ?, 0, NULL, UNIX_TIMESTAMP(), UNIX_TIMESTAMP())
	`
	payload := fmt.Sprintf(`{"displayName":"App\\Jobs\\RunReconcileVendorJob","job":"Illuminate\\Queue\\CallQueuedHandler@call","maxTries":null,"data":{"commandName":"App\\Jobs\\RunReconcileVendorJob","command":"O:29:\"App\\Jobs\\RunReconcileVendorJob\":1:{s:9:\"projectId\";s:%d:\"%s\";}"}}`, len(projectID), projectID)

	_, err := d.db.ExecContext(ctx, query, queueName, payload)
	return err
}
