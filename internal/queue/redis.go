package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"go-reconciliation/internal/config"
	"go-reconciliation/internal/logger"
	"github.com/redis/go-redis/v9"
)

// Laravel Redis queue key format: laravel_database_{queueName}
const laravelQueuePrefix = "laravel_database_"

// Queue names matching app/Constants/Jobs.php
const (
	QueueReconcile             = "reconcile"
	QueueReconcileHighLoad     = "reconcile-high-load"
	QueueReconcileVendor       = "reconcile-vendor"
	QueueReconcileVendorHL     = "reconcile-vendor-high-load"
)

// LaravelJob represents the outer envelope of a Laravel Redis queue job
type LaravelJob struct {
	UUID        string          `json:"uuid"`
	DisplayName string          `json:"displayName"`
	Job         string          `json:"job"`
	MaxTries    *int            `json:"maxTries"`
	Timeout     *int            `json:"timeout"`
	Data        LaravelJobData  `json:"data"`
	Attempts    int             `json:"attempts"`
}

// LaravelJobData holds the serialized command
type LaravelJobData struct {
	CommandName string `json:"commandName"`
	Command     string `json:"command"` // PHP serialized — we parse with regex
}

// ReconcileJobPayload is what we extract from a ReconcileJob
type ReconcileJobPayload struct {
	JobUUID   string
	QueueName string
	ProjectID string
	TargetDate string
	TargetType string
	UserID     string
}

// ReconcileVendorJobPayload is what we extract from a RunReconcileVendorJob
type ReconcileVendorJobPayload struct {
	JobUUID          string
	QueueName        string
	ProjectID        string
	ReconcileVendorID string
	UserID           string
}

// Client wraps go-redis client
type Client struct {
	rdb *redis.Client
}

// NewClient creates a Redis client from config
func NewClient(cfg *config.Config) (*Client, error) {
	addr := fmt.Sprintf("%s:%s", cfg.RedisHost, cfg.RedisPort)
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis at %s: %w", addr, err)
	}

	logger.Info.Printf("Connected to Redis: %s DB=%d", addr, cfg.RedisDB)
	return &Client{rdb: rdb}, nil
}

// Close shuts down the Redis connection
func (c *Client) Close() error {
	return c.rdb.Close()
}

// PopReconcileJob performs a blocking pop (BLPOP) from the given queue and
// returns a parsed ReconcileJobPayload. Blocks for up to blockDuration.
// Returns (nil, nil) on timeout (no job available).
func (c *Client) PopReconcileJob(ctx context.Context, queueName string, blockDuration time.Duration) (*ReconcileJobPayload, string, error) {
	key := laravelQueuePrefix + queueName
	result, err := c.rdb.BLPop(ctx, blockDuration, key).Result()
	if err == redis.Nil {
		return nil, "", nil // Timeout — no job
	}
	if err != nil {
		return nil, "", fmt.Errorf("BLPOP error on %s: %w", key, err)
	}

	rawJob := result[1] // result[0] is the key name, result[1] is the value
	payload, err := parseReconcileJob(rawJob, queueName)
	if err != nil {
		return nil, rawJob, fmt.Errorf("failed to parse reconcile job: %w", err)
	}

	return payload, rawJob, nil
}

// PopReconcileVendorJob pops and parses a RunReconcileVendorJob payload
func (c *Client) PopReconcileVendorJob(ctx context.Context, queueName string, blockDuration time.Duration) (*ReconcileVendorJobPayload, string, error) {
	key := laravelQueuePrefix + queueName
	result, err := c.rdb.BLPop(ctx, blockDuration, key).Result()
	if err == redis.Nil {
		return nil, "", nil
	}
	if err != nil {
		return nil, "", fmt.Errorf("BLPOP error on %s: %w", key, err)
	}

	rawJob := result[1]
	payload, err := parseReconcileVendorJob(rawJob, queueName)
	if err != nil {
		return nil, rawJob, fmt.Errorf("failed to parse vendor reconcile job: %w", err)
	}

	return payload, rawJob, nil
}

// PushBack re-queues a job (for retry) back to the front of the queue (LPUSH)
func (c *Client) PushBack(ctx context.Context, queueName string, rawJob string) error {
	key := laravelQueuePrefix + queueName
	return c.rdb.LPush(ctx, key, rawJob).Err()
}

// PushFailed pushes a failed job into the laravel failed jobs list
func (c *Client) PushFailed(ctx context.Context, queueName string, rawJob string, errMsg string) error {
	// In a production setup, you'd write to the failed_jobs MySQL table.
	// For now, log and push to a dedicated failed list.
	key := laravelQueuePrefix + queueName + ":failed"
	return c.rdb.RPush(ctx, key, rawJob).Err()
}

// QueueLength returns the current number of jobs waiting in a queue
func (c *Client) QueueLength(ctx context.Context, queueName string) (int64, error) {
	key := laravelQueuePrefix + queueName
	return c.rdb.LLen(ctx, key).Result()
}

// --------------------------------------------------------------------------
// Internal parsers — Laravel serializes PHP objects, we extract fields
// via JSON + regex on the serialized command string.
// --------------------------------------------------------------------------

// parseReconcileJob decodes a raw Laravel job JSON and extracts ReconcileJob fields.
// Laravel serializes job constructors as PHP serialized strings inside the "command" field.
// We parse the relevant string properties with regex to avoid a full PHP deserializer.
func parseReconcileJob(rawJob string, queueName string) (*ReconcileJobPayload, error) {
	var job LaravelJob
	if err := json.Unmarshal([]byte(rawJob), &job); err != nil {
		return nil, fmt.Errorf("JSON unmarshal error: %w", err)
	}

	if !strings.Contains(job.DisplayName, "ReconcileJob") {
		return nil, fmt.Errorf("unexpected job type: %s (expected ReconcileJob)", job.DisplayName)
	}

	payload := &ReconcileJobPayload{
		JobUUID:   job.UUID,
		QueueName: queueName,
	}

	// Extract string properties from PHP serialized command
	// PHP serialized string format: s:LENGTH:"VALUE";
	// Example: s:9:"projectId";s:36:"uuid-here";
	payload.ProjectID = extractPhpSerializedString(job.Data.Command, "projectId")
	payload.TargetDate = extractPhpSerializedString(job.Data.Command, "targetDate")
	payload.TargetType = extractPhpSerializedString(job.Data.Command, "targetType")
	payload.UserID = extractPhpSerializedString(job.Data.Command, "userId")

	if payload.ProjectID == "" {
		return nil, fmt.Errorf("could not extract projectId from command payload")
	}

	return payload, nil
}

func parseReconcileVendorJob(rawJob string, queueName string) (*ReconcileVendorJobPayload, error) {
	var job LaravelJob
	if err := json.Unmarshal([]byte(rawJob), &job); err != nil {
		return nil, fmt.Errorf("JSON unmarshal error: %w", err)
	}

	if !strings.Contains(job.DisplayName, "RunReconcileVendorJob") {
		return nil, fmt.Errorf("unexpected job type: %s (expected RunReconcileVendorJob)", job.DisplayName)
	}

	payload := &ReconcileVendorJobPayload{
		JobUUID:   job.UUID,
		QueueName: queueName,
	}

	payload.ProjectID = extractPhpSerializedString(job.Data.Command, "projectId")
	payload.ReconcileVendorID = extractPhpSerializedString(job.Data.Command, "reconcileVendorId")
	payload.UserID = extractPhpSerializedString(job.Data.Command, "userId")

	if payload.ProjectID == "" {
		return nil, fmt.Errorf("could not extract projectId from vendor job payload")
	}

	return payload, nil
}

// extractPhpSerializedString extracts the value of a named property from a PHP
// serialized string. For example, given property "projectId" and command:
//   ...s:9:"projectId";s:36:"some-uuid-here";...
// it returns "some-uuid-here".
func extractPhpSerializedString(command, property string) string {
	// Pattern: s:<propLen>:"<propName>";s:<valLen>:"<value>";
	// We match the property name then capture the value that follows.
	pattern := fmt.Sprintf(`s:\d+:"%s";(?:N;|s:\d+:"([^"]*)"(?:;|})?)`, regexp.QuoteMeta(property))
	re := regexp.MustCompile(pattern)
	matches := re.FindStringSubmatch(command)
	if len(matches) >= 2 {
		return matches[1] // capture group 1 = the value (empty string for N;)
	}
	return ""
}
