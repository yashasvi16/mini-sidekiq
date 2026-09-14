// Package client is the public API for Mini-Sidekiq: enqueuing jobs from
// any Go service (Client) and running a worker server that processes them
// (Server, in server.go).
package client

import (
	"context"
	"fmt"
	"time"

	"sidekiq"
	"sidekiq/internal/broker"
)

// P is shorthand for building a job payload as a plain map.
type P map[string]any

// Queues maps a queue name to its relative poll weight - see
// dispatcher.QueueConfig for what the weight controls.
type Queues map[string]int

// Config configures a Client's connection to the broker.
type Config struct {
	RedisAddr string // e.g. "localhost:6379"
}

// EnqueueOptions customizes how a single job is enqueued.
type EnqueueOptions struct {
	Queue       string        // defaults to "default"
	In          time.Duration // delay before the job becomes eligible to run
	MaxAttempts int           // defaults to 3 (sidekiq.NewJob's default) if zero
}

// Client enqueues jobs onto the broker for workers to process.
type Client struct {
	broker broker.Broker
}

// NewClient creates a Client backed by Redis at cfg.RedisAddr.
func NewClient(cfg Config) *Client {
	return &Client{broker: broker.NewRedisBroker(cfg.RedisAddr)}
}

// Enqueue builds and enqueues a new job of the given type with the given
// payload. It returns the created Job - its ID is already assigned by the
// time this returns, even before confirming the broker write succeeded,
// because IDs are generated client-side (see sidekiq.NewJob) specifically so
// a caller can safely retry a failed Enqueue call without risking a
// duplicate job under a different ID.
func (c *Client) Enqueue(ctx context.Context, jobType string, payload any, opts ...EnqueueOptions) (*sidekiq.Job, error) {
	queue := "default"
	var delay time.Duration
	var maxAttempts int
	if len(opts) > 0 {
		if opts[0].Queue != "" {
			queue = opts[0].Queue
		}
		delay = opts[0].In
		maxAttempts = opts[0].MaxAttempts
	}

	job, err := sidekiq.NewJob(queue, jobType, payload)
	if err != nil {
		return nil, fmt.Errorf("build job: %w", err)
	}
	if delay > 0 {
		job.ProcessAt = time.Now().Add(delay)
	}
	if maxAttempts > 0 {
		job.MaxAttempts = maxAttempts
	}

	if err := c.broker.Enqueue(ctx, job); err != nil {
		return nil, fmt.Errorf("enqueue: %w", err)
	}
	return job, nil
}

// Close releases the client's underlying broker connection.
func (c *Client) Close() error {
	return c.broker.Close()
}
