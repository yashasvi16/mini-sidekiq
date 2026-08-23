package broker

import (
	"context"
	"sidekiq"
	"time"
)

type Broker interface {
	Enqueue(ctx context.Context, job *sidekiq.Job) error
	Dequeue(ctx context.Context, queues []string) (*sidekiq.Job, error)
	Acknowledge(ctx context.Context, job *sidekiq.Job) error
	Requeue(ctx context.Context, job *sidekiq.Job, delay time.Duration) error
	MoveToDeadLetter(ctx context.Context, job *sidekiq.Job) error
	GetJobsByStatus(ctx context.Context, status sidekiq.JobStatus, limit int) ([]*sidekiq.Job, error)
	GetQueueStats(ctx context.Context) (map[string]QueueStats, error)
	Close() error
}

type QueueStats struct {
	Queue     string `json:"queue"`
	Enqueued  int    `json:"enqueued"`
	Retry     int    `json:"retry"`
	Scheduled int    `json:"scheduled"`
	Active    int    `json:"active"`
	Error     int    `json:"error"`
	Dead      int    `json:"dead"`
}
