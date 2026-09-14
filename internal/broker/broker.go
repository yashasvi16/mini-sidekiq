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
	// PromoteDueJobs finds jobs in the scheduled set and each of the given
	// queues' retry sets whose time has arrived, and atomically moves each
	// one into its own active queue. Returns how many jobs were promoted.
	PromoteDueJobs(ctx context.Context, queues []string) (int, error)
	// GetJob fetches a single job by ID, or (nil, nil) if it doesn't exist.
	GetJob(ctx context.Context, id string) (*sidekiq.Job, error)
	// RetryDeadJob resets a dead job's attempts/error and moves it back into
	// its active queue. Returns an error if the job isn't currently dead.
	RetryDeadJob(ctx context.Context, id string) error
	// DeleteJob removes a job's record and any queue/retry/dead-letter
	// entries referencing it, wherever it currently is. A best-effort admin
	// operation - not used anywhere on the hot path.
	DeleteJob(ctx context.Context, id string) error
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
