package dashboard

import (
	"context"
	"time"

	"sidekiq"
	"sidekiq/internal/broker"
)

// fakeBroker is an in-memory Broker for testing the dashboard's HTTP layer
// in isolation, without a real Redis/Postgres instance.
type fakeBroker struct {
	jobs         map[string]*sidekiq.Job
	stats        map[string]broker.QueueStats
	retryErr     error
	deleteErr    error
	getQueuesErr error
	getJobsErr   error
	retriedIDs   []string
	deletedIDs   []string
}

func newFakeBroker() *fakeBroker {
	return &fakeBroker{
		jobs:  make(map[string]*sidekiq.Job),
		stats: make(map[string]broker.QueueStats),
	}
}

func (f *fakeBroker) Enqueue(ctx context.Context, job *sidekiq.Job) error { return nil }
func (f *fakeBroker) Dequeue(ctx context.Context, queues []string) (*sidekiq.Job, error) {
	return nil, nil
}
func (f *fakeBroker) Acknowledge(ctx context.Context, job *sidekiq.Job) error { return nil }
func (f *fakeBroker) Requeue(ctx context.Context, job *sidekiq.Job, delay time.Duration) error {
	return nil
}
func (f *fakeBroker) MoveToDeadLetter(ctx context.Context, job *sidekiq.Job) error { return nil }
func (f *fakeBroker) PromoteDueJobs(ctx context.Context, queues []string) (int, error) {
	return 0, nil
}
func (f *fakeBroker) Close() error { return nil }

func (f *fakeBroker) GetJobsByStatus(ctx context.Context, status sidekiq.JobStatus, limit int) ([]*sidekiq.Job, error) {
	if f.getJobsErr != nil {
		return nil, f.getJobsErr
	}
	var out []*sidekiq.Job
	for _, j := range f.jobs {
		if j.Status == status {
			out = append(out, j)
		}
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (f *fakeBroker) GetQueueStats(ctx context.Context) (map[string]broker.QueueStats, error) {
	if f.getQueuesErr != nil {
		return nil, f.getQueuesErr
	}
	return f.stats, nil
}

func (f *fakeBroker) GetJob(ctx context.Context, id string) (*sidekiq.Job, error) {
	return f.jobs[id], nil
}

func (f *fakeBroker) RetryDeadJob(ctx context.Context, id string) error {
	if f.retryErr != nil {
		return f.retryErr
	}
	f.retriedIDs = append(f.retriedIDs, id)
	if job, ok := f.jobs[id]; ok {
		job.Status = sidekiq.StatusPending
		job.Attempts = 0
	}
	return nil
}

func (f *fakeBroker) DeleteJob(ctx context.Context, id string) error {
	if f.deleteErr != nil {
		return f.deleteErr
	}
	f.deletedIDs = append(f.deletedIDs, id)
	delete(f.jobs, id)
	return nil
}
