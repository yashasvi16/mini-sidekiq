package worker

import (
	"context"
	"sync"
	"time"

	"sidekiq"
	"sidekiq/internal/broker"
)

// fakeBroker is a minimal in-memory Broker used to test the worker pool in
// isolation, without needing a real Redis instance.
type fakeBroker struct {
	mu           sync.Mutex
	acked        []string
	requeued     []requeueCall
	deadLettered []string
}

type requeueCall struct {
	jobID string
	delay time.Duration
}

func newFakeBroker() *fakeBroker {
	return &fakeBroker{}
}

func (f *fakeBroker) Enqueue(ctx context.Context, job *sidekiq.Job) error { return nil }

func (f *fakeBroker) Dequeue(ctx context.Context, queues []string) (*sidekiq.Job, error) {
	return nil, nil
}

func (f *fakeBroker) Acknowledge(ctx context.Context, job *sidekiq.Job) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acked = append(f.acked, job.ID)
	return nil
}

func (f *fakeBroker) Requeue(ctx context.Context, job *sidekiq.Job, delay time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requeued = append(f.requeued, requeueCall{jobID: job.ID, delay: delay})
	return nil
}

func (f *fakeBroker) MoveToDeadLetter(ctx context.Context, job *sidekiq.Job) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deadLettered = append(f.deadLettered, job.ID)
	return nil
}

func (f *fakeBroker) GetJobsByStatus(ctx context.Context, status sidekiq.JobStatus, limit int) ([]*sidekiq.Job, error) {
	return nil, nil
}

func (f *fakeBroker) GetQueueStats(ctx context.Context) (map[string]broker.QueueStats, error) {
	return nil, nil
}

func (f *fakeBroker) PromoteDueJobs(ctx context.Context, queues []string) (int, error) {
	return 0, nil
}

func (f *fakeBroker) GetJob(ctx context.Context, id string) (*sidekiq.Job, error) { return nil, nil }
func (f *fakeBroker) RetryDeadJob(ctx context.Context, id string) error           { return nil }
func (f *fakeBroker) DeleteJob(ctx context.Context, id string) error              { return nil }

func (f *fakeBroker) Close() error { return nil }

func (f *fakeBroker) counts() (acked, requeued, dead int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.acked), len(f.requeued), len(f.deadLettered)
}
