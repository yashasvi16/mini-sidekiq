package dispatcher

import (
	"context"
	"sync"
	"time"

	"sidekiq"
	"sidekiq/internal/broker"
)

// fakeQueueBroker is an in-memory Broker whose Dequeue only supports the
// single-queue-at-a-time form the Dispatcher actually uses, backed by a
// plain per-queue FIFO slice.
type fakeQueueBroker struct {
	mu           sync.Mutex
	queues       map[string][]*sidekiq.Job
	dequeueCalls []string
}

func newFakeQueueBroker() *fakeQueueBroker {
	return &fakeQueueBroker{queues: make(map[string][]*sidekiq.Job)}
}

func (f *fakeQueueBroker) seed(queue string, jobs ...*sidekiq.Job) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queues[queue] = append(f.queues[queue], jobs...)
}

func (f *fakeQueueBroker) Enqueue(ctx context.Context, job *sidekiq.Job) error { return nil }

func (f *fakeQueueBroker) Dequeue(ctx context.Context, queues []string) (*sidekiq.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if len(queues) != 1 {
		panic("fakeQueueBroker only supports single-queue Dequeue calls, matching Dispatcher's usage")
	}
	q := queues[0]
	f.dequeueCalls = append(f.dequeueCalls, q)

	jobs := f.queues[q]
	if len(jobs) == 0 {
		return nil, nil
	}
	job := jobs[0]
	f.queues[q] = jobs[1:]
	return job, nil
}

func (f *fakeQueueBroker) Acknowledge(ctx context.Context, job *sidekiq.Job) error { return nil }

func (f *fakeQueueBroker) Requeue(ctx context.Context, job *sidekiq.Job, delay time.Duration) error {
	return nil
}

func (f *fakeQueueBroker) MoveToDeadLetter(ctx context.Context, job *sidekiq.Job) error { return nil }

func (f *fakeQueueBroker) GetJobsByStatus(ctx context.Context, status sidekiq.JobStatus, limit int) ([]*sidekiq.Job, error) {
	return nil, nil
}

func (f *fakeQueueBroker) GetQueueStats(ctx context.Context) (map[string]broker.QueueStats, error) {
	return nil, nil
}

func (f *fakeQueueBroker) PromoteDueJobs(ctx context.Context, queues []string) (int, error) {
	return 0, nil
}

func (f *fakeQueueBroker) GetJob(ctx context.Context, id string) (*sidekiq.Job, error) {
	return nil, nil
}
func (f *fakeQueueBroker) RetryDeadJob(ctx context.Context, id string) error { return nil }
func (f *fakeQueueBroker) DeleteJob(ctx context.Context, id string) error    { return nil }

func (f *fakeQueueBroker) Close() error { return nil }

// fakeSubmitter records every job handed to it, standing in for a real
// worker.Pool.
type fakeSubmitter struct {
	mu   sync.Mutex
	jobs []*sidekiq.Job
}

func (s *fakeSubmitter) Submit(ctx context.Context, job *sidekiq.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs = append(s.jobs, job)
	return nil
}

func (s *fakeSubmitter) submitted() []*sidekiq.Job {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*sidekiq.Job, len(s.jobs))
	copy(out, s.jobs)
	return out
}
