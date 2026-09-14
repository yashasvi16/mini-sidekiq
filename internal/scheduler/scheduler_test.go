package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"

	"sidekiq"
	"sidekiq/internal/broker"
)

// fakeBroker only implements what Scheduler actually calls; the rest are
// unused stubs to satisfy broker.Broker.
type fakeBroker struct {
	mu    sync.Mutex
	calls [][]string
	n     int
	err   error
}

func (f *fakeBroker) PromoteDueJobs(ctx context.Context, queues []string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, queues)
	return f.n, f.err
}

func (f *fakeBroker) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
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
func (f *fakeBroker) GetJobsByStatus(ctx context.Context, status sidekiq.JobStatus, limit int) ([]*sidekiq.Job, error) {
	return nil, nil
}
func (f *fakeBroker) GetQueueStats(ctx context.Context) (map[string]broker.QueueStats, error) {
	return nil, nil
}
func (f *fakeBroker) GetJob(ctx context.Context, id string) (*sidekiq.Job, error) { return nil, nil }
func (f *fakeBroker) RetryDeadJob(ctx context.Context, id string) error           { return nil }
func (f *fakeBroker) DeleteJob(ctx context.Context, id string) error              { return nil }
func (f *fakeBroker) Close() error                                                { return nil }

func TestSchedulerPollsPeriodicallyUntilCancelled(t *testing.T) {
	fb := &fakeBroker{n: 2}
	s := NewScheduler(fb, []string{"critical", "default"}, 20*time.Millisecond, nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.Run(ctx)
		close(done)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for fb.callCount() < 3 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if fb.callCount() < 3 {
		t.Fatalf("expected at least 3 PromoteDueJobs calls, got %d", fb.callCount())
	}

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after ctx cancellation")
	}

	fb.mu.Lock()
	defer fb.mu.Unlock()
	want := []string{"critical", "default"}
	for i, got := range fb.calls[0] {
		if got != want[i] {
			t.Errorf("call queues = %v, want %v", fb.calls[0], want)
			break
		}
	}
}
