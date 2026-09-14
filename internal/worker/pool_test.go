package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"sidekiq"
)

func waitForCondition(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

func TestPoolProcessesJobSuccessfully(t *testing.T) {
	registry := NewRegistry()
	fb := newFakeBroker()
	pool := NewPool(2, 10, registry, fb, nil)

	done := make(chan struct{})
	registry.Register("Noop", func(ctx context.Context, job *sidekiq.Job) error {
		close(done)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool.Start(ctx)

	job, err := sidekiq.NewJob("default", "Noop", nil)
	if err != nil {
		t.Fatalf("NewJob failed: %v", err)
	}
	if err := pool.Submit(ctx, job); err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never ran")
	}

	waitForCondition(t, func() bool {
		acked, _, _ := fb.counts()
		return acked == 1
	})

	cancel()
	pool.Stop()

	fb.mu.Lock()
	defer fb.mu.Unlock()
	if len(fb.acked) != 1 || fb.acked[0] != job.ID {
		t.Errorf("expected job %s acknowledged, got acked=%v", job.ID, fb.acked)
	}
}

func TestPoolRequeuesOnFailureUnderMaxAttempts(t *testing.T) {
	registry := NewRegistry()
	fb := newFakeBroker()
	pool := NewPool(1, 10, registry, fb, nil)

	registry.Register("Fail", func(ctx context.Context, job *sidekiq.Job) error {
		return errors.New("boom")
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool.Start(ctx)

	job, err := sidekiq.NewJob("default", "Fail", nil)
	if err != nil {
		t.Fatalf("NewJob failed: %v", err)
	}
	job.MaxAttempts = 3

	if err := pool.Submit(ctx, job); err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	waitForCondition(t, func() bool {
		_, requeued, _ := fb.counts()
		return requeued == 1
	})

	cancel()
	pool.Stop()

	fb.mu.Lock()
	defer fb.mu.Unlock()
	if len(fb.requeued) != 1 {
		t.Fatalf("expected 1 requeue, got %d", len(fb.requeued))
	}
	if fb.requeued[0].jobID != job.ID {
		t.Errorf("requeued wrong job: got %s want %s", fb.requeued[0].jobID, job.ID)
	}
	if fb.requeued[0].delay <= 0 {
		t.Errorf("expected a positive backoff delay, got %v", fb.requeued[0].delay)
	}
	if len(fb.deadLettered) != 0 {
		t.Errorf("expected no dead-lettered jobs, got %v", fb.deadLettered)
	}
	if job.Attempts != 1 {
		t.Errorf("expected Attempts=1, got %d", job.Attempts)
	}
	if job.Status != sidekiq.StatusRetry {
		t.Errorf("expected Status=retry, got %v", job.Status)
	}
}

func TestPoolMovesToDeadLetterAfterMaxAttempts(t *testing.T) {
	registry := NewRegistry()
	fb := newFakeBroker()
	pool := NewPool(1, 10, registry, fb, nil)

	registry.Register("Fail", func(ctx context.Context, job *sidekiq.Job) error {
		return errors.New("boom")
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool.Start(ctx)

	job, err := sidekiq.NewJob("default", "Fail", nil)
	if err != nil {
		t.Fatalf("NewJob failed: %v", err)
	}
	job.MaxAttempts = 1

	if err := pool.Submit(ctx, job); err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	waitForCondition(t, func() bool {
		_, _, dead := fb.counts()
		return dead == 1
	})

	cancel()
	pool.Stop()

	fb.mu.Lock()
	defer fb.mu.Unlock()
	if len(fb.deadLettered) != 1 || fb.deadLettered[0] != job.ID {
		t.Errorf("expected job dead-lettered, got %v", fb.deadLettered)
	}
	if len(fb.requeued) != 0 {
		t.Errorf("expected no requeues, got %v", fb.requeued)
	}
	if job.Status != sidekiq.StatusDead {
		t.Errorf("expected Status=dead, got %v", job.Status)
	}
}

func TestPoolRecoversFromPanic(t *testing.T) {
	registry := NewRegistry()
	fb := newFakeBroker()
	pool := NewPool(1, 10, registry, fb, nil)

	registry.Register("Panic", func(ctx context.Context, job *sidekiq.Job) error {
		panic("something went very wrong")
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool.Start(ctx)

	job, err := sidekiq.NewJob("default", "Panic", nil)
	if err != nil {
		t.Fatalf("NewJob failed: %v", err)
	}
	job.MaxAttempts = 1

	if err := pool.Submit(ctx, job); err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	waitForCondition(t, func() bool {
		_, _, dead := fb.counts()
		return dead == 1
	})

	// Prove the worker goroutine survived the panic: submit a second,
	// well-behaved job on the same pool and confirm it still gets processed.
	done := make(chan struct{})
	registry.Register("Noop2", func(ctx context.Context, job *sidekiq.Job) error {
		close(done)
		return nil
	})
	job2, err := sidekiq.NewJob("default", "Noop2", nil)
	if err != nil {
		t.Fatalf("NewJob failed: %v", err)
	}
	if err := pool.Submit(ctx, job2); err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("pool did not survive the panic - second job never processed")
	}

	cancel()
	pool.Stop()
}

func TestPoolDeadLettersWhenNoHandlerRegistered(t *testing.T) {
	registry := NewRegistry()
	fb := newFakeBroker()
	pool := NewPool(1, 10, registry, fb, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool.Start(ctx)

	job, err := sidekiq.NewJob("default", "Unregistered", nil)
	if err != nil {
		t.Fatalf("NewJob failed: %v", err)
	}

	if err := pool.Submit(ctx, job); err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	waitForCondition(t, func() bool {
		_, _, dead := fb.counts()
		return dead == 1
	})

	cancel()
	pool.Stop()

	fb.mu.Lock()
	defer fb.mu.Unlock()
	if len(fb.requeued) != 0 {
		t.Errorf("expected no requeues for a missing handler (config error, not transient), got %v", fb.requeued)
	}
}

func TestPoolGracefulShutdownWaitsForInFlightJob(t *testing.T) {
	registry := NewRegistry()
	fb := newFakeBroker()
	pool := NewPool(1, 10, registry, fb, nil)

	started := make(chan struct{})
	finish := make(chan struct{})
	registry.Register("Slow", func(ctx context.Context, job *sidekiq.Job) error {
		close(started)
		<-finish // block until the test says the handler may finish
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	pool.Start(ctx)

	job, err := sidekiq.NewJob("default", "Slow", nil)
	if err != nil {
		t.Fatalf("NewJob failed: %v", err)
	}
	if err := pool.Submit(ctx, job); err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	<-started // handler is now running

	// Signal shutdown while the handler is still mid-flight.
	cancel()

	stopped := make(chan struct{})
	go func() {
		pool.Stop()
		close(stopped)
	}()

	select {
	case <-stopped:
		t.Fatal("Stop returned before the in-flight job finished")
	case <-time.After(100 * time.Millisecond):
		// expected: Stop is still blocked, the handler hasn't returned yet
	}

	close(finish) // let the handler complete

	select {
	case <-stopped:
		// good: Stop unblocked once the in-flight job finished
	case <-time.After(2 * time.Second):
		t.Fatal("Stop never returned after the in-flight job finished")
	}

	waitForCondition(t, func() bool {
		acked, _, _ := fb.counts()
		return acked == 1
	})
}
