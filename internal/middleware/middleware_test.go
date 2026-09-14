package middleware

import (
	"context"
	"errors"
	"testing"
	"time"

	"sidekiq"
)

func mustJob(t *testing.T) *sidekiq.Job {
	t.Helper()
	job, err := sidekiq.NewJob("default", "Test", nil)
	if err != nil {
		t.Fatalf("NewJob failed: %v", err)
	}
	return job
}

func TestChainOrdering(t *testing.T) {
	var order []string

	mark := func(name string) Middleware {
		return func(next Handler) Handler {
			return func(ctx context.Context, job *sidekiq.Job) error {
				order = append(order, name+":before")
				err := next(ctx, job)
				order = append(order, name+":after")
				return err
			}
		}
	}

	base := Handler(func(ctx context.Context, job *sidekiq.Job) error {
		order = append(order, "base")
		return nil
	})

	chain := Chain(base, mark("outer"), mark("inner"))
	if err := chain(context.Background(), mustJob(t)); err != nil {
		t.Fatalf("chain returned error: %v", err)
	}

	want := []string{"outer:before", "inner:before", "base", "inner:after", "outer:after"}
	if len(order) != len(want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Errorf("order[%d] = %q, want %q (full: %v)", i, order[i], want[i], order)
		}
	}
}

func TestChainWithNoMiddlewareRunsBaseDirectly(t *testing.T) {
	called := false
	base := Handler(func(ctx context.Context, job *sidekiq.Job) error {
		called = true
		return nil
	})

	chain := Chain(base)
	if err := chain(context.Background(), mustJob(t)); err != nil {
		t.Fatalf("chain returned error: %v", err)
	}
	if !called {
		t.Error("base handler was never called")
	}
}

func TestRecoveryMiddlewareConvertsPanicToError(t *testing.T) {
	base := Handler(func(ctx context.Context, job *sidekiq.Job) error {
		panic("boom")
	})

	chain := Chain(base, RecoveryMiddleware())

	err := chain(context.Background(), mustJob(t))
	if err == nil {
		t.Fatal("expected an error from the recovered panic, got nil")
	}
}

func TestRecoveryMiddlewarePassesThroughNormalError(t *testing.T) {
	wantErr := errors.New("boom")
	base := Handler(func(ctx context.Context, job *sidekiq.Job) error {
		return wantErr
	})

	chain := Chain(base, RecoveryMiddleware())

	err := chain(context.Background(), mustJob(t))
	if !errors.Is(err, wantErr) {
		t.Errorf("got err %v, want %v", err, wantErr)
	}
}

func TestTimeoutMiddlewareCancelsContextAfterDuration(t *testing.T) {
	var sawDone bool
	base := Handler(func(ctx context.Context, job *sidekiq.Job) error {
		<-ctx.Done()
		sawDone = true
		return ctx.Err()
	})

	chain := Chain(base, TimeoutMiddleware(20*time.Millisecond))

	err := chain(context.Background(), mustJob(t))
	if err == nil {
		t.Fatal("expected a context deadline error, got nil")
	}
	if !sawDone {
		t.Error("handler never observed ctx.Done()")
	}
}

func TestTimeoutMiddlewareDoesNotAffectFastHandlers(t *testing.T) {
	base := Handler(func(ctx context.Context, job *sidekiq.Job) error {
		return nil
	})

	chain := Chain(base, TimeoutMiddleware(1*time.Second))

	if err := chain(context.Background(), mustJob(t)); err != nil {
		t.Errorf("expected no error for a fast handler, got %v", err)
	}
}

func TestMetricsMiddlewareDoesNotAlterOutcome(t *testing.T) {
	wantErr := errors.New("boom")
	base := Handler(func(ctx context.Context, job *sidekiq.Job) error {
		return wantErr
	})

	chain := Chain(base, MetricsMiddleware())

	err := chain(context.Background(), mustJob(t))
	if !errors.Is(err, wantErr) {
		t.Errorf("got err %v, want %v", err, wantErr)
	}
}
