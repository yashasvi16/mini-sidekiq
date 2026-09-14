package middleware

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"sidekiq"
)

// Handler processes a single job. This is the canonical definition - the
// worker package aliases its own Handler to this one so both packages refer
// to exactly the same type, with no conversions needed at the boundary.
type Handler func(ctx context.Context, job *sidekiq.Job) error

// Middleware wraps a Handler with cross-cutting behavior (logging, metrics,
// timeouts, panic recovery) and returns a new Handler.
type Middleware func(next Handler) Handler

// Chain composes middlewares around a base Handler. Middlewares run in the
// order given - the first one in the slice is the outermost wrapper, so it
// sees the job first (e.g. logging "job started") and the final error last
// (e.g. logging "job finished", after everything inside it has run).
func Chain(base Handler, mws ...Middleware) Handler {
	h := base
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

// LoggingMiddleware logs each job's start, completion, duration, and error.
func LoggingMiddleware(logger *slog.Logger) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, job *sidekiq.Job) error {
			start := time.Now()
			logger.Info("job started", "id", job.ID, "type", job.Type, "queue", job.Queue, "attempt", job.Attempts+1)
			err := next(ctx, job)
			logger.Info("job finished", "id", job.ID, "type", job.Type, "duration", time.Since(start), "error", err)
			return err
		}
	}
}

// RecoveryMiddleware converts a panic inside the wrapped handler into a
// regular error, so one bad job can't crash the worker goroutine running it.
// The worker pool also carries its own independent recover() as a last-
// resort safety net, so this is about controlling *where* in the middleware
// chain a panic gets caught (e.g. before or after logging/metrics see it),
// not about being the only thing standing between a panic and a crash.
func RecoveryMiddleware() Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, job *sidekiq.Job) (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("panic: %v\n%s", r, debug.Stack())
				}
			}()
			return next(ctx, job)
		}
	}
}

// TimeoutMiddleware bounds how long a single job's handler may run. Go has
// no mechanism to forcibly kill a running goroutine, so this only gives the
// handler a cancelled ctx to notice and abort on - a handler that ignores
// ctx.Done() will still run to completion regardless.
func TimeoutMiddleware(d time.Duration) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, job *sidekiq.Job) error {
			ctx, cancel := context.WithTimeout(ctx, d)
			defer cancel()
			return next(ctx, job)
		}
	}
}
