package worker

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"sidekiq"
	"sidekiq/internal/broker"
	"sidekiq/internal/middleware"
	"sidekiq/internal/retry"
)

// bookkeepingTimeout bounds broker calls that record a job's outcome
// (Acknowledge/Requeue/MoveToDeadLetter). These deliberately use their own
// context, independent of the worker's ctx: if a job finishes its handler
// right as shutdown begins, the worker's ctx may already be cancelled, and
// using it here would make the outcome-recording call fail immediately,
// silently stranding the job in sidekiq:active forever.
const bookkeepingTimeout = 5 * time.Second

// Pool runs a fixed number of worker goroutines that pull jobs off an
// internal channel and execute them via handlers registered in a Registry.
type Pool struct {
	jobCh      chan *sidekiq.Job
	workers    int
	wg         sync.WaitGroup
	registry   *Registry
	broker     broker.Broker
	logger     *slog.Logger
	middleware []middleware.Middleware
}

// NewPool creates a Pool with the given number of worker goroutines and an
// input channel buffered to bufferSize. Producers hand jobs in via Submit.
// LoggingMiddleware is enabled by default; add more with Use.
func NewPool(workers, bufferSize int, registry *Registry, b broker.Broker, logger *slog.Logger) *Pool {
	if logger == nil {
		logger = slog.Default()
	}
	return &Pool{
		jobCh:      make(chan *sidekiq.Job, bufferSize),
		workers:    workers,
		registry:   registry,
		broker:     b,
		logger:     logger,
		middleware: []middleware.Middleware{middleware.LoggingMiddleware(logger)},
	}
}

// Use appends middleware to the chain every job's handler runs through, in
// the order given (earlier entries are outermost - see middleware.Chain).
// Call this before Start; adding middleware concurrently with running
// workers is not safe.
func (p *Pool) Use(mws ...middleware.Middleware) {
	p.middleware = append(p.middleware, mws...)
}

// Submit hands a job to the pool for processing. It blocks if every worker
// is busy and the internal buffer is full - this is the pool's backpressure
// mechanism - unless ctx is cancelled first, in which case it returns
// ctx.Err().
func (p *Pool) Submit(ctx context.Context, job *sidekiq.Job) error {
	select {
	case p.jobCh <- job:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Start launches the worker goroutines and returns immediately. Workers run
// until ctx is cancelled.
func (p *Pool) Start(ctx context.Context) {
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.runWorker(ctx, i)
	}
}

// Stop blocks until every worker goroutine has exited, which means every
// in-flight job has finished processing. Callers must cancel the context
// passed to Start before calling Stop, or this blocks forever.
func (p *Pool) Stop() {
	p.wg.Wait()
}

func (p *Pool) runWorker(ctx context.Context, id int) {
	defer p.wg.Done()
	for {
		select {
		case job, ok := <-p.jobCh:
			if !ok {
				return
			}
			p.process(ctx, job)
		case <-ctx.Done():
			return
		}
	}
}

func (p *Pool) process(ctx context.Context, job *sidekiq.Job) {
	handler, ok := p.registry.Get(job.Type)
	if !ok {
		// No handler will ever appear for this - it's a config error, not a
		// transient failure, so there's no point burning retry attempts on it.
		job.LastError = fmt.Sprintf("no handler registered for job type %q", job.Type)
		job.Status = sidekiq.StatusDead
		p.deadLetter(job)
		return
	}

	chain := middleware.Chain(handler, p.middleware...)
	if err := p.runHandler(ctx, chain, job); err != nil {
		job.LastError = err.Error()
		p.fail(job)
		return
	}

	p.acknowledge(job)
}

// runHandler executes handler, converting a panic into an error so one bad
// job - or a panicking middleware - can't crash the worker goroutine and take
// the whole pool down with it. This is a last-resort safety net independent
// of whether the caller also configured middleware.RecoveryMiddleware.
func (p *Pool) runHandler(ctx context.Context, handler Handler, job *sidekiq.Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v\n%s", r, debug.Stack())
		}
	}()
	return handler(ctx, job)
}

func (p *Pool) acknowledge(job *sidekiq.Job) {
	ctx, cancel := context.WithTimeout(context.Background(), bookkeepingTimeout)
	defer cancel()
	if err := p.broker.Acknowledge(ctx, job); err != nil {
		p.logger.Error("acknowledge failed", "job_id", job.ID, "error", err)
	}
}

func (p *Pool) deadLetter(job *sidekiq.Job) {
	ctx, cancel := context.WithTimeout(context.Background(), bookkeepingTimeout)
	defer cancel()
	if err := p.broker.MoveToDeadLetter(ctx, job); err != nil {
		p.logger.Error("move to dead letter failed", "job_id", job.ID, "error", err)
	}
}

func (p *Pool) fail(job *sidekiq.Job) {
	job.Attempts++
	if job.Attempts >= job.MaxAttempts {
		job.Status = sidekiq.StatusDead
		p.deadLetter(job)
		return
	}

	job.Status = sidekiq.StatusRetry
	delay := retry.NextDelay(job.Attempts)

	ctx, cancel := context.WithTimeout(context.Background(), bookkeepingTimeout)
	defer cancel()
	if err := p.broker.Requeue(ctx, job, delay); err != nil {
		p.logger.Error("requeue failed", "job_id", job.ID, "error", err)
	}
}
