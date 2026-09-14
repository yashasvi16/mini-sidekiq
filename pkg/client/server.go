package client

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"sidekiq/internal/broker"
	"sidekiq/internal/dispatcher"
	"sidekiq/internal/middleware"
	"sidekiq/internal/scheduler"
	"sidekiq/internal/worker"
)

// Handler processes a single job. Re-exported from worker so callers of
// this package never need to import internal/worker themselves.
type Handler = worker.Handler

// Middleware wraps a Handler with cross-cutting behavior. Re-exported from
// middleware for the same reason.
type Middleware = middleware.Middleware

// defaultPollInterval is how long the dispatcher pauses after finding an
// empty queue, so an idle system doesn't spin hammering Redis.
const defaultPollInterval = 250 * time.Millisecond

// defaultSchedulerInterval is how often the scheduler checks for
// scheduled/retry jobs whose time has come, matching the plan's ~5s cadence.
const defaultSchedulerInterval = 5 * time.Second

// ServerConfig configures a Server's connection, concurrency, and queues.
type ServerConfig struct {
	RedisAddr   string
	Concurrency int    // number of worker goroutines
	Queues      Queues // queue name -> relative poll weight

	// BufferSize is the worker pool's internal channel capacity (see
	// worker.Pool.Submit for what this bounds). Defaults to Concurrency*2
	// if zero.
	BufferSize int

	// PollInterval and SchedulerInterval override the dispatcher/scheduler
	// polling cadence; both default if zero.
	PollInterval      time.Duration
	SchedulerInterval time.Duration

	Logger *slog.Logger
}

// Server runs a worker pool, priority dispatcher, and retry/scheduled-job
// scheduler together against a single Redis-backed broker.
type Server struct {
	broker     broker.Broker
	registry   *worker.Registry
	pool       *worker.Pool
	dispatcher *dispatcher.Dispatcher
	scheduler  *scheduler.Scheduler
	logger     *slog.Logger
}

// NewServer builds a Server from cfg. Register handlers with Register (and
// optionally Use) before calling Run.
func NewServer(cfg ServerConfig) *Server {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	bufferSize := cfg.BufferSize
	if bufferSize == 0 {
		bufferSize = cfg.Concurrency * 2
	}
	pollInterval := cfg.PollInterval
	if pollInterval == 0 {
		pollInterval = defaultPollInterval
	}
	schedulerInterval := cfg.SchedulerInterval
	if schedulerInterval == 0 {
		schedulerInterval = defaultSchedulerInterval
	}

	b := broker.NewRedisBroker(cfg.RedisAddr)
	registry := worker.NewRegistry()
	pool := worker.NewPool(cfg.Concurrency, bufferSize, registry, b, logger)

	var queueConfigs []dispatcher.QueueConfig
	var queueNames []string
	for name, weight := range cfg.Queues {
		queueConfigs = append(queueConfigs, dispatcher.QueueConfig{Name: name, Weight: weight})
		queueNames = append(queueNames, name)
	}

	disp := dispatcher.NewDispatcher(b, pool, queueConfigs, pollInterval, logger)
	sched := scheduler.NewScheduler(b, queueNames, schedulerInterval, logger)

	return &Server{
		broker:     b,
		registry:   registry,
		pool:       pool,
		dispatcher: disp,
		scheduler:  sched,
		logger:     logger,
	}
}

// Register associates a job type with the handler that processes it.
func (s *Server) Register(jobType string, h Handler) {
	s.registry.Register(jobType, h)
}

// Use adds middleware to every job's processing chain, in addition to the
// pool's default LoggingMiddleware. Call before Run.
func (s *Server) Use(mws ...Middleware) {
	s.pool.Use(mws...)
}

// Run starts the worker pool, dispatcher, and scheduler, and blocks until
// ctx is cancelled - at which point it stops the dispatcher and scheduler
// first (no new jobs fetched), then waits for the pool to finish every
// in-flight job before returning. This is the graceful shutdown sequence
// from the plan: stop fetching, drain in-flight, then close up.
func (s *Server) Run(ctx context.Context) {
	s.pool.Start(ctx)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		s.dispatcher.Run(ctx)
	}()
	go func() {
		defer wg.Done()
		s.scheduler.Run(ctx)
	}()

	<-ctx.Done()
	wg.Wait()     // dispatcher and scheduler have stopped fetching new work
	s.pool.Stop() // block until every in-flight job finishes

	if err := s.broker.Close(); err != nil {
		s.logger.Error("broker close failed", "error", err)
	}
}
