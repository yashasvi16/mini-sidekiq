package dispatcher

import (
	"context"
	"log/slog"
	"time"

	"sidekiq"
	"sidekiq/internal/broker"
)

// Submitter is the subset of worker.Pool the dispatcher needs. *worker.Pool
// satisfies it structurally; tests can substitute a fake without needing a
// running pool.
type Submitter interface {
	Submit(ctx context.Context, job *sidekiq.Job) error
}

// QueueConfig assigns a relative poll weight to a queue: how many times it
// appears in one pass of the poll schedule, controlling how much more often
// it gets checked relative to other queues without ever starving them.
type QueueConfig struct {
	Name   string
	Weight int
}

// buildPollSchedule expands a weighted queue list into a flat polling order.
// e.g. critical(3x), default(2x), low(1x) becomes
// [critical, critical, critical, default, default, low]. Cycling through
// this repeatedly polls higher-weight queues more often, without ever
// skipping lower-weight ones entirely - avoiding both strict-priority
// starvation and pure round-robin's disregard for priority.
func buildPollSchedule(queues []QueueConfig) []string {
	var schedule []string
	for _, q := range queues {
		for i := 0; i < q.Weight; i++ {
			schedule = append(schedule, q.Name)
		}
	}
	return schedule
}

// Dispatcher repeatedly dequeues jobs from the broker, one queue at a time
// following a weighted schedule, and hands them to a Submitter (a worker
// Pool) for execution.
type Dispatcher struct {
	broker       broker.Broker
	submitter    Submitter
	schedule     []string
	pollInterval time.Duration
	logger       *slog.Logger
}

// NewDispatcher builds a Dispatcher polling the given queues according to
// their configured weights. pollInterval is how long to pause after a
// Dequeue attempt finds nothing, so an idle system doesn't spin hammering
// the broker with empty polls.
func NewDispatcher(b broker.Broker, submitter Submitter, queues []QueueConfig, pollInterval time.Duration, logger *slog.Logger) *Dispatcher {
	if logger == nil {
		logger = slog.Default()
	}
	return &Dispatcher{
		broker:       b,
		submitter:    submitter,
		schedule:     buildPollSchedule(queues),
		pollInterval: pollInterval,
		logger:       logger,
	}
}

// Run polls the schedule in a loop, dequeuing and submitting jobs, until ctx
// is cancelled. It blocks - call it in its own goroutine.
func (d *Dispatcher) Run(ctx context.Context) {
	if len(d.schedule) == 0 {
		d.logger.Error("dispatcher has no queues configured, nothing to poll")
		return
	}

	idx := 0
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		queue := d.schedule[idx%len(d.schedule)]
		idx++

		job, err := d.broker.Dequeue(ctx, []string{queue})
		if err != nil {
			if ctx.Err() != nil {
				return // shutting down - the error is just ctx cancellation
			}
			d.logger.Error("dequeue failed", "queue", queue, "error", err)
			d.sleep(ctx)
			continue
		}

		if job == nil {
			d.sleep(ctx)
			continue
		}

		if err := d.submitter.Submit(ctx, job); err != nil {
			// ctx was cancelled while waiting for a free worker slot. The
			// job stays recorded in sidekiq:active and will be picked back
			// up once its deadline passes (once a reaper exists - Phase 5).
			return
		}
	}
}

func (d *Dispatcher) sleep(ctx context.Context) {
	select {
	case <-time.After(d.pollInterval):
	case <-ctx.Done():
	}
}
