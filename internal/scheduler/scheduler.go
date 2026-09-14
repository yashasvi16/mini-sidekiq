package scheduler

import (
	"context"
	"log/slog"
	"time"

	"sidekiq/internal/broker"
)

// Scheduler periodically promotes due scheduled/retry jobs back into their
// active queues, where a Dispatcher will pick them up in the normal flow.
type Scheduler struct {
	broker   broker.Broker
	queues   []string
	interval time.Duration
	logger   *slog.Logger
}

// NewScheduler builds a Scheduler that checks for due jobs across the given
// queues (and the shared scheduled set) every interval.
func NewScheduler(b broker.Broker, queues []string, interval time.Duration, logger *slog.Logger) *Scheduler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Scheduler{broker: b, queues: queues, interval: interval, logger: logger}
}

// Run polls on a ticker until ctx is cancelled. It blocks - call it in its
// own goroutine.
func (s *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n, err := s.broker.PromoteDueJobs(ctx, s.queues)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				s.logger.Error("promote due jobs failed", "error", err)
				continue
			}
			if n > 0 {
				s.logger.Info("promoted due jobs", "count", n)
			}
		case <-ctx.Done():
			return
		}
	}
}
