package middleware

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"sidekiq"
)

var (
	jobsProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "sidekiq_jobs_processed_total",
			Help: "Total number of jobs processed, labeled by job type and outcome.",
		},
		[]string{"type", "outcome"},
	)

	jobDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "sidekiq_job_duration_seconds",
			Help:    "Job handler execution time in seconds, labeled by job type.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"type"},
	)
)

func init() {
	prometheus.MustRegister(jobsProcessed, jobDuration)
}

// MetricsMiddleware records a Prometheus counter (by outcome) and a duration
// histogram for every job that passes through it.
func MetricsMiddleware() Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, job *sidekiq.Job) error {
			start := time.Now()
			err := next(ctx, job)
			jobDuration.WithLabelValues(job.Type).Observe(time.Since(start).Seconds())

			outcome := "success"
			if err != nil {
				outcome = "failure"
			}
			jobsProcessed.WithLabelValues(job.Type, outcome).Inc()
			return err
		}
	}
}
