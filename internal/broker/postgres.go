package broker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"sidekiq"
)

// schema deliberately diverges from the plan's sketch in one place: `id` has
// no DEFAULT gen_random_uuid(). IDs are assigned client-side by
// sidekiq.NewJob, the same reasoning as the Redis broker (see redis.go's
// Enqueue comment) - a client-generated ID is what makes retrying a failed
// Enqueue call safe instead of risking a duplicate row under a new ID.
const schema = `
CREATE TABLE IF NOT EXISTS jobs (
	id           TEXT PRIMARY KEY,
	queue        TEXT NOT NULL DEFAULT 'default',
	type         TEXT NOT NULL,
	payload      JSONB NOT NULL,
	priority     INT NOT NULL DEFAULT 5,
	status       TEXT NOT NULL DEFAULT 'pending',
	attempts     INT NOT NULL DEFAULT 0,
	max_attempts INT NOT NULL DEFAULT 3,
	last_error   TEXT NOT NULL DEFAULT '',
	unique_key   TEXT,
	process_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_jobs_queue_status_process ON jobs (queue, status, process_at)
	WHERE status IN ('pending', 'retry');

CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_unique_key ON jobs (unique_key)
	WHERE unique_key IS NOT NULL AND status NOT IN ('completed', 'dead');
`

// uniqueViolationCode is Postgres's SQLSTATE for a unique-constraint
// violation - used to detect a duplicate UniqueKey and treat it as a
// successful no-op (idempotent enqueue) instead of a real error.
const uniqueViolationCode = "23505"

type postgresBroker struct {
	pool *pgxpool.Pool
}

var _ Broker = (*postgresBroker)(nil)

// NewPostgresBroker connects to Postgres at connString and ensures the
// jobs table/indexes exist, creating them if this is a fresh database.
func NewPostgresBroker(ctx context.Context, connString string) (*postgresBroker, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}
	if _, err := pool.Exec(ctx, schema); err != nil {
		pool.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	return &postgresBroker{pool: pool}, nil
}

func (b *postgresBroker) Enqueue(ctx context.Context, job *sidekiq.Job) error {
	var uniqueKey *string
	if job.UniqueKey != "" {
		uniqueKey = &job.UniqueKey
	}

	_, err := b.pool.Exec(ctx, `
		INSERT INTO jobs (id, queue, type, payload, priority, status, attempts, max_attempts, last_error, unique_key, process_at, created_at, updated_at)
		VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, $8, $9, $10, $11, $12, $12)
	`,
		job.ID, job.Queue, job.Type, []byte(job.Payload), job.Priority, string(job.Status),
		job.Attempts, job.MaxAttempts, job.LastError, uniqueKey, job.ProcessAt, job.CreatedAt,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == uniqueViolationCode {
			// A non-terminal job with this UniqueKey already exists - per
			// the plan's idempotency concept, this is a silent no-op, not
			// an error: the caller's work is already queued.
			return nil
		}
		return fmt.Errorf("insert job: %w", err)
	}
	return nil
}

// Dequeue tries each queue in order (matching the Redis broker's contract),
// atomically claiming one due job per queue with SELECT ... FOR UPDATE SKIP
// LOCKED so concurrent workers never claim the same row - SKIP LOCKED means
// a row another transaction already has locked is treated as if it weren't
// there, instead of blocking behind it.
func (b *postgresBroker) Dequeue(ctx context.Context, queues []string) (*sidekiq.Job, error) {
	for _, queue := range queues {
		row := b.pool.QueryRow(ctx, `
			UPDATE jobs
			SET status = 'active', updated_at = NOW()
			WHERE id = (
				SELECT id FROM jobs
				WHERE queue = $1
				  AND status IN ('pending', 'retry')
				  AND process_at <= NOW()
				ORDER BY priority ASC, process_at ASC
				LIMIT 1
				FOR UPDATE SKIP LOCKED
			)
			RETURNING id, queue, type, payload, priority, status, attempts, max_attempts, last_error, unique_key, process_at, created_at
		`, queue)

		job, err := scanJob(row)
		if errors.Is(err, pgx.ErrNoRows) {
			continue // this queue had nothing due - try the next one
		}
		if err != nil {
			return nil, fmt.Errorf("dequeue from %s: %w", queue, err)
		}
		return job, nil
	}
	return nil, nil
}

func (b *postgresBroker) Acknowledge(ctx context.Context, job *sidekiq.Job) error {
	// Unlike the Redis broker, a completed row is kept (status updated, not
	// deleted) rather than removed - Postgres already has the row, so
	// retaining it costs nothing and gives GetJobsByStatus real completed-job
	// history for free, which the Redis broker can't provide.
	_, err := b.pool.Exec(ctx, `UPDATE jobs SET status = 'completed', updated_at = NOW() WHERE id = $1`, job.ID)
	if err != nil {
		return fmt.Errorf("acknowledge job %s: %w", job.ID, err)
	}
	return nil
}

func (b *postgresBroker) Requeue(ctx context.Context, job *sidekiq.Job, delay time.Duration) error {
	processAt := time.Now().Add(delay)
	_, err := b.pool.Exec(ctx, `
		UPDATE jobs
		SET status = 'retry', attempts = $2, last_error = $3, process_at = $4, updated_at = NOW()
		WHERE id = $1
	`, job.ID, job.Attempts, job.LastError, processAt)
	if err != nil {
		return fmt.Errorf("requeue job %s: %w", job.ID, err)
	}
	return nil
}

func (b *postgresBroker) MoveToDeadLetter(ctx context.Context, job *sidekiq.Job) error {
	_, err := b.pool.Exec(ctx, `
		UPDATE jobs
		SET status = 'dead', attempts = $2, last_error = $3, updated_at = NOW()
		WHERE id = $1
	`, job.ID, job.Attempts, job.LastError)
	if err != nil {
		return fmt.Errorf("move job %s to dead letter: %w", job.ID, err)
	}
	return nil
}

func (b *postgresBroker) GetJobsByStatus(ctx context.Context, status sidekiq.JobStatus, limit int) ([]*sidekiq.Job, error) {
	rows, err := b.pool.Query(ctx, `
		SELECT id, queue, type, payload, priority, status, attempts, max_attempts, last_error, unique_key, process_at, created_at
		FROM jobs WHERE status = $1
		ORDER BY updated_at DESC
		LIMIT $2
	`, string(status), limit)
	if err != nil {
		return nil, fmt.Errorf("query jobs by status: %w", err)
	}
	defer rows.Close()

	var jobs []*sidekiq.Job
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return jobs, fmt.Errorf("scan job: %w", err)
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return jobs, fmt.Errorf("iterate jobs: %w", err)
	}
	return jobs, nil
}

// GetQueueStats aggregates counts per queue in a single query - simpler than
// the Redis broker's version, which has to SCAN for per-queue keys and read
// individual job records to attribute the shared scheduled/active sets.
// Postgres storing every job as one row with a status column makes this a
// GROUP BY instead of a multi-step reconstruction.
func (b *postgresBroker) GetQueueStats(ctx context.Context) (map[string]QueueStats, error) {
	rows, err := b.pool.Query(ctx, `
		SELECT queue,
			COUNT(*) FILTER (WHERE status = 'pending' AND process_at <= NOW()) AS enqueued,
			COUNT(*) FILTER (WHERE status = 'pending' AND process_at > NOW())  AS scheduled,
			COUNT(*) FILTER (WHERE status = 'retry')                          AS retry,
			COUNT(*) FILTER (WHERE status = 'active')                         AS active,
			COUNT(*) FILTER (WHERE status = 'dead')                           AS dead
		FROM jobs
		GROUP BY queue
	`)
	if err != nil {
		return nil, fmt.Errorf("query queue stats: %w", err)
	}
	defer rows.Close()

	stats := make(map[string]QueueStats)
	for rows.Next() {
		var s QueueStats
		if err := rows.Scan(&s.Queue, &s.Enqueued, &s.Scheduled, &s.Retry, &s.Active, &s.Dead); err != nil {
			return nil, fmt.Errorf("scan queue stats: %w", err)
		}
		stats[s.Queue] = s
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate queue stats: %w", err)
	}
	return stats, nil
}

// PromoteDueJobs is a deliberate no-op for Postgres. The Redis broker needs
// this step because delayed jobs live in a physically separate structure
// (a sorted set) from ready-to-run jobs (a list), so becoming "due" requires
// actively moving them. Postgres has no such split: every job is one row
// with a process_at column, and Dequeue's own WHERE clause
// (status IN ('pending','retry') AND process_at <= NOW()) already treats a
// due job as immediately eligible - there's nothing separate to promote.
func (b *postgresBroker) PromoteDueJobs(ctx context.Context, queues []string) (int, error) {
	return 0, nil
}

// GetJob fetches a single job's row by ID.
func (b *postgresBroker) GetJob(ctx context.Context, id string) (*sidekiq.Job, error) {
	row := b.pool.QueryRow(ctx, `
		SELECT id, queue, type, payload, priority, status, attempts, max_attempts, last_error, unique_key, process_at, created_at
		FROM jobs WHERE id = $1
	`, id)

	job, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get job %s: %w", id, err)
	}
	return job, nil
}

// RetryDeadJob resets a dead job back to pending, eligible for Dequeue
// again. Only affects rows currently dead - no separate "claim" step is
// needed the way Redis needs one, since the UPDATE's WHERE clause already
// makes this a single atomic conditional write.
func (b *postgresBroker) RetryDeadJob(ctx context.Context, id string) error {
	cmdTag, err := b.pool.Exec(ctx, `
		UPDATE jobs
		SET status = 'pending', attempts = 0, last_error = '', process_at = NOW(), updated_at = NOW()
		WHERE id = $1 AND status = 'dead'
	`, id)
	if err != nil {
		return fmt.Errorf("retry dead job %s: %w", id, err)
	}
	if cmdTag.RowsAffected() == 0 {
		return fmt.Errorf("job %s not found in dead status", id)
	}
	return nil
}

func (b *postgresBroker) DeleteJob(ctx context.Context, id string) error {
	if _, err := b.pool.Exec(ctx, `DELETE FROM jobs WHERE id = $1`, id); err != nil {
		return fmt.Errorf("delete job %s: %w", id, err)
	}
	return nil
}

func (b *postgresBroker) Close() error {
	b.pool.Close()
	return nil
}

// rowScanner is satisfied by both pgx.Row (QueryRow) and pgx.Rows (Query),
// letting scanJob serve both Dequeue's single-row case and
// GetJobsByStatus's multi-row case.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanJob(row rowScanner) (*sidekiq.Job, error) {
	var job sidekiq.Job
	var status string
	var payload []byte
	var uniqueKey *string

	err := row.Scan(
		&job.ID, &job.Queue, &job.Type, &payload, &job.Priority, &status,
		&job.Attempts, &job.MaxAttempts, &job.LastError, &uniqueKey,
		&job.ProcessAt, &job.CreatedAt,
	)
	if err != nil {
		return nil, err
	}

	job.Payload = payload
	job.Status = sidekiq.JobStatus(status)
	if uniqueKey != nil {
		job.UniqueKey = *uniqueKey
	}
	return &job, nil
}
