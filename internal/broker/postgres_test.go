package broker

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"sidekiq"
)

// Port 5433, not the Postgres default 5432 - this machine has a native
// Windows PostgreSQL service already bound to 5432, so the docker-compose
// Postgres container maps to 5433 instead to avoid colliding with it.
const testPostgresConnString = "postgres://sidekiq:sidekiq@localhost:5433/sidekiq"

func newTestPostgresBroker(t *testing.T) *postgresBroker {
	t.Helper()
	ctx := context.Background()
	b, err := NewPostgresBroker(ctx, testPostgresConnString)
	if err != nil {
		t.Fatalf("NewPostgresBroker failed: %v", err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

func cleanupPostgresJob(t *testing.T, b *postgresBroker, id string) {
	t.Helper()
	t.Cleanup(func() {
		b.pool.Exec(context.Background(), `DELETE FROM jobs WHERE id = $1`, id)
	})
}

func TestPostgresEnqueueDequeueRoundTrip(t *testing.T) {
	ctx := context.Background()
	b := newTestPostgresBroker(t)

	job, err := sidekiq.NewJob("pg-critical", "TestJob", map[string]string{"foo": "bar"})
	if err != nil {
		t.Fatalf("NewJob failed: %v", err)
	}
	cleanupPostgresJob(t, b, job.ID)

	if err := b.Enqueue(ctx, job); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	got, err := b.Dequeue(ctx, []string{"pg-critical"})
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}
	if got == nil {
		t.Fatal("Dequeue returned nil, expected the enqueued job")
	}
	if got.ID != job.ID {
		t.Errorf("ID = %q, want %q", got.ID, job.ID)
	}
	if got.Type != job.Type {
		t.Errorf("Type = %q, want %q", got.Type, job.Type)
	}
	// JSONB canonicalizes formatting on storage (e.g. adds a space after
	// ":"), so compare parsed values, not raw bytes.
	var gotPayload, wantPayload map[string]string
	if err := json.Unmarshal(got.Payload, &gotPayload); err != nil {
		t.Fatalf("unmarshal got.Payload: %v", err)
	}
	if err := json.Unmarshal(job.Payload, &wantPayload); err != nil {
		t.Fatalf("unmarshal job.Payload: %v", err)
	}
	if !reflect.DeepEqual(gotPayload, wantPayload) {
		t.Errorf("Payload = %v, want %v", gotPayload, wantPayload)
	}
	if got.Status != sidekiq.StatusActive {
		t.Errorf("Status = %v, want active", got.Status)
	}

	// Row should no longer be claimable - it's already active.
	again, err := b.Dequeue(ctx, []string{"pg-critical"})
	if err != nil {
		t.Fatalf("second Dequeue failed: %v", err)
	}
	if again != nil {
		t.Errorf("expected no job available on second Dequeue, got %+v", again)
	}
}

func TestPostgresDequeueSkipsFutureProcessAt(t *testing.T) {
	ctx := context.Background()
	b := newTestPostgresBroker(t)

	job, err := sidekiq.NewJob("pg-scheduled", "TestJob", nil)
	if err != nil {
		t.Fatalf("NewJob failed: %v", err)
	}
	job.ProcessAt = time.Now().Add(1 * time.Hour)
	cleanupPostgresJob(t, b, job.ID)

	if err := b.Enqueue(ctx, job); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	got, err := b.Dequeue(ctx, []string{"pg-scheduled"})
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}
	if got != nil {
		t.Errorf("expected no job due yet, got %+v", got)
	}
}

func TestPostgresDequeueTriesQueuesInOrder(t *testing.T) {
	ctx := context.Background()
	b := newTestPostgresBroker(t)

	job, err := sidekiq.NewJob("pg-low-only", "TestJob", nil)
	if err != nil {
		t.Fatalf("NewJob failed: %v", err)
	}
	cleanupPostgresJob(t, b, job.ID)

	if err := b.Enqueue(ctx, job); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	got, err := b.Dequeue(ctx, []string{"pg-empty-queue", "pg-low-only"})
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}
	if got == nil || got.ID != job.ID {
		t.Errorf("expected to fall through to pg-low-only and find job %s, got %+v", job.ID, got)
	}
}

func TestPostgresAcknowledgeRetainsCompletedRow(t *testing.T) {
	ctx := context.Background()
	b := newTestPostgresBroker(t)

	job, err := sidekiq.NewJob("pg-ack", "TestJob", nil)
	if err != nil {
		t.Fatalf("NewJob failed: %v", err)
	}
	cleanupPostgresJob(t, b, job.ID)

	if err := b.Enqueue(ctx, job); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}
	got, err := b.Dequeue(ctx, []string{"pg-ack"})
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}

	if err := b.Acknowledge(ctx, got); err != nil {
		t.Fatalf("Acknowledge failed: %v", err)
	}

	jobs, err := b.GetJobsByStatus(ctx, sidekiq.StatusCompleted, 100)
	if err != nil {
		t.Fatalf("GetJobsByStatus failed: %v", err)
	}
	found := false
	for _, j := range jobs {
		if j.ID == job.ID {
			found = true
		}
	}
	if !found {
		t.Errorf("expected job %s retained with status=completed, not found in %d results", job.ID, len(jobs))
	}
}

func TestPostgresRequeuePersistsAttemptsAndDelay(t *testing.T) {
	ctx := context.Background()
	b := newTestPostgresBroker(t)

	job, err := sidekiq.NewJob("pg-retry", "TestJob", nil)
	if err != nil {
		t.Fatalf("NewJob failed: %v", err)
	}
	cleanupPostgresJob(t, b, job.ID)

	if err := b.Enqueue(ctx, job); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}
	got, err := b.Dequeue(ctx, []string{"pg-retry"})
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}

	got.Attempts++
	got.LastError = "boom"
	got.Status = sidekiq.StatusRetry

	if err := b.Requeue(ctx, got, 1*time.Hour); err != nil {
		t.Fatalf("Requeue failed: %v", err)
	}

	// Not due for another hour, so it shouldn't be dequeueable yet.
	again, err := b.Dequeue(ctx, []string{"pg-retry"})
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}
	if again != nil {
		t.Errorf("expected job not due yet, got %+v", again)
	}

	jobs, err := b.GetJobsByStatus(ctx, sidekiq.StatusRetry, 100)
	if err != nil {
		t.Fatalf("GetJobsByStatus failed: %v", err)
	}
	found := false
	for _, j := range jobs {
		if j.ID == job.ID {
			found = true
			if j.Attempts != 1 {
				t.Errorf("expected Attempts=1, got %d", j.Attempts)
			}
			if j.LastError != "boom" {
				t.Errorf("expected LastError=boom, got %q", j.LastError)
			}
		}
	}
	if !found {
		t.Errorf("expected job %s in retry results", job.ID)
	}
}

func TestPostgresMoveToDeadLetter(t *testing.T) {
	ctx := context.Background()
	b := newTestPostgresBroker(t)

	job, err := sidekiq.NewJob("pg-dead", "TestJob", nil)
	if err != nil {
		t.Fatalf("NewJob failed: %v", err)
	}
	job.MaxAttempts = 1
	cleanupPostgresJob(t, b, job.ID)

	if err := b.Enqueue(ctx, job); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}
	got, err := b.Dequeue(ctx, []string{"pg-dead"})
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}
	got.Attempts = 1
	got.LastError = "fatal"

	if err := b.MoveToDeadLetter(ctx, got); err != nil {
		t.Fatalf("MoveToDeadLetter failed: %v", err)
	}

	jobs, err := b.GetJobsByStatus(ctx, sidekiq.StatusDead, 100)
	if err != nil {
		t.Fatalf("GetJobsByStatus failed: %v", err)
	}
	found := false
	for _, j := range jobs {
		if j.ID == job.ID {
			found = true
		}
	}
	if !found {
		t.Errorf("expected job %s in dead-letter results", job.ID)
	}
}

func TestPostgresEnqueueDeduplicatesUniqueKey(t *testing.T) {
	ctx := context.Background()
	b := newTestPostgresBroker(t)

	job1, err := sidekiq.NewJob("pg-unique", "TestJob", nil)
	if err != nil {
		t.Fatalf("NewJob failed: %v", err)
	}
	job1.UniqueKey = "report-42"
	cleanupPostgresJob(t, b, job1.ID)

	job2, err := sidekiq.NewJob("pg-unique", "TestJob", nil)
	if err != nil {
		t.Fatalf("NewJob failed: %v", err)
	}
	job2.UniqueKey = "report-42"
	cleanupPostgresJob(t, b, job2.ID)

	if err := b.Enqueue(ctx, job1); err != nil {
		t.Fatalf("first Enqueue failed: %v", err)
	}
	// Same UniqueKey, still non-terminal - should be a silent no-op, not an error.
	if err := b.Enqueue(ctx, job2); err != nil {
		t.Fatalf("second Enqueue with duplicate UniqueKey should not error, got: %v", err)
	}

	jobs, err := b.GetJobsByStatus(ctx, sidekiq.StatusPending, 100)
	if err != nil {
		t.Fatalf("GetJobsByStatus failed: %v", err)
	}
	count := 0
	for _, j := range jobs {
		if j.UniqueKey == "report-42" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected exactly 1 job with UniqueKey=report-42, got %d", count)
	}
}

func TestPostgresGetQueueStats(t *testing.T) {
	ctx := context.Background()
	b := newTestPostgresBroker(t)

	job, err := sidekiq.NewJob("pg-stats", "TestJob", nil)
	if err != nil {
		t.Fatalf("NewJob failed: %v", err)
	}
	cleanupPostgresJob(t, b, job.ID)

	if err := b.Enqueue(ctx, job); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	stats, err := b.GetQueueStats(ctx)
	if err != nil {
		t.Fatalf("GetQueueStats failed: %v", err)
	}
	s, ok := stats["pg-stats"]
	if !ok {
		t.Fatalf("expected stats entry for pg-stats, got %v", stats)
	}
	if s.Enqueued < 1 {
		t.Errorf("expected Enqueued >= 1, got %d", s.Enqueued)
	}
}

func TestPostgresPromoteDueJobsIsANoOp(t *testing.T) {
	ctx := context.Background()
	b := newTestPostgresBroker(t)

	n, err := b.PromoteDueJobs(ctx, []string{"pg-stats"})
	if err != nil {
		t.Fatalf("PromoteDueJobs failed: %v", err)
	}
	if n != 0 {
		t.Errorf("expected PromoteDueJobs to always return 0 for Postgres, got %d", n)
	}
}

func TestPostgresGetJob(t *testing.T) {
	ctx := context.Background()
	b := newTestPostgresBroker(t)

	job, err := sidekiq.NewJob("pg-getjob", "TestJob", nil)
	if err != nil {
		t.Fatalf("NewJob failed: %v", err)
	}
	cleanupPostgresJob(t, b, job.ID)

	if err := b.Enqueue(ctx, job); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	got, err := b.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("GetJob failed: %v", err)
	}
	if got == nil || got.ID != job.ID {
		t.Errorf("expected job %s, got %+v", job.ID, got)
	}
}

func TestPostgresGetJobReturnsNilForMissingJob(t *testing.T) {
	ctx := context.Background()
	b := newTestPostgresBroker(t)

	got, err := b.GetJob(ctx, "does-not-exist")
	if err != nil {
		t.Fatalf("GetJob failed: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil for a missing job, got %+v", got)
	}
}

func TestPostgresRetryDeadJob(t *testing.T) {
	ctx := context.Background()
	b := newTestPostgresBroker(t)

	job, err := sidekiq.NewJob("pg-retry-dead", "TestJob", nil)
	if err != nil {
		t.Fatalf("NewJob failed: %v", err)
	}
	job.MaxAttempts = 1
	cleanupPostgresJob(t, b, job.ID)

	if err := b.Enqueue(ctx, job); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}
	got, err := b.Dequeue(ctx, []string{"pg-retry-dead"})
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}
	got.Attempts = 1
	got.LastError = "fatal"
	if err := b.MoveToDeadLetter(ctx, got); err != nil {
		t.Fatalf("MoveToDeadLetter failed: %v", err)
	}

	if err := b.RetryDeadJob(ctx, job.ID); err != nil {
		t.Fatalf("RetryDeadJob failed: %v", err)
	}

	reset, err := b.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("GetJob failed: %v", err)
	}
	if reset.Status != sidekiq.StatusPending {
		t.Errorf("expected Status=pending, got %v", reset.Status)
	}
	if reset.Attempts != 0 {
		t.Errorf("expected Attempts=0, got %d", reset.Attempts)
	}
}

func TestPostgresRetryDeadJobFailsIfNotDead(t *testing.T) {
	ctx := context.Background()
	b := newTestPostgresBroker(t)

	job, err := sidekiq.NewJob("pg-not-dead", "TestJob", nil)
	if err != nil {
		t.Fatalf("NewJob failed: %v", err)
	}
	cleanupPostgresJob(t, b, job.ID)

	if err := b.Enqueue(ctx, job); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	if err := b.RetryDeadJob(ctx, job.ID); err == nil {
		t.Error("expected an error retrying a job that isn't dead, got nil")
	}
}

func TestPostgresDeleteJob(t *testing.T) {
	ctx := context.Background()
	b := newTestPostgresBroker(t)

	job, err := sidekiq.NewJob("pg-delete", "TestJob", nil)
	if err != nil {
		t.Fatalf("NewJob failed: %v", err)
	}
	cleanupPostgresJob(t, b, job.ID)

	if err := b.Enqueue(ctx, job); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	if err := b.DeleteJob(ctx, job.ID); err != nil {
		t.Fatalf("DeleteJob failed: %v", err)
	}

	got, err := b.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("GetJob failed: %v", err)
	}
	if got != nil {
		t.Errorf("expected job to be gone after DeleteJob, got %+v", got)
	}
}
