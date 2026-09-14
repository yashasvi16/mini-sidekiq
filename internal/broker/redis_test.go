package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"testing"
	"time"

	"sidekiq"

	"github.com/redis/go-redis/v9"
)

func TestEnqueueDequeueRoundTrip(t *testing.T) {
	ctx := context.Background()
	b := NewRedisBroker("localhost:6379")

	job, err := sidekiq.NewJob("critical", "TestJob", map[string]string{"foo": "bar"})
	if err != nil {
		t.Fatalf("NewJob failed: %v", err)
	}

	// Clean up whatever this test creates, regardless of pass/fail.
	t.Cleanup(func() {
		b.client.Del(ctx, "sidekiq:job:"+job.ID)
		b.client.ZRem(ctx, "sidekiq:active", job.ID)
		b.client.LRem(ctx, "sidekiq:queue:critical", 0, job.ID)
	})

	if err := b.Enqueue(ctx, job); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	got, err := b.Dequeue(ctx, []string{"critical"})
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}
	if got == nil {
		t.Fatal("Dequeue returned nil job, expected the one we enqueued")
	}

	if got.ID != job.ID {
		t.Errorf("ID = %q, want %q", got.ID, job.ID)
	}
	if got.Type != job.Type {
		t.Errorf("Type = %q, want %q", got.Type, job.Type)
	}
	if got.Queue != job.Queue {
		t.Errorf("Queue = %q, want %q", got.Queue, job.Queue)
	}
	if string(got.Payload) != string(job.Payload) {
		t.Errorf("Payload = %s, want %s", got.Payload, job.Payload)
	}

	// Dequeue should have atomically recorded the job as in-flight.
	score, err := b.client.ZScore(ctx, "sidekiq:active", job.ID).Result()
	if err != nil {
		t.Fatalf("expected job in sidekiq:active, ZScore failed: %v", err)
	}
	if score <= 0 {
		t.Errorf("sidekiq:active score = %v, want a future deadline timestamp", score)
	}

	// The queue itself should now be empty.
	length, err := b.client.LLen(ctx, "sidekiq:queue:critical").Result()
	if err != nil {
		t.Fatalf("LLen failed: %v", err)
	}
	if length != 0 {
		t.Errorf("sidekiq:queue:critical length = %d, want 0", length)
	}
}

func TestDequeueEmptyQueueReturnsNil(t *testing.T) {
	ctx := context.Background()
	b := NewRedisBroker("localhost:6379")

	got, err := b.Dequeue(ctx, []string{"nonexistent-queue-xyz"})
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}
	if got != nil {
		t.Errorf("got %+v, want nil for an empty queue", got)
	}
}

func TestAcknowledge(t *testing.T) {
	ctx := context.Background()
	b := NewRedisBroker("localhost:6379")

	job, err := sidekiq.NewJob("critical", "TestJob", map[string]string{"foo": "bar"})
	if err != nil {
		t.Fatalf("new job failed %v", err)
	}

	t.Cleanup(func() {
		b.client.Del(ctx, "sidekiq:job:"+job.ID)
		b.client.ZRem(ctx, "sidekiq:active", job.ID)
		b.client.LRem(ctx, "sidekiq:queue:critical", 0, job.ID)
	})

	if err := b.Enqueue(ctx, job); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	got, err := b.Dequeue(ctx, []string{"critical"})
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}

	if err := b.Acknowledge(ctx, got); err != nil {
		t.Fatalf("aknowledge failed: %v", err)
	}

	_, err = b.client.ZScore(ctx, "sidekiq:active", got.ID).Result()
	if err != redis.Nil {
		t.Errorf("expected job removed from sidekiq:active, got err=%v", err)
	}

	jobkey := fmt.Sprintf("sidekiq:job:%s", got.ID)
	_, err = b.client.Get(ctx, jobkey).Result()
	if err != redis.Nil {
		t.Errorf("expected job removed from sidekiq:job:{id}, got err=%v", err)
	}
}

func TestRequeue(t *testing.T) {
	ctx := context.Background()
	b := NewRedisBroker("localhost:6379")

	job, err := sidekiq.NewJob("critical", "TestJob", map[string]string{"foo": "bar"})
	if err != nil {
		t.Fatalf("new job failed %v", err)
	}

	t.Cleanup(func() {
		b.client.Del(ctx, "sidekiq:job:"+job.ID)
		b.client.ZRem(ctx, "sidekiq:active", job.ID)
		b.client.LRem(ctx, "sidekiq:queue:critical", 0, job.ID)
		b.client.ZRem(ctx, "sidekiq:retry:critical", job.ID)
		b.client.LRem(ctx, "sidekiq:dead:critical", 0, job.ID)
	})

	if err := b.Enqueue(ctx, job); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	got, err := b.Dequeue(ctx, []string{"critical"})
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}

	got.Attempts++
	got.LastError = "boom"
	got.Status = sidekiq.StatusRetry

	if err := b.Requeue(ctx, got, 5*time.Second); err != nil {
		t.Fatalf("requeue failed %v", err)
	}

	_, err = b.client.ZScore(ctx, "sidekiq:active", got.ID).Result()
	if err != redis.Nil {
		t.Errorf("expected job removed from sidekiq:active, got err=%v", err)
	}

	score, err := b.client.ZScore(ctx, "sidekiq:retry:"+got.Queue, got.ID).Result()
	if err != nil {
		t.Fatalf("get data from sidekiq:retry:{queue}, %v", err)
	}
	expectedScore := time.Now().Add(5 * time.Second).Unix()
	if math.Abs(score-float64(expectedScore)) > 1 {
		t.Errorf("expected score roughly %d, got %f", expectedScore, score)
	}

	data, err := b.client.Get(ctx, "sidekiq:job:"+got.ID).Result()
	if err != nil {
		t.Fatalf("get data from sidekiq:job:{id}, %v", err)
	}
	var unmarshalledJob sidekiq.Job
	err = json.Unmarshal([]byte(data), &unmarshalledJob)
	if err != nil {
		t.Fatalf("error unmarshalling sidekiq:job, %v", err)
	}
	if unmarshalledJob.Status != got.Status {
		t.Errorf("expected job status is different, got status: %v", unmarshalledJob.Status)
	}

}

func TestMoveToDeadLetter(t *testing.T) {
	ctx := context.Background()
	b := NewRedisBroker("localhost:6379")

	job, err := sidekiq.NewJob("critical", "TestJob", map[string]string{"foo": "bar"})
	if err != nil {
		t.Fatalf("new job failed %v", err)
	}

	t.Cleanup(func() {
		b.client.Del(ctx, "sidekiq:job:"+job.ID)
		b.client.ZRem(ctx, "sidekiq:active", job.ID)
		b.client.LRem(ctx, "sidekiq:queue:critical", 0, job.ID)
		b.client.LRem(ctx, "sidekiq:dead:critical", 0, job.ID)
	})

	if err := b.Enqueue(ctx, job); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	got, err := b.Dequeue(ctx, []string{"critical"})
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}

	if err := b.MoveToDeadLetter(ctx, got); err != nil {
		t.Fatalf("Move to dead letter failed: %v", err)
	}

	_, err = b.client.ZScore(ctx, "sidekiq:active", got.ID).Result()
	if err != redis.Nil {
		t.Errorf("expected job removed from sidekiq:active, got err=%v", err)
	}

	_, err = b.client.LPos(ctx, "sidekiq:dead:"+got.Queue, got.ID, redis.LPosArgs{}).Result()
	if err == redis.Nil {
		t.Errorf("expected to be present in sidekiq:dead")
	} else if err != nil {
		t.Fatalf("LPos failed: %v", err)
	}

	jobkey := fmt.Sprintf("sidekiq:job:%s", got.ID)
	data, err := b.client.Get(ctx, jobkey).Result()
	if err != nil {
		t.Fatalf("get data from sidekiq:job:{id}, %v", err)
	}
	var unmarshalledJob sidekiq.Job
	err = json.Unmarshal([]byte(data), &unmarshalledJob)
	if err != nil {
		t.Fatalf("error unmarshalling sidekiq:job, %v", err)
	}
	if unmarshalledJob.Status != sidekiq.StatusDead {
		t.Errorf("expected job status to be dead, got status=%v", unmarshalledJob.Status)
	}
}

func TestPromoteDueJobsFromScheduled(t *testing.T) {
	ctx := context.Background()
	b := NewRedisBroker("localhost:6379")

	job, err := sidekiq.NewJob("critical", "TestJob", map[string]string{"foo": "bar"})
	if err != nil {
		t.Fatalf("new job failed: %v", err)
	}
	job.ProcessAt = time.Now().Add(1 * time.Hour) // schedule far in the future first

	t.Cleanup(func() {
		b.client.Del(ctx, "sidekiq:job:"+job.ID)
		b.client.ZRem(ctx, "sidekiq:scheduled", job.ID)
		b.client.LRem(ctx, "sidekiq:queue:critical", 0, job.ID)
	})

	if err := b.Enqueue(ctx, job); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// Confirm it actually landed in sidekiq:scheduled, not the active queue.
	if _, err := b.client.ZScore(ctx, "sidekiq:scheduled", job.ID).Result(); err != nil {
		t.Fatalf("expected job in sidekiq:scheduled, ZScore failed: %v", err)
	}

	// Simulate time passing: backdate its score so it's now due. (Enqueue
	// itself won't do this - a past ProcessAt routes straight to the active
	// queue instead of sidekiq:scheduled, so this step is the only way to
	// get a "was scheduled, now due" job into that state.)
	if err := b.client.ZAdd(ctx, "sidekiq:scheduled", redis.Z{
		Score:  float64(time.Now().Add(-1 * time.Minute).Unix()),
		Member: job.ID,
	}).Err(); err != nil {
		t.Fatalf("failed to backdate score: %v", err)
	}

	n, err := b.PromoteDueJobs(ctx, []string{"critical"})
	if err != nil {
		t.Fatalf("PromoteDueJobs failed: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 promoted job, got %d", n)
	}

	if _, err := b.client.ZScore(ctx, "sidekiq:scheduled", job.ID).Result(); err != redis.Nil {
		t.Errorf("expected job removed from sidekiq:scheduled, got err=%v", err)
	}

	if _, err := b.client.LPos(ctx, "sidekiq:queue:critical", job.ID, redis.LPosArgs{}).Result(); err == redis.Nil {
		t.Errorf("expected job present in sidekiq:queue:critical")
	} else if err != nil {
		t.Fatalf("LPos failed: %v", err)
	}
}

func TestPromoteDueJobsFromRetry(t *testing.T) {
	ctx := context.Background()
	b := NewRedisBroker("localhost:6379")

	job, err := sidekiq.NewJob("critical", "TestJob", map[string]string{"foo": "bar"})
	if err != nil {
		t.Fatalf("new job failed: %v", err)
	}

	t.Cleanup(func() {
		b.client.Del(ctx, "sidekiq:job:"+job.ID)
		b.client.ZRem(ctx, "sidekiq:retry:critical", job.ID)
		b.client.LRem(ctx, "sidekiq:queue:critical", 0, job.ID)
		b.client.ZRem(ctx, "sidekiq:active", job.ID)
	})

	if err := b.Enqueue(ctx, job); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}
	got, err := b.Dequeue(ctx, []string{"critical"})
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}

	got.Attempts++
	got.Status = sidekiq.StatusRetry
	// A negative delay puts the retry score in the past - already due -
	// without needing a separate backdating step like the scheduled test.
	if err := b.Requeue(ctx, got, -1*time.Minute); err != nil {
		t.Fatalf("Requeue failed: %v", err)
	}

	n, err := b.PromoteDueJobs(ctx, []string{"critical"})
	if err != nil {
		t.Fatalf("PromoteDueJobs failed: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 promoted job, got %d", n)
	}

	if _, err := b.client.ZScore(ctx, "sidekiq:retry:critical", got.ID).Result(); err != redis.Nil {
		t.Errorf("expected job removed from sidekiq:retry:critical, got err=%v", err)
	}

	if _, err := b.client.LPos(ctx, "sidekiq:queue:critical", got.ID, redis.LPosArgs{}).Result(); err == redis.Nil {
		t.Errorf("expected job present in sidekiq:queue:critical")
	} else if err != nil {
		t.Fatalf("LPos failed: %v", err)
	}
}

func TestPromoteDueJobsSkipsNotYetDue(t *testing.T) {
	ctx := context.Background()
	b := NewRedisBroker("localhost:6379")

	job, err := sidekiq.NewJob("critical", "TestJob", map[string]string{"foo": "bar"})
	if err != nil {
		t.Fatalf("new job failed: %v", err)
	}
	job.ProcessAt = time.Now().Add(1 * time.Hour)

	t.Cleanup(func() {
		b.client.Del(ctx, "sidekiq:job:"+job.ID)
		b.client.ZRem(ctx, "sidekiq:scheduled", job.ID)
		b.client.LRem(ctx, "sidekiq:queue:critical", 0, job.ID)
	})

	if err := b.Enqueue(ctx, job); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	n, err := b.PromoteDueJobs(ctx, []string{"critical"})
	if err != nil {
		t.Fatalf("PromoteDueJobs failed: %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 promoted jobs (not due yet), got %d", n)
	}

	if _, err := b.client.ZScore(ctx, "sidekiq:scheduled", job.ID).Result(); err != nil {
		t.Errorf("expected job to remain in sidekiq:scheduled, ZScore failed: %v", err)
	}
}

func TestGetJobsByStatusDead(t *testing.T) {
	ctx := context.Background()
	b := NewRedisBroker("localhost:6379")

	job, err := sidekiq.NewJob("stats-test-queue", "TestJob", map[string]string{"foo": "bar"})
	if err != nil {
		t.Fatalf("new job failed: %v", err)
	}
	job.MaxAttempts = 1

	t.Cleanup(func() {
		b.client.Del(ctx, "sidekiq:job:"+job.ID)
		b.client.LRem(ctx, "sidekiq:dead:stats-test-queue", 0, job.ID)
	})

	if err := b.Enqueue(ctx, job); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}
	got, err := b.Dequeue(ctx, []string{"stats-test-queue"})
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}
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
			if j.Status != sidekiq.StatusDead {
				t.Errorf("expected returned job status=dead, got %v", j.Status)
			}
		}
	}
	if !found {
		t.Errorf("expected job %s in dead-letter results, got %d jobs", job.ID, len(jobs))
	}
}

func TestGetJobsByStatusCompletedIsAlwaysEmpty(t *testing.T) {
	ctx := context.Background()
	b := NewRedisBroker("localhost:6379")

	jobs, err := b.GetJobsByStatus(ctx, sidekiq.StatusCompleted, 100)
	if err != nil {
		t.Fatalf("GetJobsByStatus failed: %v", err)
	}
	if len(jobs) != 0 {
		t.Errorf("expected no completed jobs (Acknowledge deletes records, none retained), got %d", len(jobs))
	}
}

func TestGetJobsByStatusRejectsUnsupportedStatus(t *testing.T) {
	ctx := context.Background()
	b := NewRedisBroker("localhost:6379")

	_, err := b.GetJobsByStatus(ctx, sidekiq.JobStatus("bogus"), 100)
	if err == nil {
		t.Fatal("expected an error for an unsupported status, got nil")
	}
}

func TestGetQueueStats(t *testing.T) {
	ctx := context.Background()
	b := NewRedisBroker("localhost:6379")

	queue := "stats-queue-2"
	job, err := sidekiq.NewJob(queue, "TestJob", map[string]string{"foo": "bar"})
	if err != nil {
		t.Fatalf("new job failed: %v", err)
	}

	t.Cleanup(func() {
		b.client.Del(ctx, "sidekiq:job:"+job.ID)
		b.client.LRem(ctx, "sidekiq:queue:"+queue, 0, job.ID)
	})

	if err := b.Enqueue(ctx, job); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	stats, err := b.GetQueueStats(ctx)
	if err != nil {
		t.Fatalf("GetQueueStats failed: %v", err)
	}

	s, ok := stats[queue]
	if !ok {
		t.Fatalf("expected stats entry for queue %q, got %v", queue, stats)
	}
	if s.Enqueued < 1 {
		t.Errorf("expected Enqueued >= 1 for queue %q, got %d", queue, s.Enqueued)
	}
}

func TestCloseReleasesConnection(t *testing.T) {
	b := NewRedisBroker("localhost:6379")
	if err := b.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func TestGetJob(t *testing.T) {
	ctx := context.Background()
	b := NewRedisBroker("localhost:6379")

	job, err := sidekiq.NewJob("critical", "TestJob", map[string]string{"foo": "bar"})
	if err != nil {
		t.Fatalf("new job failed: %v", err)
	}
	t.Cleanup(func() {
		b.client.Del(ctx, "sidekiq:job:"+job.ID)
		b.client.LRem(ctx, "sidekiq:queue:critical", 0, job.ID)
	})

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

func TestGetJobReturnsNilForMissingJob(t *testing.T) {
	ctx := context.Background()
	b := NewRedisBroker("localhost:6379")

	got, err := b.GetJob(ctx, "does-not-exist")
	if err != nil {
		t.Fatalf("GetJob failed: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil for a missing job, got %+v", got)
	}
}

func TestRetryDeadJob(t *testing.T) {
	ctx := context.Background()
	b := NewRedisBroker("localhost:6379")

	job, err := sidekiq.NewJob("retry-test-queue", "TestJob", nil)
	if err != nil {
		t.Fatalf("new job failed: %v", err)
	}
	job.MaxAttempts = 1
	t.Cleanup(func() {
		b.client.Del(ctx, "sidekiq:job:"+job.ID)
		b.client.LRem(ctx, "sidekiq:queue:retry-test-queue", 0, job.ID)
		b.client.LRem(ctx, "sidekiq:dead:retry-test-queue", 0, job.ID)
	})

	if err := b.Enqueue(ctx, job); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}
	got, err := b.Dequeue(ctx, []string{"retry-test-queue"})
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

	if _, err := b.client.LPos(ctx, "sidekiq:dead:retry-test-queue", job.ID, redis.LPosArgs{}).Result(); err != redis.Nil {
		t.Errorf("expected job removed from dead-letter, got err=%v", err)
	}
	if _, err := b.client.LPos(ctx, "sidekiq:queue:retry-test-queue", job.ID, redis.LPosArgs{}).Result(); err == redis.Nil {
		t.Errorf("expected job present in sidekiq:queue:retry-test-queue")
	}

	reset, err := b.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("GetJob failed: %v", err)
	}
	if reset.Attempts != 0 {
		t.Errorf("expected Attempts reset to 0, got %d", reset.Attempts)
	}
	if reset.Status != sidekiq.StatusPending {
		t.Errorf("expected Status=pending, got %v", reset.Status)
	}
}

func TestRetryDeadJobFailsIfNotDead(t *testing.T) {
	ctx := context.Background()
	b := NewRedisBroker("localhost:6379")

	job, err := sidekiq.NewJob("critical", "TestJob", nil)
	if err != nil {
		t.Fatalf("new job failed: %v", err)
	}
	t.Cleanup(func() {
		b.client.Del(ctx, "sidekiq:job:"+job.ID)
		b.client.LRem(ctx, "sidekiq:queue:critical", 0, job.ID)
	})

	if err := b.Enqueue(ctx, job); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	if err := b.RetryDeadJob(ctx, job.ID); err == nil {
		t.Error("expected an error retrying a job that isn't dead, got nil")
	}
}

func TestDeleteJob(t *testing.T) {
	ctx := context.Background()
	b := NewRedisBroker("localhost:6379")

	job, err := sidekiq.NewJob("critical", "TestJob", nil)
	if err != nil {
		t.Fatalf("new job failed: %v", err)
	}
	t.Cleanup(func() {
		b.client.Del(ctx, "sidekiq:job:"+job.ID)
		b.client.LRem(ctx, "sidekiq:queue:critical", 0, job.ID)
	})

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
	if _, err := b.client.LPos(ctx, "sidekiq:queue:critical", job.ID, redis.LPosArgs{}).Result(); err != redis.Nil {
		t.Errorf("expected job removed from sidekiq:queue:critical, got err=%v", err)
	}
}
