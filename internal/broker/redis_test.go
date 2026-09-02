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
