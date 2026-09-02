package broker

import (
	"context"
	"testing"

	"sidekiq"
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
