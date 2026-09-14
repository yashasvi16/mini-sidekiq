package dispatcher

import (
	"context"
	"reflect"
	"testing"
	"time"

	"sidekiq"
)

func waitForCondition(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

func TestBuildPollSchedule(t *testing.T) {
	schedule := buildPollSchedule([]QueueConfig{
		{Name: "critical", Weight: 3},
		{Name: "default", Weight: 2},
		{Name: "low", Weight: 1},
	})

	want := []string{"critical", "critical", "critical", "default", "default", "low"}
	if !reflect.DeepEqual(schedule, want) {
		t.Errorf("schedule = %v, want %v", schedule, want)
	}
}

func TestDispatcherHonorsWeightedScheduleOrder(t *testing.T) {
	fb := newFakeQueueBroker()
	sub := &fakeSubmitter{}

	mustJob := func(t *testing.T, queue string) *sidekiq.Job {
		t.Helper()
		job, err := sidekiq.NewJob(queue, "Noop", nil)
		if err != nil {
			t.Fatalf("NewJob failed: %v", err)
		}
		return job
	}

	fb.seed("critical", mustJob(t, "critical"), mustJob(t, "critical"), mustJob(t, "critical"))
	fb.seed("default", mustJob(t, "default"), mustJob(t, "default"))
	fb.seed("low", mustJob(t, "low"))

	d := NewDispatcher(fb, sub, []QueueConfig{
		{Name: "critical", Weight: 3},
		{Name: "default", Weight: 2},
		{Name: "low", Weight: 1},
	}, 10*time.Millisecond, nil)

	ctx, cancel := context.WithCancel(context.Background())
	go d.Run(ctx)

	waitForCondition(t, func() bool {
		return len(sub.submitted()) == 6
	})
	cancel()

	var gotOrder []string
	for _, job := range sub.submitted() {
		gotOrder = append(gotOrder, job.Queue)
	}
	wantOrder := []string{"critical", "critical", "critical", "default", "default", "low"}
	if !reflect.DeepEqual(gotOrder, wantOrder) {
		t.Errorf("submitted queue order = %v, want %v", gotOrder, wantOrder)
	}
}

func TestDispatcherStopsOnContextCancellation(t *testing.T) {
	fb := newFakeQueueBroker() // all queues empty - dispatcher will just poll and sleep
	sub := &fakeSubmitter{}

	d := NewDispatcher(fb, sub, []QueueConfig{{Name: "default", Weight: 1}}, 10*time.Millisecond, nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		d.Run(ctx)
		close(done)
	}()

	// let it poll a few empty cycles first
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after ctx cancellation")
	}
}

func TestDispatcherWithNoQueuesReturnsImmediately(t *testing.T) {
	fb := newFakeQueueBroker()
	sub := &fakeSubmitter{}

	d := NewDispatcher(fb, sub, nil, 10*time.Millisecond, nil)

	done := make(chan struct{})
	go func() {
		d.Run(context.Background())
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("Run with no queues configured should return immediately instead of blocking forever")
	}
}
