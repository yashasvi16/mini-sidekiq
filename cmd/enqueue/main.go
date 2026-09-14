// Command enqueue is a small CLI for pushing test jobs onto the queue
// without writing a throwaway program each time - handy for exercising the
// server/dashboard by hand.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"sidekiq/pkg/client"
)

func main() {
	redisAddr := flag.String("redis", "localhost:6379", "Redis address")
	queue := flag.String("queue", "default", "queue name (e.g. critical, default, low)")
	jobType := flag.String("type", "SendEmail", "job type - must match a handler registered in cmd/server (SendEmail, FailingJob)")
	payload := flag.String("payload", `{"to":"test@example.com"}`, "JSON payload")
	delay := flag.Duration("in", 0, "delay before the job becomes eligible to run, e.g. -in 10s")
	maxAttempts := flag.Int("max-attempts", 0, "override MaxAttempts (0 = default of 3) - lower this to reach dead-letter faster when testing FailingJob")
	count := flag.Int("count", 1, "how many copies to enqueue")
	flag.Parse()

	var payloadValue any
	if err := json.Unmarshal([]byte(*payload), &payloadValue); err != nil {
		fmt.Fprintln(os.Stderr, "invalid JSON payload:", err)
		os.Exit(1)
	}

	c := client.NewClient(client.Config{RedisAddr: *redisAddr})
	defer c.Close()

	ctx := context.Background()
	for i := 0; i < *count; i++ {
		job, err := c.Enqueue(ctx, *jobType, payloadValue, client.EnqueueOptions{Queue: *queue, In: *delay, MaxAttempts: *maxAttempts})
		if err != nil {
			fmt.Fprintln(os.Stderr, "enqueue failed:", err)
			os.Exit(1)
		}
		fmt.Printf("enqueued %s  queue=%s  type=%s", job.ID, job.Queue, job.Type)
		if *delay > 0 {
			fmt.Printf("  runs at %s", time.Now().Add(*delay).Format(time.Kitchen))
		}
		fmt.Println()
	}
}
