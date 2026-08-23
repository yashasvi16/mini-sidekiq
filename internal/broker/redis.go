package broker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"

	"sidekiq"
)

type redisBroker struct {
	client *redis.Client
}

func NewRedisBroker(addr string) *redisBroker {
	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	return &redisBroker{client: client}
}

func (b *redisBroker) Enqueue(ctx context.Context, job *sidekiq.Job) error {
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}

	jobKey := fmt.Sprintf("sidekiq:job:%s", job.ID)
	if err := b.client.Set(ctx, jobKey, data, 0).Err(); err != nil {
		return fmt.Errorf("store job data: %w", err)
	}

	queueKey := fmt.Sprintf("sidekiq:queue:%s", job.Queue)
	if err := b.client.LPush(ctx, queueKey, job.ID).Err(); err != nil {
		return fmt.Errorf("push to queue: %w", err)
	}

	// TODO: not atomic — job data write and queue push are two separate
	// round trips. If the process crashes between them, sidekiq:job:{id}
	// exists but nothing points to it, so it's never processed. Revisit
	// with a Lua script or MULTI/EXEC once retry/dead-letter (Phase 5)
	// forces the same question there too.

	return nil
}
