package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"sidekiq"
)

type redisBroker struct {
	client *redis.Client
}

var rpopZaddScript = redis.NewScript(`
local id = redis.call('RPOP', KEYS[1])
if not id then
	return nil
end
redis.call('ZADD', KEYS[2], ARGV[1], id)
return id
`)

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

	now := time.Now()
	if !job.ProcessAt.After(now) {
		queueKey := fmt.Sprintf("sidekiq:queue:%s", job.Queue)
		if err := b.client.LPush(ctx, queueKey, job.ID).Err(); err != nil {
			return fmt.Errorf("push to queue: %w", err)
		}
	} else {
		err := b.client.ZAdd(ctx, "sidekiq:scheduled", redis.Z{Score: float64(job.ProcessAt.Unix()), Member: job.ID}).Err()
		if err != nil {
			return fmt.Errorf("add to scheduled: %w", err)
		}
	}

	// TODO: not atomic — job data write and queue push are two separate
	// round trips. If the process crashes between them, sidekiq:job:{id}
	// exists but nothing points to it, so it's never processed. Revisit
	// with a Lua script or MULTI/EXEC once retry/dead-letter (Phase 5)
	// forces the same question there too.

	return nil
}

func (b *redisBroker) Dequeue(ctx context.Context, queues []string) (*sidekiq.Job, error) {
	var jobID string
	for _, queue := range queues {
		queuekey := fmt.Sprintf("sidekiq:queue:%s", queue)
		timeout := time.Duration(30 * time.Second)
		result, err := rpopZaddScript.Run(ctx, b.client, []string{queuekey, "sidekiq:active"}, time.Now().Add(timeout).Unix()).Result()
		if err == redis.Nil {
			continue
		}

		if err != nil {
			return nil, fmt.Errorf("script run %w", err)
		}

		if result != nil {
			jobID = result.(string)
			break
		}
	}

	if jobID == "" {
		return nil, nil
	}

	jobKey := fmt.Sprintf("sidekiq:job:%s", jobID)
	data, err := b.client.Get(ctx, jobKey).Result()
	if err != nil {
		return nil, fmt.Errorf("get job in dequeue: %w", err)
	}

	var job sidekiq.Job
	err = json.Unmarshal([]byte(data), &job)
	if err != nil {
		return nil, fmt.Errorf("job unmarshal: %w", err)
	}

	return &job, nil
}
