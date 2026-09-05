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

var setBranchLpushZaddScript = redis.NewScript(`
redis.call('SET', KEYS[1], ARGV[1])

local processAt = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

if processAt <= now then
    redis.call('LPUSH', KEYS[2], ARGV[5])
else
    redis.call('ZADD', KEYS[3], processAt, ARGV[5])
end
return 1
`)

var setZaddZremScript = redis.NewScript(`
redis.call('SET', KEYS[1], ARGV[1])

local processAt = tonumber(ARGV[2])
redis.call('ZADD', KEYS[2], processAt, ARGV[3])

redis.call('ZREM', KEYS[3], ARGV[3])

return 1
`)

var setLpushZremScript = redis.NewScript(`
redis.call('SET', KEYS[1], ARGV[1])

redis.call('LPUSH', KEYS[2], ARGV[2])

redis.call('ZREM', KEYS[3], ARGV[2])

return 1
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
	queueKey := fmt.Sprintf("sidekiq:queue:%s", job.Queue)
	now := time.Now()
	_, err = setBranchLpushZaddScript.Run(
		ctx,
		b.client,
		[]string{jobKey, queueKey, "sidekiq:scheduled"},
		data,
		job.ProcessAt.Unix(),
		now.Unix(),
		job.Queue,
		job.ID,
	).Result()

	if err != nil {
		return fmt.Errorf("atomic enqueu: %w", err)
	}

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

func (b *redisBroker) Acknowledge(ctx context.Context, job *sidekiq.Job) error {
	if err := b.client.ZRem(ctx, "sidekiq:active", job.ID).Err(); err != nil {
		return fmt.Errorf("remove from active: %w", err)
	}

	jobKey := fmt.Sprintf("sidekiq:job:%s", job.ID)
	if err := b.client.Del(ctx, jobKey).Err(); err != nil {
		return fmt.Errorf("delete job key: %w", err)
	}

	return nil
}

func (b *redisBroker) Requeue(ctx context.Context, job *sidekiq.Job, delay time.Duration) error {
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("job data marshal: %w", err)
	}

	jobKey := fmt.Sprintf("sidekiq:job:%s", job.ID)
	retryKey := fmt.Sprintf("sidekiq:retry:%s", job.Queue)
	_, err = setZaddZremScript.Run(
		ctx,
		b.client,
		[]string{jobKey, retryKey, "sidekiq:active"},
		data,
		time.Now().Add(delay).Unix(),
		job.ID,
	).Result()

	if err != nil {
		return fmt.Errorf("atomic requeue: %w", err)
	}

	return nil
}

func (b *redisBroker) MoveToDeadLetter(ctx context.Context, job *sidekiq.Job) error {
	job.Status = sidekiq.StatusDead
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("job data marshal: %w", err)
	}

	jobKey := fmt.Sprintf("sidekiq:job:%s", job.ID)
	deadKey := fmt.Sprintf("sidekiq:dead:%s", job.Queue)
	_, err = setLpushZremScript.Run(
		ctx,
		b.client,
		[]string{jobKey, deadKey, "sidekiq:active"},
		data,
		job.ID,
	).Result()

	if err != nil {
		return fmt.Errorf("atomic move to dead letter: %w", err)
	}

	return nil
}
