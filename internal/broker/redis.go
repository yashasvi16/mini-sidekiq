package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"sidekiq"
)

type redisBroker struct {
	client *redis.Client
}

var _ Broker = (*redisBroker)(nil)

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

// retryDeadJobScript atomically claims a job out of its dead-letter list
// (LREM removing at most one occurrence - the "claim" step, same reasoning
// as promoteJobScript's ZREM) and pushes it back onto its active queue with
// freshly reset job data.
var retryDeadJobScript = redis.NewScript(`
local deadKey = KEYS[1]
local jobKey = KEYS[2]
local queueKey = KEYS[3]
local jobID = ARGV[1]
local newData = ARGV[2]

local removed = redis.call('LREM', deadKey, 1, jobID)
if removed == 0 then
	return 0
end

redis.call('SET', jobKey, newData)
redis.call('LPUSH', queueKey, jobID)
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

// promoteJobScript atomically claims one due job out of a scheduled/retry
// sorted set and pushes it onto its target active queue. The ZREM "claim"
// step means two concurrent scheduler instances racing on the same job ID
// can't both promote it - only one ZREM actually removes anything.
var promoteJobScript = redis.NewScript(`
local sourceKey = KEYS[1]
local jobKey = KEYS[2]
local jobID = ARGV[1]

local removed = redis.call('ZREM', sourceKey, jobID)
if removed == 0 then
	return 0
end

local jobData = redis.call('GET', jobKey)
if not jobData then
	return 0
end

local decoded = cjson.decode(jobData)
local queueKey = 'sidekiq:queue:' .. decoded['queue']
redis.call('LPUSH', queueKey, jobID)
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

func (b *redisBroker) PromoteDueJobs(ctx context.Context, queues []string) (int, error) {
	now := strconv.FormatInt(time.Now().Unix(), 10)

	sourceKeys := []string{"sidekiq:scheduled"}
	for _, q := range queues {
		sourceKeys = append(sourceKeys, fmt.Sprintf("sidekiq:retry:%s", q))
	}

	promoted := 0
	for _, sourceKey := range sourceKeys {
		ids, err := b.client.ZRangeByScore(ctx, sourceKey, &redis.ZRangeBy{
			Min: "-inf",
			Max: now,
		}).Result()
		if err != nil {
			return promoted, fmt.Errorf("zrangebyscore %s: %w", sourceKey, err)
		}

		for _, id := range ids {
			jobKey := fmt.Sprintf("sidekiq:job:%s", id)
			result, err := promoteJobScript.Run(ctx, b.client, []string{sourceKey, jobKey}, id).Result()
			if err != nil {
				return promoted, fmt.Errorf("promote job %s: %w", id, err)
			}
			if n, ok := result.(int64); ok && n == 1 {
				promoted++
			}
		}
	}

	return promoted, nil
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

// GetJobsByStatus returns up to limit jobs currently in the given status.
//
// Pending, retry, and dead jobs are found by scanning the per-queue keys
// that hold them (sidekiq:queue:*, sidekiq:retry:*, sidekiq:dead:*) - queue
// names aren't tracked anywhere centrally, so SCAN is how the broker
// discovers which queues currently exist. Active jobs come from the single
// shared sidekiq:active set. Completed jobs are not retained anywhere -
// Acknowledge deletes a job's record entirely once it succeeds - so this
// always returns an empty slice for sidekiq.StatusCompleted; keeping a
// bounded completed-job history would need Acknowledge to change to keep
// records around (e.g. with a TTL) instead of deleting them outright.
func (b *redisBroker) GetJobsByStatus(ctx context.Context, status sidekiq.JobStatus, limit int) ([]*sidekiq.Job, error) {
	var ids []string
	var err error

	switch status {
	case sidekiq.StatusDead:
		ids, err = b.collectIDsFromLists(ctx, "sidekiq:dead:*", limit)
	case sidekiq.StatusRetry:
		ids, err = b.collectIDsFromLists(ctx, "sidekiq:retry:*", limit)
	case sidekiq.StatusPending:
		ids, err = b.collectIDsFromLists(ctx, "sidekiq:queue:*", limit)
	case sidekiq.StatusActive:
		ids, err = b.client.ZRange(ctx, "sidekiq:active", 0, int64(limit)-1).Result()
	case sidekiq.StatusCompleted:
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported status %q", status)
	}
	if err != nil {
		return nil, err
	}

	jobs := make([]*sidekiq.Job, 0, len(ids))
	for _, id := range ids {
		data, err := b.client.Get(ctx, "sidekiq:job:"+id).Result()
		if err == redis.Nil {
			continue // listed but its record is gone (raced with another op) - skip
		}
		if err != nil {
			return jobs, fmt.Errorf("get job %s: %w", id, err)
		}
		var job sidekiq.Job
		if err := json.Unmarshal([]byte(data), &job); err != nil {
			return jobs, fmt.Errorf("unmarshal job %s: %w", id, err)
		}
		jobs = append(jobs, &job)
	}
	return jobs, nil
}

// collectIDsFromLists scans for keys matching pattern - each expected to be
// a Redis List - and gathers up to limit member IDs across all of them.
func (b *redisBroker) collectIDsFromLists(ctx context.Context, pattern string, limit int) ([]string, error) {
	var ids []string
	iter := b.client.Scan(ctx, 0, pattern, 0).Iterator()
	for iter.Next(ctx) {
		if len(ids) >= limit {
			break
		}
		key := iter.Val()
		remaining := int64(limit - len(ids))
		vals, err := b.client.LRange(ctx, key, 0, remaining-1).Result()
		if err != nil {
			return ids, fmt.Errorf("lrange %s: %w", key, err)
		}
		ids = append(ids, vals...)
	}
	if err := iter.Err(); err != nil {
		return ids, fmt.Errorf("scan %s: %w", pattern, err)
	}
	return ids, nil
}

// GetQueueStats reports per-queue depth across pending, retry, and dead
// storage (found the same way GetJobsByStatus finds them - scanning for
// per-queue keys). Scheduled and active jobs live in single shared keys
// rather than per-queue ones, so attributing them to a queue means reading
// each entry's own stored Queue field.
func (b *redisBroker) GetQueueStats(ctx context.Context) (map[string]QueueStats, error) {
	stats := make(map[string]QueueStats)

	if err := b.scanEach(ctx, "sidekiq:queue:*", func(key string) error {
		queue := strings.TrimPrefix(key, "sidekiq:queue:")
		n, err := b.client.LLen(ctx, key).Result()
		if err != nil {
			return err
		}
		s := stats[queue]
		s.Queue = queue
		s.Enqueued = int(n)
		stats[queue] = s
		return nil
	}); err != nil {
		return nil, fmt.Errorf("scan pending queues: %w", err)
	}

	if err := b.scanEach(ctx, "sidekiq:retry:*", func(key string) error {
		queue := strings.TrimPrefix(key, "sidekiq:retry:")
		n, err := b.client.ZCard(ctx, key).Result()
		if err != nil {
			return err
		}
		s := stats[queue]
		s.Queue = queue
		s.Retry = int(n)
		stats[queue] = s
		return nil
	}); err != nil {
		return nil, fmt.Errorf("scan retry queues: %w", err)
	}

	if err := b.scanEach(ctx, "sidekiq:dead:*", func(key string) error {
		queue := strings.TrimPrefix(key, "sidekiq:dead:")
		n, err := b.client.LLen(ctx, key).Result()
		if err != nil {
			return err
		}
		s := stats[queue]
		s.Queue = queue
		s.Dead = int(n)
		stats[queue] = s
		return nil
	}); err != nil {
		return nil, fmt.Errorf("scan dead queues: %w", err)
	}

	if err := b.attributeGlobalSetToQueues(ctx, "sidekiq:scheduled", stats, func(s *QueueStats) *int { return &s.Scheduled }); err != nil {
		return nil, err
	}
	if err := b.attributeGlobalSetToQueues(ctx, "sidekiq:active", stats, func(s *QueueStats) *int { return &s.Active }); err != nil {
		return nil, err
	}

	return stats, nil
}

func (b *redisBroker) scanEach(ctx context.Context, pattern string, fn func(key string) error) error {
	iter := b.client.Scan(ctx, 0, pattern, 0).Iterator()
	for iter.Next(ctx) {
		if err := fn(iter.Val()); err != nil {
			return err
		}
	}
	return iter.Err()
}

// attributeGlobalSetToQueues reads every member of a single shared sorted
// set (sidekiq:scheduled or sidekiq:active), fetches each job's own record
// to learn which queue it belongs to, and increments the field field
// selects on that queue's entry in stats.
func (b *redisBroker) attributeGlobalSetToQueues(ctx context.Context, setKey string, stats map[string]QueueStats, field func(*QueueStats) *int) error {
	ids, err := b.client.ZRange(ctx, setKey, 0, -1).Result()
	if err != nil {
		return fmt.Errorf("zrange %s: %w", setKey, err)
	}
	for _, id := range ids {
		data, err := b.client.Get(ctx, "sidekiq:job:"+id).Result()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			return fmt.Errorf("get job %s: %w", id, err)
		}
		var job sidekiq.Job
		if err := json.Unmarshal([]byte(data), &job); err != nil {
			return fmt.Errorf("unmarshal job %s: %w", id, err)
		}
		s := stats[job.Queue]
		s.Queue = job.Queue
		*field(&s) = *field(&s) + 1
		stats[job.Queue] = s
	}
	return nil
}

// GetJob fetches a single job's record by ID.
func (b *redisBroker) GetJob(ctx context.Context, id string) (*sidekiq.Job, error) {
	data, err := b.client.Get(ctx, "sidekiq:job:"+id).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get job %s: %w", id, err)
	}

	var job sidekiq.Job
	if err := json.Unmarshal([]byte(data), &job); err != nil {
		return nil, fmt.Errorf("unmarshal job %s: %w", id, err)
	}
	return &job, nil
}

// RetryDeadJob resets a dead job (clears Attempts/LastError, sets ProcessAt
// to now) and atomically moves it from its dead-letter list back onto its
// active queue.
func (b *redisBroker) RetryDeadJob(ctx context.Context, id string) error {
	job, err := b.GetJob(ctx, id)
	if err != nil {
		return err
	}
	if job == nil {
		return fmt.Errorf("job %s not found", id)
	}

	job.Status = sidekiq.StatusPending
	job.Attempts = 0
	job.LastError = ""
	job.ProcessAt = time.Now()

	newData, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal job %s: %w", id, err)
	}

	deadKey := fmt.Sprintf("sidekiq:dead:%s", job.Queue)
	jobKey := fmt.Sprintf("sidekiq:job:%s", id)
	queueKey := fmt.Sprintf("sidekiq:queue:%s", job.Queue)

	result, err := retryDeadJobScript.Run(ctx, b.client, []string{deadKey, jobKey, queueKey}, id, newData).Result()
	if err != nil {
		return fmt.Errorf("retry dead job %s: %w", id, err)
	}
	if n, ok := result.(int64); !ok || n != 1 {
		return fmt.Errorf("job %s not found in dead-letter queue %s", id, job.Queue)
	}
	return nil
}

// DeleteJob removes a job's record and clears any reference to it from
// every structure it could plausibly be in. This is a best-effort admin
// operation, not part of the hot path, so it isn't a single atomic script -
// each removal is independently idempotent (removing something already
// absent is a harmless no-op), so a partial failure just means re-running
// it finishes the job instead of corrupting anything.
func (b *redisBroker) DeleteJob(ctx context.Context, id string) error {
	job, err := b.GetJob(ctx, id)
	if err != nil {
		return err
	}
	if job == nil {
		return nil // already gone
	}

	pipe := b.client.Pipeline()
	pipe.LRem(ctx, "sidekiq:queue:"+job.Queue, 0, id)
	pipe.ZRem(ctx, "sidekiq:retry:"+job.Queue, id)
	pipe.LRem(ctx, "sidekiq:dead:"+job.Queue, 0, id)
	pipe.ZRem(ctx, "sidekiq:active", id)
	pipe.ZRem(ctx, "sidekiq:scheduled", id)
	pipe.Del(ctx, "sidekiq:job:"+id)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("delete job %s: %w", id, err)
	}
	return nil
}

// Close releases the underlying Redis connection pool.
func (b *redisBroker) Close() error {
	return b.client.Close()
}
