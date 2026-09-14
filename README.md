# Mini-Sidekiq

A distributed job queue for Go, in the spirit of Sidekiq (Ruby), Celery (Python), and BullMQ (Node.js) — built from scratch as a systems-engineering exercise in Go concurrency, at-least-once delivery, and storage-engine tradeoffs.

## What it does

- **Enqueue jobs** from any Go service via a small client library, with priority queues and optional delay.
- **Process jobs** with a fixed-size worker pool, weighted priority dispatch, exponential-backoff retries, and a dead-letter queue for permanent failures.
- **Two interchangeable storage backends** behind one `Broker` interface: Redis (Lua-scripted atomicity) and PostgreSQL (`SELECT ... FOR UPDATE SKIP LOCKED`).
- **A dashboard** (REST API + embedded UI) for inspecting queues, filtering jobs by status, retrying dead jobs, and deleting jobs.
- **Graceful shutdown**: stop accepting new work, let in-flight jobs finish, then exit.

## Quick start

```bash
docker compose up -d          # Redis + Postgres
go run ./cmd/server            # starts processing jobs
go run ./cmd/dashboard         # dashboard at http://localhost:8080
```

Enqueue a job from your own code:

```go
c := client.NewClient(client.Config{RedisAddr: "localhost:6379"})
defer c.Close()

job, err := c.Enqueue(ctx, "SendEmail", client.P{"to": "user@example.com"},
    client.EnqueueOptions{Queue: "critical"})
```

Register a handler and run a server:

```go
server := client.NewServer(client.ServerConfig{
    RedisAddr:   "localhost:6379",
    Concurrency: 10,
    Queues:      client.Queues{"critical": 3, "default": 2, "low": 1},
})
server.Register("SendEmail", func(ctx context.Context, job *sidekiq.Job) error {
    return sendEmail(job.Payload)
})
server.Run(ctx) // blocks until ctx is cancelled, then drains gracefully
```

Config can come from a YAML file, environment variables (`SIDEKIQ_REDIS_ADDR`, `SIDEKIQ_CONCURRENCY`, ...), or defaults — see `config.example.yaml` and `-config` on both commands.

## Architecture

```
Client.Enqueue → Broker (Redis or Postgres)
                     │
              Dispatcher (weighted priority poll)
                     │
              Pool.Submit → worker goroutines → Middleware chain → Handler
                     │                                  │
              (success)                            (failure)
                     │                                  │
              Acknowledge                    Requeue (< MaxAttempts)
                                              or MoveToDeadLetter

Scheduler: polls due scheduled/retry jobs → back into their queue
Dashboard: REST API + embedded UI, reads/writes via the same Broker
```

| Package | Responsibility |
|---|---|
| `job.go` (root) | `Job`/`JobStatus`, client-side ID generation (enables idempotent retry of `Enqueue`) |
| `internal/broker` | `Broker` interface + Redis and Postgres implementations |
| `internal/worker` | `Registry` (job type → handler), `Pool` (goroutines, backpressure, graceful shutdown) |
| `internal/dispatcher` | Weighted priority polling across queues |
| `internal/scheduler` | Promotes due scheduled/retry jobs back into their queue |
| `internal/retry` | Exponential backoff with jitter |
| `internal/middleware` | Composable `Handler` chain: logging, recovery, timeout, Prometheus metrics |
| `internal/dashboard` | REST API + embedded UI |
| `pkg/client` | Public API (`Client`, `Server`) |
| `config` | YAML + env config loading (viper) |

## Why two storage backends

The `Broker` interface abstracts "where jobs live," but the two implementations solve atomicity completely differently — deliberately, to show *why* each storage engine's primitives fit the problem, not just to have two options:

- **Redis** has no transactions with conditional logic, so multi-step operations (pop-and-record, claim-and-promote) are Lua scripts — the whole script runs as one indivisible unit on the server, letting one command's result feed the next atomically.
- **Postgres** gets atomicity from row-level locking: `SELECT ... FOR UPDATE SKIP LOCKED` lets many workers claim different rows concurrently without ever blocking on or double-claiming the same one, and a single `UPDATE ... WHERE ... RETURNING` is atomic by nature — no scripting needed.
- **Scheduled jobs** need an explicit "promotion" step in Redis (moving between a sorted set and a list are physically different structures) but are a no-op in Postgres (a due job is just a row whose `process_at` already satisfies `Dequeue`'s own `WHERE` clause).

## Key engineering concepts

- **At-least-once delivery**: a job is never removed from its queue until acknowledged. Redis: `RPOP`+`ZADD` atomically moves it to an "active" set with a deadline score. Postgres: the row's `status` column *is* the in-flight marker.
- **Idempotent enqueue**: job IDs are generated client-side (`sidekiq.NewJob`), not by the broker — so retrying a failed `Enqueue` call after an ambiguous network error is safe; a broker-generated ID couldn't offer that.
- **Backpressure**: `Pool.Submit` blocks once its internal buffered channel is full, naturally throttling the dispatcher instead of accepting unbounded work.
- **Graceful shutdown**: cancelling `Server.Run`'s context stops the dispatcher/scheduler first (no new work fetched), then waits for the worker pool to finish every in-flight job before returning.
- **Panic isolation**: a job handler that panics can't take down its worker goroutine — `runtime/debug.Stack()` is captured and turned into a normal error.

## Testing

```bash
make test        # everything except real Redis/Postgres integration tests
make test-race    # full suite with the race detector
make test-integration  # broker tests against real Redis + Postgres (docker compose)
```

Worker/dispatcher/scheduler/middleware/dashboard tests use in-memory fakes — no external services needed. Broker tests are real integration tests against both backends via `docker-compose.yml`.

## Known limitations

- **Reaper not implemented**: a crashed worker's in-flight job (Redis: stuck in `sidekiq:active`; Postgres: stuck `status='active'`) isn't automatically requeued yet. The score/timestamp needed to detect "stuck too long" is already recorded — a periodic scan-and-requeue process is the natural next addition.
- **`UniqueKey` idempotency** is enforced by Postgres (a partial unique index) but not yet by the Redis broker.
- **Completed-job history**: Postgres retains completed rows; Redis's `Acknowledge` deletes the job record entirely, so `GetJobsByStatus(StatusCompleted)` is always empty there.
- **No queue pause/resume** in the dashboard (in the original plan's API sketch, not implemented here).
- **Windows dev note**: if a native PostgreSQL install already occupies port 5432, the docker-compose Postgres service maps to 5433 instead — see `docker-compose.yml`.
