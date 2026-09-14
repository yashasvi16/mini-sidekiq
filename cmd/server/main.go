// Command server runs a Mini-Sidekiq worker server: it connects to Redis,
// starts the worker pool/dispatcher/scheduler, and processes jobs until it
// receives SIGINT or SIGTERM, at which point it shuts down gracefully -
// letting in-flight jobs finish before exiting.
package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"log/slog"

	"sidekiq"
	"sidekiq/config"
	"sidekiq/pkg/client"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	configPath := flag.String("config", "", "path to a YAML config file (optional - defaults + SIDEKIQ_* env vars apply either way)")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Error("config load failed", "error", err)
		os.Exit(1)
	}

	queues := make(client.Queues, len(cfg.Queues))
	for name, weight := range cfg.Queues {
		queues[name] = weight
	}

	server := client.NewServer(client.ServerConfig{
		RedisAddr:         cfg.RedisAddr,
		Concurrency:       cfg.Concurrency,
		Queues:            queues,
		PollInterval:      cfg.PollInterval,
		SchedulerInterval: cfg.SchedulerInterval,
		Logger:            logger,
	})

	server.Register("SendEmail", sendEmailHandler(logger))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("server starting", "redis_addr", cfg.RedisAddr, "concurrency", cfg.Concurrency, "queues", cfg.Queues)
	server.Run(ctx)
	logger.Info("server stopped")
}

// sendEmailHandler is a placeholder job handler standing in for real work -
// enough to prove the whole pipeline (enqueue -> dispatch -> process ->
// acknowledge) actually runs end to end.
func sendEmailHandler(logger *slog.Logger) client.Handler {
	return func(ctx context.Context, job *sidekiq.Job) error {
		logger.Info("sending email", "job_id", job.ID, "payload", string(job.Payload))
		time.Sleep(200 * time.Millisecond) // simulate work
		return nil
	}
}
