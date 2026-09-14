// Command dashboard runs the Mini-Sidekiq HTTP dashboard: a REST API plus
// an embedded web UI for inspecting queues and jobs, backed by the same
// Redis broker the worker server uses.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"sidekiq/config"
	"sidekiq/internal/broker"
	"sidekiq/internal/dashboard"
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

	b := broker.NewRedisBroker(cfg.RedisAddr)
	dash := dashboard.NewServer(b)

	srv := &http.Server{Addr: cfg.DashboardAddr, Handler: dash}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("dashboard starting", "addr", cfg.DashboardAddr, "redis_addr", cfg.RedisAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server error", "error", err)
		}
	}()

	<-ctx.Done()
	logger.Info("dashboard shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown error", "error", err)
	}
	if err := b.Close(); err != nil {
		logger.Error("broker close failed", "error", err)
	}
}
