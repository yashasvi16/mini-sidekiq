package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadDefaultsWithNoFile(t *testing.T) {
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.RedisAddr != "localhost:6379" {
		t.Errorf("RedisAddr = %q, want localhost:6379", cfg.RedisAddr)
	}
	if cfg.Concurrency != 10 {
		t.Errorf("Concurrency = %d, want 10", cfg.Concurrency)
	}
	if cfg.Queues["critical"] != 3 {
		t.Errorf("Queues[critical] = %d, want 3", cfg.Queues["critical"])
	}
	if cfg.PollInterval != 250*time.Millisecond {
		t.Errorf("PollInterval = %v, want 250ms", cfg.PollInterval)
	}
}

func TestLoadMissingFileIsNotAnError(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "does-not-exist.yaml"))
	if err != nil {
		t.Errorf("expected a missing config file to be a non-error (defaults apply), got: %v", err)
	}
}

func TestLoadFromYAMLFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	yaml := `
redis_addr: "redis.internal:6379"
concurrency: 42
poll_interval: "500ms"
queues:
  urgent: 5
`
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatalf("write temp config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.RedisAddr != "redis.internal:6379" {
		t.Errorf("RedisAddr = %q, want redis.internal:6379", cfg.RedisAddr)
	}
	if cfg.Concurrency != 42 {
		t.Errorf("Concurrency = %d, want 42", cfg.Concurrency)
	}
	if cfg.PollInterval != 500*time.Millisecond {
		t.Errorf("PollInterval = %v, want 500ms", cfg.PollInterval)
	}
	if cfg.Queues["urgent"] != 5 {
		t.Errorf("Queues[urgent] = %d, want 5", cfg.Queues["urgent"])
	}
}

func TestLoadEnvOverridesFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(`redis_addr: "from-file:6379"`), 0644); err != nil {
		t.Fatalf("write temp config: %v", err)
	}

	t.Setenv("SIDEKIQ_REDIS_ADDR", "from-env:6379")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.RedisAddr != "from-env:6379" {
		t.Errorf("RedisAddr = %q, want from-env:6379 (env should win over file)", cfg.RedisAddr)
	}
}
