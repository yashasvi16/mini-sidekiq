// Package config loads runtime configuration for the worker server and
// dashboard from an optional YAML file plus environment variable overrides.
package config

import (
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config holds everything cmd/server and cmd/dashboard need to run.
type Config struct {
	RedisAddr         string         `mapstructure:"redis_addr"`
	DashboardAddr     string         `mapstructure:"dashboard_addr"`
	Concurrency       int            `mapstructure:"concurrency"`
	Queues            map[string]int `mapstructure:"queues"`
	PollInterval      time.Duration  `mapstructure:"poll_interval"`
	SchedulerInterval time.Duration  `mapstructure:"scheduler_interval"`
}

func defaults() Config {
	return Config{
		RedisAddr:         "localhost:6379",
		DashboardAddr:     ":8080",
		Concurrency:       10,
		Queues:            map[string]int{"critical": 3, "default": 2, "low": 1},
		PollInterval:      250 * time.Millisecond,
		SchedulerInterval: 5 * time.Second,
	}
}

// Load reads configuration from path (a YAML file - optional; a missing
// file is not an error, it just means defaults+env apply) layered with
// environment variable overrides. Any scalar field can be overridden via
// SIDEKIQ_<KEY>, e.g. SIDEKIQ_REDIS_ADDR or SIDEKIQ_CONCURRENCY - env always
// wins over the file. Queues (a map) is only configurable via the YAML file;
// expressing a map through a single env var isn't a natural fit, so that
// override path is deliberately not supported.
func Load(path string) (Config, error) {
	v := viper.New()
	d := defaults()
	v.SetDefault("redis_addr", d.RedisAddr)
	v.SetDefault("dashboard_addr", d.DashboardAddr)
	v.SetDefault("concurrency", d.Concurrency)
	v.SetDefault("queues", d.Queues)
	v.SetDefault("poll_interval", d.PollInterval)
	v.SetDefault("scheduler_interval", d.SchedulerInterval)

	v.SetEnvPrefix("sidekiq")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	if path != "" {
		v.SetConfigFile(path)
		if err := v.ReadInConfig(); err != nil {
			// With an explicit SetConfigFile path (as opposed to
			// SetConfigName+AddConfigPath's search behavior), a missing
			// file surfaces as a plain fs.PathError, not viper's own
			// ConfigFileNotFoundError - that type is only ever returned
			// when viper is searching multiple candidate paths itself.
			var notFoundErr viper.ConfigFileNotFoundError
			if !errors.As(err, &notFoundErr) && !errors.Is(err, fs.ErrNotExist) {
				return Config{}, fmt.Errorf("read config %s: %w", path, err)
			}
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return Config{}, fmt.Errorf("unmarshal config: %w", err)
	}
	return cfg, nil
}
