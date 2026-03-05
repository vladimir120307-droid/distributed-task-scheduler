package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config holds the full application configuration tree.
type Config struct {
	Scheduler SchedulerConfig `yaml:"scheduler"`
	Worker    WorkerConfig    `yaml:"worker"`
	Storage   StorageConfig   `yaml:"storage"`
	HashRing  HashRingConfig  `yaml:"hash_ring"`
	Retry     RetryConfig     `yaml:"retry"`
	Logging   LoggingConfig   `yaml:"logging"`
}

// SchedulerConfig controls the scheduler node.
type SchedulerConfig struct {
	ListenAddr        string        `yaml:"listen_addr"`
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
	ElectionTimeout   time.Duration `yaml:"election_timeout"`
	RebalanceInterval time.Duration `yaml:"rebalance_interval"`
	MaxTasksInFlight  int           `yaml:"max_tasks_in_flight"`
	DrainTimeout      time.Duration `yaml:"drain_timeout"`
}

// WorkerConfig controls the worker node.
type WorkerConfig struct {
	Concurrency           int           `yaml:"concurrency"`
	HealthCheckInterval   time.Duration `yaml:"health_check_interval"`
	TaskTimeout           time.Duration `yaml:"task_timeout"`
	RegisterRetryInterval time.Duration `yaml:"register_retry_interval"`
	MaxRegisterRetries    int           `yaml:"max_register_retries"`
	ShutdownGracePeriod   time.Duration `yaml:"shutdown_grace_period"`
}

// StorageConfig selects the storage backend.
type StorageConfig struct {
	Backend         string        `yaml:"backend"`
	GCInterval      time.Duration `yaml:"gc_interval"`
	RetentionPeriod time.Duration `yaml:"retention_period"`
}

// HashRingConfig tunes the consistent hashing ring.
type HashRingConfig struct {
	VirtualNodes      int `yaml:"virtual_nodes"`
	ReplicationFactor int `yaml:"replication_factor"`
}

// RetryConfig defines exponential backoff parameters.
type RetryConfig struct {
	MaxRetries     int           `yaml:"max_retries"`
	BaseDelay      time.Duration `yaml:"base_delay"`
	MaxDelay       time.Duration `yaml:"max_delay"`
	Multiplier     float64       `yaml:"multiplier"`
	JitterFraction float64       `yaml:"jitter_fraction"`
}

// LoggingConfig controls log output.
type LoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
	Output string `yaml:"output"`
}

// Load reads a YAML configuration file at the given path and returns a
// populated Config. Missing fields are filled with sensible defaults.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}

	cfg := DefaultConfig()
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}

	return cfg, nil
}

// DefaultConfig returns a Config with sensible defaults for every field.
func DefaultConfig() *Config {
	return &Config{
		Scheduler: SchedulerConfig{
			ListenAddr:        ":9090",
			HeartbeatInterval: 2 * time.Second,
			ElectionTimeout:   10 * time.Second,
			RebalanceInterval: 30 * time.Second,
			MaxTasksInFlight:  10000,
			DrainTimeout:      30 * time.Second,
		},
		Worker: WorkerConfig{
			Concurrency:           4,
			HealthCheckInterval:   5 * time.Second,
			TaskTimeout:           300 * time.Second,
			RegisterRetryInterval: 5 * time.Second,
			MaxRegisterRetries:    10,
			ShutdownGracePeriod:   15 * time.Second,
		},
		Storage: StorageConfig{
			Backend:         "memory",
			GCInterval:      60 * time.Second,
			RetentionPeriod: 168 * time.Hour,
		},
		HashRing: HashRingConfig{
			VirtualNodes:      150,
			ReplicationFactor: 2,
		},
		Retry: RetryConfig{
			MaxRetries:     3,
			BaseDelay:      1 * time.Second,
			MaxDelay:       60 * time.Second,
			Multiplier:     2.0,
			JitterFraction: 0.1,
		},
		Logging: LoggingConfig{
			Level:  "info",
			Format: "json",
			Output: "stdout",
		},
	}
}

// Validate performs basic sanity checks on the configuration values.
func (c *Config) Validate() error {
	if c.Scheduler.ListenAddr == "" {
		return fmt.Errorf("scheduler.listen_addr must not be empty")
	}
	if c.Scheduler.HeartbeatInterval <= 0 {
		return fmt.Errorf("scheduler.heartbeat_interval must be positive")
	}
	if c.Scheduler.ElectionTimeout <= c.Scheduler.HeartbeatInterval {
		return fmt.Errorf("scheduler.election_timeout must exceed heartbeat_interval")
	}
	if c.Worker.Concurrency < 1 {
		return fmt.Errorf("worker.concurrency must be at least 1")
	}
	if c.Worker.TaskTimeout <= 0 {
		return fmt.Errorf("worker.task_timeout must be positive")
	}
	if c.HashRing.VirtualNodes < 1 {
		return fmt.Errorf("hash_ring.virtual_nodes must be at least 1")
	}
	if c.HashRing.ReplicationFactor < 1 {
		return fmt.Errorf("hash_ring.replication_factor must be at least 1")
	}
	if c.Retry.MaxRetries < 0 {
		return fmt.Errorf("retry.max_retries must be non-negative")
	}
	if c.Retry.BaseDelay <= 0 {
		return fmt.Errorf("retry.base_delay must be positive")
	}
	if c.Retry.Multiplier < 1.0 {
		return fmt.Errorf("retry.multiplier must be >= 1.0")
	}
	if c.Retry.JitterFraction < 0 || c.Retry.JitterFraction > 1 {
		return fmt.Errorf("retry.jitter_fraction must be in [0, 1]")
	}
	return nil
}
