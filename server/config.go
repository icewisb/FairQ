package server

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

type Config struct {
	Redis             redis.UniversalOptions
	Concurrency       int
	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
	RecoverInterval   time.Duration
	ForwarderInterval time.Duration
	PollInterval      time.Duration
	ShutdownTimeout   time.Duration
	BatchSize         int
	MaxBatchesPerTick int
	BatchPause        time.Duration
	Logger            *slog.Logger
}

func (c *Config) defaults() error {
	if c.Concurrency <= 0 || c.ShutdownTimeout <= 0 || c.ShutdownTimeout%time.Millisecond != 0 {
		return fmt.Errorf("fairq: Concurrency and ShutdownTimeout must be positive")
	}
	for field, value := range map[*time.Duration]time.Duration{&c.LeaseDuration: 30 * time.Second, &c.HeartbeatInterval: 10 * time.Second, &c.RecoverInterval: 10 * time.Second, &c.ForwarderInterval: time.Second, &c.PollInterval: time.Second, &c.BatchPause: 10 * time.Millisecond} {
		if *field == 0 {
			*field = value
		}
		if *field < time.Millisecond || *field%time.Millisecond != 0 {
			return fmt.Errorf("fairq: durations must be positive integer milliseconds")
		}
	}
	if c.HeartbeatInterval >= c.LeaseDuration {
		return fmt.Errorf("fairq: heartbeat must be shorter than lease")
	}
	if c.BatchSize == 0 {
		c.BatchSize = 100
	}
	if c.MaxBatchesPerTick == 0 {
		c.MaxBatchesPerTick = 5
	}
	if c.BatchSize < 1 || c.BatchSize > 100 || c.MaxBatchesPerTick < 1 || c.MaxBatchesPerTick > 5 {
		return fmt.Errorf("fairq: maintenance limit exceeds 100 candidates × 5 batches")
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return nil
}
