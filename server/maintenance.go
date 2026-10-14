package server

import (
	"context"
	"log/slog"
	"time"

	"fairq"
	"fairq/internal/base"
	"fairq/internal/clock"
	"fairq/internal/rdb"
)

type tickStats struct{ Scanned, Converted, Isolated, Failed int }

// maintenance is one Forwarder or Recoverer loop, with only an in-memory scan position.
type maintenance struct {
	store                 *rdb.Store
	kind                  string
	cursor                int
	clock                 clock.Clock
	random                func() float64
	batchSize, maxBatches int
	pause                 time.Duration
	logger                *slog.Logger
}

func (m *maintenance) tick(ctx context.Context) tickStats {
	var stats tickStats
	for batch := 0; batch < m.maxBatches; batch++ {
		if ctx.Err() != nil {
			return stats
		}
		page, err := m.store.Scan(ctx, m.kind, m.cursor, m.batchSize, 0)
		if err != nil {
			stats.Failed++
			m.logger.Error("maintenance scan failed", "operation", m.kind, "error", err)
			return stats
		}
		m.cursor = page.Next
		stats.Scanned += page.Scanned
		for _, c := range page.Candidates {
			if ctx.Err() != nil {
				return stats
			}
			if m.kind == "lease" {
				k := c.RetryCount + 1
				if k < 1 {
					k = 1
				}
				var delay time.Duration
				delay, err = base.Backoff(k, m.random)
				if err == nil {
					_, err = m.store.Recover(ctx, c, delay)
				}
			} else {
				err = m.store.Forward(ctx, c)
			}
			switch {
			case err == nil:
				stats.Converted++
			case fairq.IsCode(err, fairq.CorruptReference):
				stats.Isolated++
			case fairq.IsCode(err, fairq.StateConflict) || fairq.IsCode(err, fairq.Empty):
			default:
				stats.Failed++
				m.logger.Error("maintenance conversion failed", "operation", m.kind, "task_id", c.ID, "error", err)
			}
		}
		if page.Next == 0 || page.Scanned == 0 || batch == m.maxBatches-1 {
			return stats
		}
		select {
		case <-ctx.Done():
			return stats
		case <-m.clock.After(m.pause):
		}
	}
	return stats
}
