package server

import (
	"context"
	"log/slog"
	"time"

	"github.com/icewisb/FairQ"
	"github.com/icewisb/FairQ/internal/base"
	"github.com/icewisb/FairQ/internal/clock"
	"github.com/icewisb/FairQ/internal/rdb"
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
	retention             time.Duration
	logger                *slog.Logger
}

func (m *maintenance) tick(ctx context.Context) (stats tickStats) {
	defer func() {
		m.store.RecordMaintenance(m.kind, stats.Scanned, stats.Converted, stats.Isolated, stats.Failed)
	}()
	for batch := 0; batch < m.maxBatches; batch++ {
		if ctx.Err() != nil {
			return stats
		}
		page, err := m.store.Scan(ctx, m.kind, m.cursor, m.batchSize, m.retention)
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
			} else if m.kind == "retry" {
				err = m.store.Forward(ctx, c)
			} else {
				err = m.store.Clean(ctx, m.kind, c, m.retention)
			}
			switch {
			case err == nil:
				stats.Converted++
			case fairq.IsCode(err, fairq.CorruptReference):
				stats.Isolated++
			case fairq.IsCode(err, fairq.StateConflict) || fairq.IsCode(err, fairq.Empty):
			default:
				stats.Failed++
				if stats.Failed == 1 {
					m.logger.Error("maintenance conversion failed", "operation", m.kind, "task_id", c.ID, "error", err)
				}
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
