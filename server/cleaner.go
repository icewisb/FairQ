package server

import (
	"context"
	"time"

	"github.com/icewisb/FairQ/internal/rdb"
)

func (s *Server) cleaner(ctx context.Context) {
	arch := &maintenance{store: s.store, kind: "archived", clock: s.clock, random: s.random, batchSize: s.cfg.BatchSize, maxBatches: s.cfg.MaxBatchesPerTick, pause: s.cfg.BatchPause, retention: s.cfg.ArchivedRetention, logger: s.cfg.Logger}
	completed := *arch
	completed.kind = "completed"
	if !s.wait(ctx, time.Duration(s.random()*float64(s.cfg.RetentionCleanupInterval))) {
		return
	}
	first, second := arch, &completed
	for {
		first.maxBatches = s.cfg.MaxBatchesPerTick
		stats := first.tick(ctx)
		second.maxBatches = (s.cfg.BatchSize*s.cfg.MaxBatchesPerTick - stats.Scanned) / s.cfg.BatchSize
		if second.maxBatches > 0 {
			second.tick(ctx)
		}
		first, second = second, first
		for name, key := range map[string]string{"archived": rdb.Archived, "bad_refs": rdb.BadRefs} {
			var n int64
			var err error
			if name == "archived" {
				n, err = s.store.Redis.ZCard(ctx, key).Result()
			} else {
				n, err = s.store.Redis.LLen(ctx, key).Result()
			}
			if err != nil {
				s.logError("capacity", err)
			} else if n >= 10000 {
				s.cfg.Logger.Warn("queue evidence/retention threshold reached", "index", name, "count", n)
			}
		}
		if !s.wait(ctx, s.cfg.RetentionCleanupInterval) {
			return
		}
	}
}
