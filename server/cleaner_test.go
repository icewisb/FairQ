package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/icewisb/FairQ"
	"github.com/icewisb/FairQ/client"
	"github.com/icewisb/FairQ/internal/rdb"
	"github.com/redis/go-redis/v9"
)

func TestRetentionThroughPublicClientAndCleaner(t *testing.T) {
	cfg := config(t)
	cfg.RetentionCleanupInterval = 10 * time.Millisecond
	s, c := running(t, cfg)
	ctx := context.Background()
	producer, err := client.New(ctx, client.Config{Redis: cfg.Redis})
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	if _, err = producer.Enqueue(ctx, fairq.Task{ID: "retained", Type: "test"}, fairq.EnqueueOptions{GroupKey: "g", Retention: 200 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	if err = s.Start(func(context.Context, fairq.Task) error { return nil }); err != nil {
		t.Fatal(err)
	}
	eventually(t, time.Second, func() bool {
		info, e := producer.GetTask(ctx, "retained")
		return e == nil && info.State == "completed"
	})
	eventually(t, time.Second, func() bool {
		return c.Exists(ctx, rdb.TaskKey("retained")).Val() == 0 && c.ZCard(ctx, rdb.Completed).Val() == 0
	})
}

func TestCleanerDoesNotStarveCompletedBehindArchived(t *testing.T) {
	cfg := config(t)
	cfg.RetentionCleanupInterval = 10 * time.Millisecond
	cfg.BatchSize = 1
	cfg.MaxBatchesPerTick = 1
	s, c := running(t, cfg)
	ctx := context.Background()
	// Unrepairable archived entries consume every first-priority budget.
	for i := 0; i < 20; i++ {
		c.ZAdd(ctx, rdb.Archived, redis.Z{Member: fmt.Sprint(i), Score: 0})
	}
	seed(t, s.store, "completed")
	c.HSet(ctx, rdb.TaskKey("completed"), "retention_ms", 1)
	a, err := s.store.Dequeue(ctx, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.store.Done(ctx, a); err != nil {
		t.Fatal(err)
	}
	if err = s.Start(func(context.Context, fairq.Task) error { return nil }); err != nil {
		t.Fatal(err)
	}
	eventually(t, time.Second, func() bool { return c.Exists(ctx, rdb.TaskKey("completed")).Val() == 0 })
	if c.ZCard(ctx, rdb.Archived).Val() != 20 {
		t.Fatal("ambiguous archived evidence deleted")
	}
}
