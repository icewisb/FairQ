package rdb_test

import (
	"context"
	"testing"
	"time"

	"fairq"
	"fairq/internal/base"
	"fairq/internal/rdb"
	"github.com/redis/go-redis/v9"
)

func TestRetentionAndSafeCleaner(t *testing.T) {
	s, c := store(t)
	ctx := context.Background()
	task := fairq.Task{ID: "a", Type: "test"}
	opts := fairq.EnqueueOptions{GroupKey: "g", Retention: time.Hour}
	g, msg, _ := base.Validate(task, opts, base.MaxPayload)
	if _, err := s.Enqueue(ctx, task, opts, g, msg); err != nil {
		t.Fatal(err)
	}
	a, err := s.Dequeue(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Done(ctx, a); err != nil {
		t.Fatal(err)
	}
	info, err := s.GetTask(ctx, a.ID)
	if err != nil || info.State != "completed" {
		t.Fatal(info, err)
	}
	page, err := s.Scan(ctx, "completed", 0, 100, 0)
	if err != nil || len(page.Candidates) != 0 {
		t.Fatal(page, err)
	}
	c.ZAdd(ctx, rdb.Completed, redis.Z{Member: a.ID, Score: 0})
	page, _ = s.Scan(ctx, "completed", 0, 100, 0)
	candidate := page.Candidates[0]
	c.HSet(ctx, rdb.TaskKey(a.ID), "state", "active", "attempt_token", "new")
	c.SAdd(ctx, rdb.Active, a.ID)
	c.ZAdd(ctx, rdb.Lease, redis.Z{Member: a.ID, Score: float64(time.Now().Add(time.Hour).UnixMilli())})
	if err = s.Clean(ctx, "completed", candidate, 0); !fairq.IsCode(err, fairq.StateConflict) {
		t.Fatal(err)
	}
	if c.Exists(ctx, rdb.TaskKey(a.ID)).Val() != 1 || !c.SIsMember(ctx, rdb.Active, a.ID).Val() {
		t.Fatal("cleaned legitimate owner")
	}
	c.SRem(ctx, rdb.Active, a.ID)
	c.ZRem(ctx, rdb.Lease, a.ID)
	c.HSet(ctx, rdb.TaskKey(a.ID), "state", "completed", "attempt_token", a.Token)
	if err = s.Clean(ctx, "completed", candidate, 0); err != nil {
		t.Fatal(err)
	}
	if c.Exists(ctx, rdb.TaskKey(a.ID)).Val() != 0 || c.ZCard(ctx, rdb.Completed).Val() != 0 {
		t.Fatal("retention not cleaned")
	}
}

func TestArchiveThresholdDoesNotTrim(t *testing.T) {
	s, c := store(t)
	ctx := context.Background()
	entries := make([]redis.Z, 10000)
	for i := range entries {
		entries[i] = redis.Z{Member: i, Score: 1}
	}
	if err := c.ZAdd(ctx, rdb.Archived, entries...).Err(); err != nil {
		t.Fatal(err)
	}
	enqueue(t, s, "latest", "g")
	a, err := s.Dequeue(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Fail(ctx, a, 0, "", true); err != nil {
		t.Fatal(err)
	}
	if c.ZCard(ctx, rdb.Archived).Val() != 10001 {
		t.Fatal("archive threshold trimmed/refused tasks")
	}
}
