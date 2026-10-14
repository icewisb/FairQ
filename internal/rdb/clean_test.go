package rdb_test

import (
	"context"
	"testing"
	"time"

	"github.com/icewisb/FairQ"
	"github.com/icewisb/FairQ/internal/base"
	"github.com/icewisb/FairQ/internal/rdb"
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

func TestArchivedNinetyDayRetention(t *testing.T) {
	s, c := store(t)
	ctx := context.Background()
	now, err := c.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"young", "due"} {
		enqueue(t, s, id, "g")
		a, e := s.Dequeue(ctx, time.Minute)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = s.Fail(ctx, a, 0, "", true); e != nil {
			t.Fatal(e)
		}
		age := 89 * 24 * time.Hour
		if id == "due" {
			age = 91 * 24 * time.Hour
		}
		if e = c.ZAdd(ctx, rdb.Archived, redis.Z{Member: id, Score: float64(now.Add(-age).UnixMilli())}).Err(); e != nil {
			t.Fatal(e)
		}
	}
	page, err := s.Scan(ctx, "archived", 0, 100, 90*24*time.Hour)
	if err != nil || len(page.Candidates) != 1 || page.Candidates[0].ID != "due" {
		t.Fatal(page, err)
	}
	if err = s.Clean(ctx, "archived", page.Candidates[0], 90*24*time.Hour); err != nil {
		t.Fatal(err)
	}
	if c.Exists(ctx, rdb.TaskKey("due")).Val() != 0 || c.Exists(ctx, rdb.TaskKey("young")).Val() != 1 || c.ZCard(ctx, rdb.Archived).Val() != 1 {
		t.Fatal("90-day retention violated")
	}
}
