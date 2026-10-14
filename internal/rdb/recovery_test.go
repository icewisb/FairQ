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

func TestRetryCountsAndForwardFIFO(t *testing.T) {
	s, c := store(t)
	ctx := context.Background()
	enqueue(t, s, "a", "g")
	for k := 1; k <= 26; k++ {
		a, err := s.Dequeue(ctx, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		delay, _ := base.Backoff(k, func() float64 { return 0 })
		before := c.Time(ctx).Val()
		tr, err := s.Fail(ctx, a, delay, "handler error", false)
		if err != nil {
			t.Fatal(err)
		}
		if tr.RetryCount != k {
			t.Fatal(tr)
		}
		if k == 26 {
			if tr.State != "archived" || c.HGet(ctx, rdb.TaskKey(a.ID), "retry_count").Val() != "26" {
				t.Fatal(tr)
			}
			break
		}
		if tr.State != "retry" || tr.At.Before(before.Truncate(time.Millisecond).Add(delay)) {
			t.Fatal(tr)
		}
		if _, err = s.Fail(ctx, a, delay, "again", false); !fairq.IsCode(err, fairq.StateConflict) {
			t.Fatal("repeated failure", err)
		}
		c.ZAdd(ctx, rdb.Retry, redis.Z{Member: a.ID, Score: 0})
		page, err := s.Scan(ctx, "retry", 0, 100, 0)
		if err != nil || len(page.Candidates) != 1 {
			t.Fatal(page, err)
		}
		if err = s.Forward(ctx, page.Candidates[0]); err != nil {
			t.Fatal(err)
		}
		if err = s.Forward(ctx, page.Candidates[0]); !fairq.IsCode(err, fairq.StateConflict) {
			t.Fatal(err)
		}
	}
	enqueue(t, s, "r", "g")
	a, err := s.Dequeue(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Fail(ctx, a, 15*time.Second, "error", false); err != nil {
		t.Fatal(err)
	}
	enqueue(t, s, "existing", "g")
	c.ZAdd(ctx, rdb.Retry, redis.Z{Member: a.ID, Score: 0})
	page, _ := s.Scan(ctx, "retry", 0, 100, 0)
	if err = s.Forward(ctx, page.Candidates[0]); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"existing", "r"} {
		a, err = s.Dequeue(ctx, time.Minute)
		if err != nil || a.ID != want {
			t.Fatal(a, err)
		}
	}
}

func TestRecoverChecksSnapshotAndRecoveryTime(t *testing.T) {
	s, c := store(t)
	ctx := context.Background()
	enqueue(t, s, "a", "g")
	a, err := s.Dequeue(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	now := c.Time(ctx).Val()
	expired := now.Add(-10 * time.Second).UnixMilli()
	c.ZAdd(ctx, rdb.Lease, redis.Z{Member: a.ID, Score: float64(expired)})
	p, err := s.Scan(ctx, "lease", 0, 100, 0)
	if err != nil || len(p.Candidates) != 1 {
		t.Fatal(p, err)
	}
	old := p.Candidates[0]
	old.Token = "stale"
	if _, err = s.Recover(ctx, old, 15*time.Second); !fairq.IsCode(err, fairq.StateConflict) {
		t.Fatal(err)
	}
	before := c.Time(ctx).Val()
	tr, err := s.Recover(ctx, p.Candidates[0], 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if tr.RetryCount != 1 || tr.State != "retry" || tr.At.Before(before.Truncate(time.Millisecond).Add(15*time.Second)) || tr.At.UnixMilli() < expired+25000 {
		t.Fatal(tr)
	}
	if _, err = s.Recover(ctx, p.Candidates[0], 15*time.Second); !fairq.IsCode(err, fairq.StateConflict) {
		t.Fatal(err)
	}
	if err = s.Done(ctx, a); !fairq.IsCode(err, fairq.StateConflict) {
		t.Fatal(err)
	}
}

func TestOrphanLeaseAndDecodeArchive(t *testing.T) {
	s, c := store(t)
	ctx := context.Background()
	c.ZAdd(ctx, rdb.Lease, redis.Z{Member: "orphan", Score: 0})
	p, _ := s.Scan(ctx, "lease", 0, 100, 0)
	if _, err := s.Recover(ctx, p.Candidates[0], 15*time.Second); !fairq.IsCode(err, fairq.CorruptReference) {
		t.Fatal(err)
	}
	if c.ZCard(ctx, rdb.Lease).Val() != 0 || c.LLen(ctx, rdb.BadRefs).Val() != 1 {
		t.Fatal("orphan not isolated")
	}
	c.ZAdd(ctx, rdb.Lease, redis.Z{Member: "unknown", Score: 0})
	c.SAdd(ctx, rdb.Active, "unknown")
	p, _ = s.Scan(ctx, "lease", 0, 100, 0)
	if _, err := s.Recover(ctx, p.Candidates[0], 15*time.Second); !fairq.IsCode(err, fairq.Internal) {
		t.Fatal(err)
	}
	if !c.SIsMember(ctx, rdb.Active, "unknown").Val() || c.ZCard(ctx, rdb.Lease).Val() != 1 {
		t.Fatal("unknown owner removed")
	}
	enqueue(t, s, "badmsg", "g")
	c.HSet(ctx, rdb.TaskKey("badmsg"), "msg", "broken{")
	a, err := s.Dequeue(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	tr, err := s.Fail(ctx, a, 0, "", true)
	if err != nil || tr.State != "archived" || tr.RetryCount != 0 {
		t.Fatal(tr, err)
	}
	if c.HGet(ctx, rdb.TaskKey(a.ID), "msg").Val() != "broken{" || c.HGet(ctx, rdb.TaskKey(a.ID), "error").Val() != "DECODE_FAILED" {
		t.Fatal("message evidence destroyed")
	}
}
