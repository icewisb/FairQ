package rdb_test

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/icewisb/FairQ"
	"github.com/icewisb/FairQ/internal/base"
	"github.com/icewisb/FairQ/internal/rdb"
	"github.com/redis/go-redis/v9"
)

func TestFailureUsesCallerDelay(t *testing.T) {
	for _, op := range []string{"retry", "recover"} {
		t.Run(op, func(t *testing.T) {
			s, c := store(t)
			ctx := context.Background()
			enqueue(t, s, "a", "g")
			a, err := s.Dequeue(ctx, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			// Deliberately outside the default backoff range: Lua must not own that policy.
			delay := 7 * time.Second
			var tr rdb.Transition
			if op == "recover" {
				if err := c.ZAdd(ctx, rdb.Lease, redis.Z{Member: a.ID, Score: 0}).Err(); err != nil {
					t.Fatal(err)
				}
			}
			before := c.Time(ctx).Val().UnixMilli()
			if op == "retry" {
				tr, err = s.Fail(ctx, a, delay, "failure", false)
			} else {
				tr, err = s.Recover(ctx, rdb.Candidate{ID: a.ID, Token: a.Token, RetryCount: a.RetryCount, Score: "0"}, delay)
			}
			after := c.Time(ctx).Val().UnixMilli()
			if err != nil || tr.State != "retry" || tr.RetryCount != 1 || tr.At.UnixMilli() < before+delay.Milliseconds() || tr.At.UnixMilli() > after+delay.Milliseconds() {
				t.Fatal(tr, err)
			}
			if c.SIsMember(ctx, rdb.Active, a.ID).Val() || c.ZScore(ctx, rdb.Lease, a.ID).Err() != redis.Nil || c.HGet(ctx, rdb.TaskKey(a.ID), "state").Val() != "retry" || c.ZScore(ctx, rdb.Retry, a.ID).Val() != float64(tr.At.UnixMilli()) {
				t.Fatal("inconsistent retry transition")
			}
		})
	}
}

func TestFailureRejectsInvalidDelayWithoutWrites(t *testing.T) {
	common, err := os.ReadFile("lua/common.lua")
	if err != nil {
		t.Fatal(err)
	}
	keys := []string{rdb.Ready, rdb.Members, rdb.Active, rdb.Lease, rdb.Retry, rdb.Archived, rdb.Completed, rdb.BadRefs}
	for _, op := range []string{"retry", "recover"} {
		body, err := os.ReadFile("lua/" + op + ".lua")
		if err != nil {
			t.Fatal(err)
		}
		for _, delay := range []string{"-1", "0.5", "nan", "inf", "9007199254740992", "9007199254740991"} {
			t.Run(op+"/"+delay, func(t *testing.T) {
				s, c := store(t)
				ctx := context.Background()
				enqueue(t, s, "a", "g")
				a, err := s.Dequeue(ctx, time.Minute)
				if err != nil {
					t.Fatal(err)
				}
				args := []any{a.ID, a.Token, a.RetryCount, delay, "failure", "error"}
				if op == "recover" {
					if err := c.ZAdd(ctx, rdb.Lease, redis.Z{Member: a.ID, Score: 0}).Err(); err != nil {
						t.Fatal(err)
					}
					args = []any{a.ID, a.Token, a.RetryCount, "0", delay}
				}
				before, err := c.HGetAll(ctx, rdb.TaskKey(a.ID)).Result()
				if err != nil {
					t.Fatal(err)
				}
				lease, err := c.ZScore(ctx, rdb.Lease, a.ID).Result()
				if err != nil {
					t.Fatal(err)
				}
				// Raw arguments exercise Lua's boundary, bypassing Go's duration validation.
				reply, err := c.Eval(ctx, string(common)+"\n"+string(body), keys, args...).Slice()
				if err != nil || len(reply) != 1 || reply[0] != "INTERNAL" {
					t.Fatal(reply, err)
				}
				if !reflect.DeepEqual(before, c.HGetAll(ctx, rdb.TaskKey(a.ID)).Val()) || !c.SIsMember(ctx, rdb.Active, a.ID).Val() || c.ZScore(ctx, rdb.Lease, a.ID).Val() != lease || c.ZCard(ctx, rdb.Retry).Val() != 0 || c.ZCard(ctx, rdb.Archived).Val() != 0 {
					t.Fatal("invalid delay changed task state or indexes")
				}
			})
		}
	}
}

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
