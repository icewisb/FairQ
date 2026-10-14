package rdb_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"fairq"
	"fairq/internal/rdb"
	"github.com/redis/go-redis/v9"
)

func TestOwnershipRejectsWrongIndexTypesBeforeWrites(t *testing.T) {
	for _, key := range []string{rdb.Active, rdb.Lease, rdb.Retry, rdb.Archived, rdb.Completed} {
		t.Run(key, func(t *testing.T) {
			s, c := store(t)
			ctx := context.Background()
			enqueue(t, s, "a", "g")
			a, err := s.Dequeue(ctx, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if err := c.Set(ctx, key, "wrong type", 0).Err(); err != nil {
				t.Fatal(err)
			}
			before := dumpDB(t, c)
			if _, err := s.Heartbeat(ctx, []rdb.Attempt{a, a}, time.Hour); !fairq.IsCode(err, fairq.Internal) {
				t.Fatal("heartbeat", err)
			}
			if err := s.Done(ctx, a); !fairq.IsCode(err, fairq.Internal) {
				t.Fatal("done", err)
			}
			if _, err := s.Fail(ctx, a, time.Second, "failure", false); !fairq.IsCode(err, fairq.Internal) {
				t.Fatal("retry", err)
			}
			if !reflect.DeepEqual(before, dumpDB(t, c)) {
				t.Fatal("invalid index type caused partial writes")
			}
		})
	}
}

func TestOwnershipAndServerTime(t *testing.T) {
	s, c := store(t)
	ctx := context.Background()
	enqueue(t, s, "a", "g")
	before := c.Time(ctx).Val()
	a, err := s.Dequeue(ctx, 30*time.Second)
	after := c.Time(ctx).Val()
	if err != nil {
		t.Fatal(err)
	}
	if a.Deadline.Before(before.Truncate(time.Millisecond).Add(30*time.Second)) || a.Deadline.After(after.Add(30*time.Second)) {
		t.Fatal("lease not based on server time")
	}
	old := a
	old.Token = "old"
	if err = s.Done(ctx, old); !fairq.IsCode(err, fairq.StateConflict) {
		t.Fatal(err)
	}
	if _, err = s.Fail(ctx, old, 7*time.Second, "stale attempt", false); !fairq.IsCode(err, fairq.StateConflict) {
		t.Fatal(err)
	}
	r, err := s.Heartbeat(ctx, []rdb.Attempt{old, a}, time.Minute)
	if err != nil || len(r) != 2 || !fairq.IsCode(r[0].Err, fairq.StateConflict) || r[1].Err != nil {
		t.Fatalf("%+v %v", r, err)
	}
	if err = s.Done(ctx, a); err != nil {
		t.Fatal(err)
	}
	if c.Exists(ctx, rdb.TaskKey("a")).Val() != 0 || c.SCard(ctx, rdb.Active).Val() != 0 || c.ZCard(ctx, rdb.Lease).Val() != 0 || c.Exists(ctx, rdb.Completed).Val() != 0 {
		t.Fatal("done indexes")
	}
	if err = s.Done(ctx, a); !fairq.IsCode(err, fairq.StateConflict) {
		t.Fatal(err)
	}
}

func TestExpiredCannotFinishOrRenew(t *testing.T) {
	s, c := store(t)
	ctx := context.Background()
	enqueue(t, s, "a", "g")
	a, err := s.Dequeue(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	now := c.Time(ctx).Val()
	c.ZAdd(ctx, rdb.Lease, redis.Z{Member: a.ID, Score: float64(now.UnixMilli())})
	if err = s.Done(ctx, a); !fairq.IsCode(err, fairq.LeaseExpired) {
		t.Fatal(err)
	}
	if _, err = s.Fail(ctx, a, 7*time.Second, "expired attempt", false); !fairq.IsCode(err, fairq.LeaseExpired) {
		t.Fatal(err)
	}
	r, err := s.Heartbeat(ctx, []rdb.Attempt{a}, time.Minute)
	if err != nil || !fairq.IsCode(r[0].Err, fairq.LeaseExpired) {
		t.Fatalf("%+v %v", r, err)
	}
	if c.HGet(ctx, rdb.TaskKey(a.ID), "state").Val() != "active" || !c.SIsMember(ctx, rdb.Active, a.ID).Val() || c.ZScore(ctx, rdb.Lease, a.ID).Val() != float64(now.UnixMilli()) {
		t.Fatal("expired state changed")
	}
}

func TestMissingIndexAndNewAttemptProtection(t *testing.T) {
	s, c := store(t)
	ctx := context.Background()
	enqueue(t, s, "a", "g")
	a, err := s.Dequeue(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	c.HSet(ctx, rdb.TaskKey(a.ID), "attempt_token", "new-token")
	if err = s.Done(ctx, a); !fairq.IsCode(err, fairq.StateConflict) {
		t.Fatal(err)
	}
	a.Token = "new-token"
	c.SRem(ctx, rdb.Active, a.ID)
	if err = s.Done(ctx, a); !fairq.IsCode(err, fairq.StateConflict) {
		t.Fatal(err)
	}
	if c.Exists(ctx, rdb.TaskKey(a.ID)).Val() != 1 {
		t.Fatal("deleted task without ownership")
	}
}
