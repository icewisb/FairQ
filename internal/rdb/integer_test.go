package rdb_test

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"fairq"
	"fairq/internal/base"
	"fairq/internal/rdb"
	"github.com/redis/go-redis/v9"
)

func TestLargeRetryIntegersRoundTrip(t *testing.T) {
	for _, recoverLease := range []bool{false, true} {
		t.Run(fmt.Sprintf("recover=%v", recoverLease), func(t *testing.T) {
			s, c := store(t)
			ctx := context.Background()
			task := fairq.Task{ID: "large-integers", Type: "test"}
			limit := min(int64(base.MaxExactInteger-1), int64(math.MaxInt)-1)
			opts := fairq.EnqueueOptions{GroupKey: "g", MaxRetry: int(limit)}
			group, msg, err := base.Validate(task, opts, base.MaxPayload)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = s.Enqueue(ctx, task, opts, group, msg); err != nil {
				t.Fatal(err)
			}
			// Reach the boundary without executing quadrillions of retries.
			if err = c.HSet(ctx, rdb.TaskKey(task.ID), "retry_count", opts.MaxRetry-1).Err(); err != nil {
				t.Fatal(err)
			}
			for _, state := range []string{"retry", "archived"} {
				a, err := s.Dequeue(ctx, time.Minute)
				if err != nil || a.MaxRetry != opts.MaxRetry {
					t.Fatalf("claim: %+v, error: %v", a, err)
				}
				var tr rdb.Transition
				if recoverLease {
					if err = c.ZAdd(ctx, rdb.Lease, redis.Z{Member: a.ID, Score: 0}).Err(); err != nil {
						t.Fatal(err)
					}
					page, e := s.Scan(ctx, "lease", 0, 100, 0)
					if e != nil || len(page.Candidates) != 1 {
						t.Fatal(page, e)
					}
					tr, err = s.Recover(ctx, page.Candidates[0], time.Hour)
				} else {
					tr, err = s.Fail(ctx, a, time.Hour, "test", false)
				}
				want := opts.MaxRetry
				if state == "archived" {
					want++
				}
				if err != nil || tr.State != state || tr.RetryCount != want || a.RetryCount != want-1 {
					t.Fatal(a, tr, err)
				}
				info, err := s.GetTask(ctx, a.ID)
				if err != nil || info.RetryCount != want || info.State != state || c.ZCard(ctx, rdb.Prefix+state).Val() != 1 || c.SCard(ctx, rdb.Active).Val() != 0 || c.ZCard(ctx, rdb.Lease).Val() != 0 {
					t.Fatal("transition did not preserve task/indexes", info, err)
				}
				if state == "retry" {
					if err = c.ZAdd(ctx, rdb.Retry, redis.Z{Member: a.ID, Score: 0}).Err(); err != nil {
						t.Fatal(err)
					}
					page, e := s.Scan(ctx, "retry", 0, 100, 0)
					if e != nil || len(page.Candidates) != 1 {
						t.Fatal(page, e)
					}
					if err = s.Forward(ctx, page.Candidates[0]); err != nil {
						t.Fatal(err)
					}
				}
			}
		})
	}
}
