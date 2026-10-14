package inspector

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"fairq"
	"fairq/client"
	"fairq/internal/base"
	"fairq/internal/rdb"
	"fairq/internal/testutil"
	"github.com/redis/go-redis/v9"
)

func snapshot(t *testing.T, c *redis.Client) map[string]string {
	t.Helper()
	ctx := context.Background()
	var cursor uint64
	out := map[string]string{}
	for {
		keys, next, err := c.Scan(ctx, cursor, "*", 100).Result()
		if err != nil {
			t.Fatal(err)
		}
		for _, key := range keys {
			v, err := c.Dump(ctx, key).Result()
			if err != nil {
				t.Fatal(err)
			}
			out[key] = v
		}
		cursor = next
		if cursor == 0 {
			return out
		}
	}
}

func TestInspectorReadOnlyAndCoverage(t *testing.T) {
	c := testutil.Redis(t, 4)
	ctx := context.Background()
	opts := testutil.Options(t, 4)
	producer, err := client.New(ctx, client.Config{Redis: opts})
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	in, err := New(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	for n := 0; n < 205; n++ {
		if _, err = producer.Enqueue(ctx, fairq.Task{ID: fmt.Sprint(n), Type: "test"}, fairq.EnqueueOptions{GroupKey: "g"}); err != nil {
			t.Fatal(err)
		}
	}
	before := snapshot(t, c)
	report, err := in.Check(ctx)
	if err != nil || !report.Complete || len(report.Issues) != 0 || report.CheckedTasks != 205 || report.CheckedPending != 205 {
		t.Fatal(report, err)
	}
	if _, err = in.Task(ctx, "0"); err != nil {
		t.Fatal(err)
	}
	if ids, err := in.Group(ctx, "g", 0, 100); err != nil || len(ids) != 100 {
		t.Fatal(ids, err)
	}
	if !reflect.DeepEqual(before, snapshot(t, c)) {
		t.Fatal("inspector modified keys")
	}
	g, _ := base.Group("g")
	c.RPush(ctx, rdb.Ready, g)
	c.SRem(ctx, rdb.Members, g)
	c.RPush(ctx, rdb.PendingKey(g), "missing")
	c.ZAdd(ctx, rdb.Lease, redis.Z{Member: "orphan", Score: 0})
	c.HSet(ctx, rdb.TaskKey("0"), "state", "active", "attempt_token", "private-token")
	c.RPush(ctx, rdb.BadRefs, `{"reason":"example"}`)
	before = snapshot(t, c)
	report, err = in.Check(ctx)
	if err != nil {
		t.Fatal(err)
	}
	kinds := map[string]bool{}
	for _, issue := range report.Issues {
		kinds[issue.Kind] = true
	}
	for _, kind := range []string{"READY_DUPLICATE", "READY_MEMBERS", "UNREACHABLE_PENDING", "MISSING_HASH", "ACTIVE_LEASE", "MAIN_STATE", "BAD_REFS"} {
		if !kinds[kind] {
			t.Fatal("not detected", kind, report)
		}
	}
	if !reflect.DeepEqual(before, snapshot(t, c)) {
		t.Fatal("inspection repaired data")
	}
	ctx, cancel := context.WithTimeout(ctx, time.Nanosecond)
	defer cancel()
	time.Sleep(time.Millisecond)
	if _, err = in.Check(ctx); err == nil {
		t.Fatal("ignored context")
	}
}
