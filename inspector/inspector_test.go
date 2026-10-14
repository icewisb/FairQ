package inspector

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/icewisb/FairQ"
	"github.com/icewisb/FairQ/client"
	"github.com/icewisb/FairQ/internal/base"
	"github.com/icewisb/FairQ/internal/rdb"
	"github.com/icewisb/FairQ/internal/testutil"
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

func TestCheckInvalidIndexScores(t *testing.T) {
	c := testutil.Redis(t, 4)
	ctx := context.Background()
	in, err := New(ctx, testutil.Options(t, 4))
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	group, _ := base.Group("g")
	for _, name := range []string{"lease", "retry", "archived", "completed"} {
		t.Run(name, func(t *testing.T) {
			state := name
			if name == "lease" {
				state = "active"
			}
			key := rdb.Prefix + name
			if err := c.HSet(ctx, rdb.TaskKey(name), "state", state, "group", group,
				"msg", `{"type":"test","payload":""}`, "attempt_token", "private-token",
				"retry_count", 0, "max_retry", 25, "timeout_ms", 0, "retention_ms", 0,
				"enqueued_at", 0).Err(); err != nil {
				t.Fatal(err)
			}
			if state == "active" {
				if err := c.SAdd(ctx, rdb.Active, name).Err(); err != nil {
					t.Fatal(err)
				}
			}
			for _, score := range []float64{math.Inf(1), math.Inf(-1), -1, 0.5, 1 << 53, 0, 1<<53 - 1} {
				t.Run(fmt.Sprint(score), func(t *testing.T) {
					if err := c.ZAdd(ctx, key, redis.Z{Member: name, Score: score}).Err(); err != nil {
						t.Fatal(err)
					}
					before := snapshot(t, c)
					report, err := in.Check(ctx)
					if err != nil || !report.Complete {
						t.Fatal(report, err)
					}
					want := 1
					if score == 0 || score == 1<<53-1 {
						want = 0
					}
					if len(report.Issues) != want || (want == 1 && (report.Issues[0].Kind != "SCORE" || report.Issues[0].Key != key || report.Issues[0].TaskID != name)) {
						t.Fatalf("score %v: %+v", score, report)
					}
					if !reflect.DeepEqual(before, snapshot(t, c)) {
						t.Fatal("inspection modified data")
					}
				})
			}
		})
	}
}

func TestCheckInvalidNumericFields(t *testing.T) {
	c := testutil.Redis(t, 4)
	ctx := context.Background()
	opts := testutil.Options(t, 4)
	producer, err := client.New(ctx, client.Config{Redis: opts})
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	if _, err := producer.Enqueue(ctx, fairq.Task{ID: "numeric", Type: "test"}, fairq.EnqueueOptions{GroupKey: "g"}); err != nil {
		t.Fatal(err)
	}
	in, err := New(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	for _, field := range []string{"retry_count", "max_retry", "timeout_ms", "retention_ms", "enqueued_at", "pending_since"} {
		t.Run(field, func(t *testing.T) {
			maximum := "9007199254740991"
			tooLarge := "9007199254740992"
			if field == "timeout_ms" || field == "retention_ms" {
				maximum = strconv.FormatInt(int64((1<<63-1)/time.Millisecond), 10)
				tooLarge = strconv.FormatInt(int64((1<<63-1)/time.Millisecond)+1, 10)
			}
			for _, value := range []string{"-1", tooLarge, "0.5", "invalid", "0", maximum} {
				if err := c.HSet(ctx, rdb.TaskKey("numeric"), field, value).Err(); err != nil {
					t.Fatal(err)
				}
				before := snapshot(t, c)
				report, err := in.Check(ctx)
				if err != nil || !report.Complete {
					t.Fatal(report, err)
				}
				want := 1
				if value == "0" || value == maximum {
					want = 0
				}
				if len(report.Issues) != want || (want == 1 && (report.Issues[0].Kind != "FIELD" || report.Issues[0].Detail != field)) {
					t.Fatalf("%s=%s: %+v", field, value, report)
				}
				if !reflect.DeepEqual(before, snapshot(t, c)) {
					t.Fatal("inspection modified data")
				}
			}
		})
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
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = in.Check(ctx); err == nil {
		t.Fatal("ignored context")
	}
}
