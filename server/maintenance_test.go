package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"fairq"
	"fairq/internal/base"
	"fairq/internal/clock"
	"fairq/internal/rdb"
	"fairq/internal/testutil"
	"github.com/redis/go-redis/v9"
)

func maint(s *rdb.Store, kind string) *maintenance {
	return &maintenance{store: s, kind: kind, clock: clock.Real{}, random: func() float64 { return 0 }, batchSize: 100, maxBatches: 5, pause: 10 * time.Millisecond, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
}
func seed(t *testing.T, s *rdb.Store, id string) {
	t.Helper()
	task := fairq.Task{ID: id, Type: "test"}
	opts := fairq.EnqueueOptions{GroupKey: "same"}
	g, msg, _ := base.Validate(task, opts, base.MaxPayload)
	if _, err := s.Enqueue(context.Background(), task, opts, g, msg); err != nil {
		t.Fatal(err)
	}
}

func TestMaintenanceChild(t *testing.T) {
	kind := os.Getenv("FAIRQ_MAINT_CHILD")
	if kind == "" {
		return
	}
	s, err := rdb.New(context.Background(), testutil.Options(t, 3))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	m := maint(s, kind)
	if kind == "retry" {
		seed(t, s, "new-"+fmt.Sprint(os.Getpid()))
	}
	var all tickStats
	for i := 0; i < 10; i++ {
		r := m.tick(context.Background())
		if r.Scanned > 500 {
			t.Fatal("unbounded tick")
		}
		all.Scanned += r.Scanned
		all.Converted += r.Converted
		all.Failed += r.Failed
	}
	if all.Failed != 0 {
		t.Fatal("maintenance failed", all)
	}
	fmt.Printf("STATS:%d/%d\n", all.Scanned, all.Converted)
}

func children(t *testing.T, kind string) {
	t.Helper()
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cmd := exec.Command(os.Args[0], "-test.run=^TestMaintenanceChild$", "-test.timeout=60s")
			cmd.Env = append(os.Environ(), "FAIRQ_MAINT_CHILD="+kind)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Errorf("child %v %s", err, out)
			} else {
				for _, line := range strings.Split(string(out), "\n") {
					if strings.HasPrefix(line, "STATS:") {
						t.Log(kind, line)
					}
				}
			}
		}()
	}
	wg.Wait()
}

func TestTenProcessMaintenance(t *testing.T) {
	c := testutil.Redis(t, 3)
	ctx := context.Background()
	s, err := rdb.New(ctx, testutil.Options(t, 3))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 1000; i++ {
		seed(t, s, fmt.Sprint(i))
		a, err := s.Dequeue(ctx, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		c.ZAdd(ctx, rdb.Lease, redis.Z{Member: a.ID, Score: 0})
	}
	children(t, "lease")
	if c.ZCard(ctx, rdb.Retry).Val() != 1000 || c.SCard(ctx, rdb.Active).Val() != 0 || c.ZCard(ctx, rdb.Lease).Val() != 0 {
		t.Fatal("recover totals")
	}
	for i := 0; i < 1000; i++ {
		id := fmt.Sprint(i)
		if c.HGet(ctx, rdb.TaskKey(id), "retry_count").Val() != "1" {
			t.Fatal("duplicate recover", id)
		}
		c.ZAdd(ctx, rdb.Retry, redis.Z{Member: id, Score: 0})
	}
	children(t, "retry")
	g, _ := base.Group("same")
	if c.ZCard(ctx, rdb.Retry).Val() != 0 || c.LLen(ctx, rdb.Ready).Val() != 1 || c.SCard(ctx, rdb.Members).Val() != 1 || c.LLen(ctx, rdb.PendingKey(g)).Val() != 1010 {
		t.Fatal("forward totals/group activation")
	}
	ids := c.LRange(ctx, rdb.PendingKey(g), 0, -1).Val()
	seen := map[string]bool{}
	for _, id := range ids {
		if seen[id] {
			t.Fatal("duplicate forward", id)
		}
		seen[id] = true
	}
}

func TestBlockedHeadDoesNotStarveRecovery(t *testing.T) {
	c := testutil.Redis(t, 3)
	ctx := context.Background()
	s, err := rdb.New(ctx, testutil.Options(t, 3))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 600; i++ {
		id := fmt.Sprintf("bad-%04d", i)
		c.SAdd(ctx, rdb.Active, id)
		c.ZAdd(ctx, rdb.Lease, redis.Z{Member: id, Score: 0})
	}
	seed(t, s, "valid")
	a, err := s.Dequeue(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	c.ZAdd(ctx, rdb.Lease, redis.Z{Member: a.ID, Score: 1})
	m := maint(s, "lease")
	for i := 0; i < 3; i++ {
		r := m.tick(ctx)
		if r.Scanned > 500 {
			t.Fatal("batch bound", r)
		}
	}
	if c.HGet(ctx, rdb.TaskKey(a.ID), "state").Val() != "retry" || c.SCard(ctx, rdb.Active).Val() != 600 {
		t.Fatal("starved recovery or damaged unknown owners")
	}
}
