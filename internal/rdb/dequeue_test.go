package rdb_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"fairq"
	"fairq/internal/base"
	"fairq/internal/rdb"
	"fairq/internal/testutil"
	"github.com/redis/go-redis/v9"
)

func store(t *testing.T) (*rdb.Store, *redis.Client) {
	t.Helper()
	c := testutil.Redis(t, 2)
	s, err := rdb.New(context.Background(), testutil.Options(t, 2))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, c
}
func enqueue(t *testing.T, s *rdb.Store, id, group string) {
	t.Helper()
	task := fairq.Task{ID: id, Type: "test", Payload: []byte(`{"ok":true}`)}
	opts := fairq.EnqueueOptions{GroupKey: group}
	g, m, err := base.Validate(task, opts, base.MaxPayload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Enqueue(context.Background(), task, opts, g, m); err != nil {
		t.Fatal(err)
	}
}

func TestRoundRobinFIFO(t *testing.T) {
	s, c := store(t)
	ctx := context.Background()
	counts := []int{10000, 1000, 1000}
	for i, n := range counts {
		for j := 0; j < n; j++ {
			enqueue(t, s, fmt.Sprintf("%d-%d", i, j), fmt.Sprint(i))
		}
	}
	seen := make([]int, 3)
	for k := 0; k < 12000; k++ {
		a, err := s.Dequeue(ctx, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		g := 0
		if k < 3000 {
			g = k % 3
		}
		want := fmt.Sprintf("%d-%d", g, seen[g])
		seen[g]++
		if a.ID != want || a.Token == "" {
			t.Fatalf("claim %d got %s want %s", k, a.ID, want)
		}
	}
	if c.LLen(ctx, rdb.Ready).Val() != 0 || c.SCard(ctx, rdb.Members).Val() != 0 || c.SCard(ctx, rdb.Active).Val() != 12000 || c.ZCard(ctx, rdb.Lease).Val() != 12000 {
		t.Fatal("bad final indexes")
	}
	if _, err := s.Dequeue(ctx, time.Minute); !fairq.IsCode(err, fairq.Empty) {
		t.Fatal(err)
	}
	enqueue(t, s, "new", "1")
	a, err := s.Dequeue(ctx, time.Minute)
	if err != nil || a.ID != "new" {
		t.Fatalf("reactivation: %+v %v", a, err)
	}
}

func TestCorruptReference(t *testing.T) {
	s, c := store(t)
	ctx := context.Background()
	g, _ := base.Group("a")
	enqueue(t, s, "valid", "a")
	c.RPush(ctx, rdb.PendingKey(g), "missing")
	if _, err := s.Dequeue(ctx, time.Minute); !fairq.IsCode(err, fairq.CorruptReference) {
		t.Fatal(err)
	}
	if c.LIndex(ctx, rdb.Ready, 0).Val() != g || c.LLen(ctx, rdb.BadRefs).Val() != 1 {
		t.Fatal("lost position/evidence")
	}
	a, err := s.Dequeue(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	before := c.HGetAll(ctx, rdb.TaskKey(a.ID)).Val()
	score := c.ZScore(ctx, rdb.Lease, a.ID).Val()
	c.LPush(ctx, rdb.PendingKey(g), a.ID)
	c.RPush(ctx, rdb.Ready, g)
	c.SAdd(ctx, rdb.Members, g)
	if _, err = s.Dequeue(ctx, time.Minute); !fairq.IsCode(err, fairq.CorruptReference) {
		t.Fatal(err)
	}
	if fmt.Sprint(before) != fmt.Sprint(c.HGetAll(ctx, rdb.TaskKey(a.ID)).Val()) || c.ZScore(ctx, rdb.Lease, a.ID).Val() != score || !c.SIsMember(ctx, rdb.Active, a.ID).Val() {
		t.Fatal("damaged legitimate active")
	}
	if c.LLen(ctx, rdb.Ready).Val() != 0 || c.SCard(ctx, rdb.Members).Val() != 0 {
		t.Fatal("empty group retained")
	}
}

func TestEvidenceFailureAndThreshold(t *testing.T) {
	s, c := store(t)
	ctx := context.Background()
	g, _ := base.Group("a")
	c.RPush(ctx, rdb.Ready, g)
	c.SAdd(ctx, rdb.Members, g)
	c.LPush(ctx, rdb.PendingKey(g), "missing")
	c.Set(ctx, rdb.BadRefs, "wrong", 0)
	if _, err := s.Dequeue(ctx, time.Minute); !fairq.IsCode(err, fairq.Internal) {
		t.Fatal(err)
	}
	if c.LLen(ctx, rdb.PendingKey(g)).Val() != 1 || c.LLen(ctx, rdb.Ready).Val() != 1 {
		t.Fatal("reference removed without evidence")
	}
	c.Del(ctx, rdb.BadRefs)
	values := make([]any, 10000)
	for i := range values {
		values[i] = "old"
	}
	c.RPush(ctx, rdb.BadRefs, values...)
	if _, err := s.Dequeue(ctx, time.Minute); !fairq.IsCode(err, fairq.CorruptReference) {
		t.Fatal(err)
	}
	if c.LLen(ctx, rdb.BadRefs).Val() != 10001 {
		t.Fatal("threshold prevented evidence")
	}
}

// This helper is invoked in separate OS processes by TestMultiProcessClaims.
func TestClaimProcess(t *testing.T) {
	if os.Getenv("FAIRQ_CLAIM_CHILD") != "1" {
		return
	}
	s, err := rdb.New(context.Background(), testutil.Options(t, 2))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var wg sync.WaitGroup
	for i := 0; i < 80; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				a, err := s.Dequeue(context.Background(), time.Minute)
				if fairq.IsCode(err, fairq.Empty) {
					return
				}
				if err != nil {
					t.Errorf("%v: %v", err, errors.Unwrap(err))
					return
				}
				fmt.Println("CLAIM:" + a.ID)
			}
		}()
	}
	wg.Wait()
}

func TestMultiProcessClaims(t *testing.T) {
	s, c := store(t)
	ctx := context.Background()
	for i := 0; i < 1000; i++ {
		enqueue(t, s, fmt.Sprint(i), fmt.Sprint(i%10))
	}
	cmds := make([]*exec.Cmd, 10)
	var wg sync.WaitGroup
	outputs := make([][]byte, 10)
	errs := make([]error, 10)
	for i := range cmds {
		cmds[i] = exec.Command(os.Args[0], "-test.run=^TestClaimProcess$", "-test.timeout=60s")
		cmds[i].Env = append(os.Environ(), "FAIRQ_CLAIM_CHILD=1")
		wg.Add(1)
		go func(i int) { defer wg.Done(); outputs[i], errs[i] = cmds[i].CombinedOutput() }(i)
	}
	wg.Wait()
	seen := map[string]bool{}
	for i, out := range outputs {
		if errs[i] != nil {
			t.Fatalf("child: %v %s", errs[i], out)
		}
		for _, line := range strings.Split(string(out), "\n") {
			if strings.HasPrefix(line, "CLAIM:") {
				id := strings.TrimPrefix(line, "CLAIM:")
				if seen[id] {
					t.Fatalf("duplicate %s", id)
				}
				seen[id] = true
			}
		}
	}
	if len(seen) != 1000 || c.SCard(ctx, rdb.Active).Val() != 1000 || c.ZCard(ctx, rdb.Lease).Val() != 1000 || c.LLen(ctx, rdb.Ready).Val() != 0 {
		t.Fatalf("lost claims: %d", len(seen))
	}
}
