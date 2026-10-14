package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fairq"
	"fairq/internal/base"
	"fairq/internal/rdb"
	"fairq/internal/testutil"
	"github.com/redis/go-redis/v9"
)

func config(t *testing.T) Config {
	return Config{Redis: testutil.Options(t, 3), Concurrency: 2, LeaseDuration: 300 * time.Millisecond, HeartbeatInterval: 50 * time.Millisecond, RecoverInterval: 20 * time.Millisecond, ForwarderInterval: 20 * time.Millisecond, PollInterval: 5 * time.Millisecond, ShutdownTimeout: time.Second, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
}
func running(t *testing.T, cfg Config) (*Server, *redis.Client) {
	t.Helper()
	c := testutil.Redis(t, 3)
	s, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Shutdown)
	return s, c
}
func eventually(t *testing.T, d time.Duration, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition did not become true")
}

func TestWorkerLifecycle(t *testing.T) {
	s, c := running(t, config(t))
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		seed(t, s.store, fmt.Sprint(i))
	}
	var active, max atomic.Int32
	if err := s.Start(func(ctx context.Context, task fairq.Task) error {
		n := active.Add(1)
		defer active.Add(-1)
		for old := max.Load(); n > old && !max.CompareAndSwap(old, n); old = max.Load() {
		}
		time.Sleep(5 * time.Millisecond)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	eventually(t, time.Second, func() bool { return c.Exists(ctx, rdb.TaskKey("19")).Val() == 0 && c.SCard(ctx, rdb.Active).Val() == 0 })
	if max.Load() > 2 || max.Load() == 0 {
		t.Fatal("concurrency", max.Load())
	}
	s.Shutdown()
	if err := s.Start(func(context.Context, fairq.Task) error { return nil }); err == nil {
		t.Fatal("restarted closed server")
	}
}

func TestFailureTimeoutPanicAndDecode(t *testing.T) {
	s, c := running(t, config(t))
	ctx := context.Background()
	for _, id := range []string{"error", "timeout", "panic", "decode"} {
		seed(t, s.store, id)
	}
	c.HSet(ctx, rdb.TaskKey("timeout"), "timeout_ms", 20)
	c.HSet(ctx, rdb.TaskKey("decode"), "msg", "invalid{")
	if err := s.Start(func(ctx context.Context, task fairq.Task) error {
		switch task.ID {
		case "error":
			return errors.New("test failure")
		case "timeout":
			<-ctx.Done()
			return ctx.Err()
		case "panic":
			panic("private payload must not appear in error")
		}
		t.Error("decode task reached handler")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	eventually(t, time.Second, func() bool { return c.ZCard(ctx, rdb.Retry).Val() == 3 && c.ZCard(ctx, rdb.Archived).Val() == 1 })
	for _, id := range []string{"error", "timeout", "panic"} {
		if c.HGet(ctx, rdb.TaskKey(id), "retry_count").Val() != "1" {
			t.Fatal(id)
		}
	}
	if c.HGet(ctx, rdb.TaskKey("panic"), "error").Val() != "handler panic" || c.HGet(ctx, rdb.TaskKey("decode"), "msg").Val() != "invalid{" {
		t.Fatal("panic/payload evidence")
	}
}

func TestShutdownKeepsHeartbeats(t *testing.T) {
	cfg := config(t)
	cfg.Concurrency = 1
	s, c := running(t, cfg)
	ctx := context.Background()
	seed(t, s.store, "a")
	seed(t, s.store, "next")
	started := make(chan struct{})
	release := make(chan struct{})
	releaseHandler := sync.OnceFunc(func() { close(release) })
	defer releaseHandler()
	if err := s.Start(func(context.Context, fairq.Task) error { close(started); <-release; return nil }); err != nil {
		t.Fatal(err)
	}
	<-started
	done := make(chan struct{})
	go func() { s.Shutdown(); close(done) }()
	eventually(t, time.Second, func() bool { s.mu.Lock(); defer s.mu.Unlock(); return s.state == 2 })
	initial := c.ZScore(ctx, rdb.Lease, "a").Val()
	eventually(t, time.Second, func() bool {
		return float64(c.Time(ctx).Val().UnixMilli()) > initial && c.ZScore(ctx, rdb.Lease, "a").Val() > initial
	})
	if c.ZScore(ctx, rdb.Lease, "a").Val() <= initial || c.HGet(ctx, rdb.TaskKey("next"), "state").Val() != "pending" {
		t.Fatal("shutdown failed to renew or claimed more")
	}
	releaseHandler()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("shutdown hung")
	}
	if c.Exists(ctx, rdb.TaskKey("a")).Val() != 0 {
		t.Fatal("inflight not completed")
	}
}

func TestShutdownTimeoutLeavesOwnership(t *testing.T) {
	cfg := config(t)
	cfg.ShutdownTimeout = 100 * time.Millisecond
	s, c := running(t, cfg)
	ctx := context.Background()
	seed(t, s.store, "a")
	started := make(chan struct{})
	canceled := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	if err := s.Start(func(ctx context.Context, _ fairq.Task) error {
		close(started)
		<-ctx.Done()
		close(canceled)
		<-release
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	s.Shutdown()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("handler not canceled")
	}
	score := c.ZScore(ctx, rdb.Lease, "a").Val()
	time.Sleep(150 * time.Millisecond)
	if c.ZScore(ctx, rdb.Lease, "a").Val() != score || !c.SIsMember(ctx, rdb.Active, "a").Val() || c.HGet(ctx, rdb.TaskKey("a"), "state").Val() != "active" {
		t.Fatal("shutdown removed or renewed unfinished state")
	}
}

func TestLostOwnershipAndConnectionCancel(t *testing.T) {
	for _, mode := range []string{"token", "network"} {
		t.Run(mode, func(t *testing.T) {
			s, c := running(t, config(t))
			ctx := context.Background()
			seed(t, s.store, "a")
			started := make(chan struct{})
			canceled := make(chan struct{})
			if err := s.Start(func(ctx context.Context, _ fairq.Task) error {
				close(started)
				<-ctx.Done()
				close(canceled)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			<-started
			if mode == "token" {
				c.HSet(ctx, rdb.TaskKey("a"), "attempt_token", "other")
			} else {
				s.store.Close()
			}
			select {
			case <-canceled:
			case <-time.After(time.Second):
				t.Fatal("handler retained unprovable lease")
			}
			s.Shutdown()
			if c.HGet(ctx, rdb.TaskKey("a"), "state").Val() != "active" {
				t.Fatal("lost worker submitted state")
			}
		})
	}
}

type scheduledWait struct {
	d  time.Duration
	ch chan time.Time
}
type controlledClock struct{ waits chan scheduledWait }

func (c *controlledClock) Now() time.Time { return time.Now() }
func (c *controlledClock) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	c.waits <- scheduledWait{d, ch}
	return ch
}

func TestCorruptionWaitReallyStopsDequeue(t *testing.T) {
	c := testutil.Redis(t, 3)
	s, err := New(context.Background(), config(t))
	if err != nil {
		t.Fatal(err)
	}
	defer s.store.Close()
	s.state = 1
	clk := &controlledClock{waits: make(chan scheduledWait, 2)}
	s.clock = clk
	g, _ := base.Group("same")
	seed(t, s.store, "valid")
	for i := 0; i < 14; i++ {
		c.RPush(context.Background(), rdb.PendingKey(g), fmt.Sprintf("missing-%d", i))
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); s.worker(ctx, func(context.Context, fairq.Task) error { return nil }) }()
	for i, want := range []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond, time.Second} {
		var w scheduledWait
		select {
		case w = <-clk.waits:
		case <-time.After(time.Second):
			t.Fatal("no corrupt wait")
		}
		if w.d != want {
			t.Fatal(w.d, want)
		}
		n := c.LLen(context.Background(), rdb.PendingKey(g)).Val()
		if n != int64(5-i) {
			t.Fatal("wrong dequeue count", n)
		}
		time.Sleep(10 * time.Millisecond)
		if c.LLen(context.Background(), rdb.PendingKey(g)).Val() != n {
			t.Fatal("dequeue during wait")
		}
		w.ch <- time.Now()
	}
	var empty scheduledWait
	select {
	case empty = <-clk.waits:
	case <-time.After(time.Second):
		t.Fatal("empty wait missing")
	}
	if empty.d != s.cfg.PollInterval {
		t.Fatal(empty.d)
	}
	for i := 0; i < 10; i++ {
		c.LPush(context.Background(), rdb.PendingKey(g), fmt.Sprintf("new-missing-%d", i))
	}
	c.RPush(context.Background(), rdb.Ready, g)
	c.SAdd(context.Background(), rdb.Members, g)
	empty.ch <- time.Now()
	select {
	case w := <-clk.waits:
		if w.d != 100*time.Millisecond {
			t.Fatal("success did not reset corruption streak", w.d)
		}
	case <-time.After(time.Second):
		t.Fatal("reset wait missing")
	}
	cancel()
	<-done
}

func TestLateRenewalCannotRevive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	e := &execution{ctx: ctx, cancel: cancel, until: time.Now().Add(-time.Millisecond), renewed: make(chan struct{}, 1)}
	e.extend(time.Now(), time.Now().Add(time.Hour))
	if _, valid := e.proof(time.Now()); valid || ctx.Err() == nil {
		t.Fatal("revived expired execution")
	}
}

func TestConfigRejectsUnsafeTiming(t *testing.T) {
	for _, change := range []func(*Config){
		func(c *Config) { c.Concurrency = 0 }, func(c *Config) { c.ShutdownTimeout = -1 },
		func(c *Config) { c.LeaseDuration = -time.Second }, func(c *Config) { c.HeartbeatInterval = c.LeaseDuration },
		func(c *Config) { c.BatchSize = 101 }, func(c *Config) { c.MaxBatchesPerTick = 6 },
	} {
		c := Config{Concurrency: 1, ShutdownTimeout: time.Second, LeaseDuration: time.Second, HeartbeatInterval: 100 * time.Millisecond}
		change(&c)
		if err := c.defaults(); err == nil {
			t.Fatal("unsafe config accepted")
		}
	}
	for n, want := range map[int]time.Duration{9: 0, 10: 100 * time.Millisecond, 11: 200 * time.Millisecond, 12: 400 * time.Millisecond, 13: 800 * time.Millisecond, 14: time.Second, 100000: time.Second} {
		if got := corruptDelay(n); got != want {
			t.Fatal(n, got, want)
		}
	}
}

func TestKilledWorkerChild(t *testing.T) {
	path := os.Getenv("FAIRQ_KILL_CHILD")
	if path == "" {
		return
	}
	s, err := New(context.Background(), config(t))
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Start(func(context.Context, fairq.Task) error {
		if err := os.WriteFile(path, []byte("started"), 0600); err != nil {
			t.Error(err)
		}
		select {}
	}); err != nil {
		t.Fatal(err)
	}
	select {}
}

func TestKillWorkerRecovery(t *testing.T) {
	c := testutil.Redis(t, 3)
	ctx := context.Background()
	s, err := New(ctx, config(t))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Shutdown()
	seed(t, s.store, "killed")
	marker := filepath.Join(t.TempDir(), "started")
	cmd := exec.Command(os.Args[0], "-test.run=^TestKilledWorkerChild$", "-test.timeout=30s")
	cmd.Env = append(os.Environ(), "FAIRQ_KILL_CHILD="+marker)
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if cmd.ProcessState == nil {
			cmd.Process.Kill()
			cmd.Wait()
		}
	}()
	eventually(t, 3*time.Second, func() bool { _, err := os.Stat(marker); return err == nil })
	if err = cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	cmd.Wait()
	done := make(chan string, 1)
	if err = s.Start(func(_ context.Context, task fairq.Task) error { done <- task.ID; return nil }); err != nil {
		t.Fatal(err)
	}
	eventually(t, 2*time.Second, func() bool { return c.HGet(ctx, rdb.TaskKey("killed"), "state").Val() == "retry" })
	if c.HGet(ctx, rdb.TaskKey("killed"), "retry_count").Val() != "1" {
		t.Fatal("wrong recovery count")
	}
	select {
	case id := <-done:
		if id != "killed" {
			t.Fatal(id)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("killed task not recovered through real backoff")
	}
}

func TestRunCancellationPreservesGracefulDrain(t *testing.T) {
	cfg := config(t)
	cfg.Concurrency = 1
	s, c := running(t, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	seed(t, s.store, "run")
	started := make(chan context.Context, 1)
	release := make(chan struct{})
	releaseHandler := sync.OnceFunc(func() { close(release) })
	defer releaseHandler()
	done := make(chan error, 1)
	go func() {
		done <- s.Run(ctx, func(handlerCtx context.Context, _ fairq.Task) error { started <- handlerCtx; <-release; return nil })
	}()
	handlerCtx := <-started
	cancel()
	eventually(t, time.Second, func() bool { s.mu.Lock(); defer s.mu.Unlock(); return s.state == 2 })
	select {
	case err := <-done:
		t.Fatal("Run returned before graceful drain", err)
	default:
	}
	if handlerCtx.Err() != nil {
		t.Fatal("Run cancellation canceled in-flight handler too early")
	}
	shutdowns := make(chan struct{}, 3)
	for i := 0; i < 3; i++ {
		go func() { s.Shutdown(); shutdowns <- struct{}{} }()
	}
	releaseHandler()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not finish")
	}
	for i := 0; i < 3; i++ {
		select {
		case <-shutdowns:
		case <-time.After(time.Second):
			t.Fatal("concurrent shutdown hung")
		}
	}
	if c.Exists(context.Background(), rdb.TaskKey("run")).Val() != 0 {
		t.Fatal("gracefully completed task retained")
	}
}
