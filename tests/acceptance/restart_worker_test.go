package acceptance_test

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"fairq"
	"fairq/internal/rdb"
	"fairq/server"
	"github.com/redis/go-redis/v9"
)

func TestWorkerSurvivesRedisRestart(t *testing.T) {
	n := newNode(t)
	n.start()
	seedTasks(t, n.options(), 1)
	ctx := context.Background()
	s, err := server.New(ctx, server.Config{Redis: n.options(), Concurrency: 2, LeaseDuration: 400 * time.Millisecond, HeartbeatInterval: 50 * time.Millisecond, RecoverInterval: 20 * time.Millisecond, ForwarderInterval: 20 * time.Millisecond, PollInterval: 10 * time.Millisecond, ShutdownTimeout: time.Second, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Shutdown()
	oldRelease, newRelease := make(chan struct{}), make(chan struct{})
	defer close(oldRelease)
	defer close(newRelease)
	first, second, canceled := make(chan struct{}, 1), make(chan struct{}, 1), make(chan struct{}, 1)
	var attempts atomic.Int32
	if err = s.Start(func(handlerCtx context.Context, _ fairq.Task) error {
		if attempts.Add(1) == 1 {
			first <- struct{}{}
			<-handlerCtx.Done()
			canceled <- struct{}{}
			<-oldRelease
			return nil
		}
		second <- struct{}{}
		<-newRelease
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-first:
	case <-time.After(time.Second):
		t.Fatal("first attempt not started")
	}
	started := time.Now()
	n.stop()
	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("lost Redis lease did not cancel handler")
	}
	n.start()
	o := n.options()
	c := redis.NewClient(o.Simple())
	defer c.Close()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if c.HGet(ctx, rdb.TaskKey("0"), "state").Val() == "retry" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("live Worker did not reconnect and recover")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if c.HGet(ctx, rdb.TaskKey("0"), "retry_count").Val() != "1" {
		t.Fatal("restart recovery count")
	}
	retryAt, err := c.ZScore(ctx, rdb.Retry, "0").Result()
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-second:
	case <-time.After(20 * time.Second):
		t.Fatal("retry was not forwarded after real backoff")
	}
	t.Logf("live Worker Redis restart: handler_cancel_and_recovery_RTO=%s retry_at=%d attempts=%d; includes actual 15s+jitter backoff", time.Since(started), int64(retryAt), attempts.Load())
	if attempts.Load() != 2 || c.HGet(ctx, rdb.TaskKey("0"), "state").Val() != "active" {
		t.Fatal("new execution missing")
	}
	// Release the late old handler while the new attempt is still active.
	oldRelease <- struct{}{}
	time.Sleep(30 * time.Millisecond)
	if c.HGet(ctx, rdb.TaskKey("0"), "state").Val() != "active" {
		t.Fatal("late old handler mutated new attempt")
	}
	newRelease <- struct{}{}
	deadline = time.Now().Add(time.Second)
	for {
		if c.Exists(ctx, rdb.TaskKey("0")).Val() == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("new attempt did not complete")
		}
		time.Sleep(5 * time.Millisecond)
	}
	healthy(t, n.options(), 0)
}
