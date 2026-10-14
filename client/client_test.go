package client_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/icewisb/FairQ"
	"github.com/icewisb/FairQ/client"
	"github.com/icewisb/FairQ/internal/base"
	"github.com/icewisb/FairQ/internal/rdb"
	"github.com/icewisb/FairQ/internal/testutil"
)

func TestEnqueueConcurrentAndQuery(t *testing.T) {
	r := testutil.Redis(t, 1)
	ctx := context.Background()
	c, err := client.New(ctx, client.Config{Redis: testutil.Options(t, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := c.Enqueue(ctx, fairq.Task{Type: "mail", Payload: []byte{0, 255}}, fairq.EnqueueOptions{GroupKey: "用户/a"})
			if err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	g, _ := base.Group("用户/a")
	if r.LLen(ctx, rdb.Ready).Val() != 1 || r.SCard(ctx, rdb.Members).Val() != 1 || r.LLen(ctx, rdb.PendingKey(g)).Val() != 100 {
		t.Fatal("group activation or task count incorrect")
	}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := c.Enqueue(ctx, fairq.Task{ID: "stable", Type: "mail"}, fairq.EnqueueOptions{GroupKey: "用户/a"})
			if err != nil && !fairq.IsCode(err, fairq.TaskIDConflict) {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if r.LLen(ctx, rdb.PendingKey(g)).Val() != 101 {
		t.Fatal("duplicate task")
	}
	info, err := c.GetTask(ctx, "stable")
	if err != nil || info.State != "pending" || info.GroupKey != "用户/a" || info.MaxRetry != 25 || info.RetryCount != 0 || info.EnqueuedAt.IsZero() {
		t.Fatalf("%+v %v", info, err)
	}
	if _, err = c.GetTask(ctx, "absent"); !errors.Is(err, fairq.ErrTaskNotFound) {
		t.Fatal(err)
	}
}

func TestValidationAndUnknownID(t *testing.T) {
	r := testutil.Redis(t, 1)
	ctx := context.Background()
	c, err := client.New(ctx, client.Config{Redis: testutil.Options(t, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for _, opts := range []fairq.EnqueueOptions{{}, {GroupKey: "x", MaxRetry: -1}, {GroupKey: "x", Timeout: -time.Second}, {GroupKey: "x", Timeout: time.Nanosecond}, {GroupKey: "x", Retention: -time.Second}} {
		if _, err := c.Enqueue(ctx, fairq.Task{Type: "x"}, opts); err == nil {
			t.Fatal("accepted invalid options")
		}
	}
	if _, err := c.Enqueue(ctx, fairq.Task{Type: "x", Payload: make([]byte, base.MaxPayload+1)}, fairq.EnqueueOptions{GroupKey: "x"}); err == nil {
		t.Fatal("accepted oversized payload")
	}
	if r.DBSize(ctx).Val() != 0 {
		t.Fatal("validation wrote data")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	info, err := c.Enqueue(canceled, fairq.Task{Type: "x"}, fairq.EnqueueOptions{GroupKey: "x"})
	if err == nil || info.ID == "" || info.State != "" {
		t.Fatalf("missing stable ID on error: %+v %v", info, err)
	}
	r.Set(ctx, rdb.Ready, "wrongtype", 0)
	info, err = c.Enqueue(ctx, fairq.Task{Type: "x"}, fairq.EnqueueOptions{GroupKey: "x"})
	if !fairq.IsCode(err, fairq.Internal) || r.Exists(ctx, rdb.TaskKey(info.ID)).Val() != 0 {
		t.Fatalf("partial write: %v", err)
	}
}

func TestRejectClusterOptions(t *testing.T) {
	opts := testutil.Options(t, 1)
	opts.IsClusterMode = true
	if c, err := client.New(context.Background(), client.Config{Redis: opts}); err == nil {
		c.Close()
		t.Fatal("cluster accepted")
	}
}

func TestTaskIDDedupEndsWhenHashIsDeleted(t *testing.T) {
	testutil.Redis(t, 1)
	ctx := context.Background()
	opts := testutil.Options(t, 1)
	producer, err := client.New(ctx, client.Config{Redis: opts})
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	store, err := rdb.New(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var previous rdb.Attempt
	for i := 0; i < 3; i++ {
		task := fairq.Task{ID: "stable", Type: "test"}
		enq := fairq.EnqueueOptions{GroupKey: "g"}
		if _, err = producer.Enqueue(ctx, task, enq); err != nil {
			t.Fatal(err)
		}
		if _, err = producer.Enqueue(ctx, task, enq); !fairq.IsCode(err, fairq.TaskIDConflict) {
			t.Fatal(err)
		}
		attempt, e := store.Dequeue(ctx, time.Minute)
		if e != nil {
			t.Fatal(e)
		}
		if i > 0 {
			if attempt.Token == previous.Token {
				t.Fatal("reused execution token")
			}
			if err = store.Done(ctx, previous); !fairq.IsCode(err, fairq.StateConflict) {
				t.Fatal("old task generation could finish new task", err)
			}
		}
		if err = store.Done(ctx, attempt); err != nil {
			t.Fatal(err)
		}
		if _, err = producer.GetTask(ctx, task.ID); !errors.Is(err, fairq.ErrTaskNotFound) {
			t.Fatal(err)
		}
		previous = attempt
	}
}
