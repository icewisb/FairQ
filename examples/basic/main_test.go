package main

import (
	"context"
	"testing"
	"time"

	"fairq/internal/rdb"
	"fairq/internal/testutil"
)

func TestExampleCompletesAndShutsDown(t *testing.T) {
	c := testutil.Redis(t, 6)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	opts := testutil.Options(t, 6)
	done := make(chan error, 1)
	go func() { done <- run(ctx, opts) }()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for c.ZCard(ctx, rdb.Completed).Val() != 1 {
		select {
		case err := <-done:
			t.Fatal("example exited before completing a task", err)
		case <-ctx.Done():
			t.Fatal("example did not complete its task")
		case <-ticker.C:
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("example did not shut down")
	}
	if c.SCard(context.Background(), rdb.Active).Val() != 0 || c.ZCard(context.Background(), rdb.Lease).Val() != 0 {
		t.Fatal("completed example retained ownership")
	}
}
