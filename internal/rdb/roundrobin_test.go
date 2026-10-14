package rdb_test

import (
	"context"
	"testing"
	"time"

	"github.com/icewisb/FairQ"
	"github.com/icewisb/FairQ/internal/base"
	"github.com/icewisb/FairQ/internal/rdb"
)

func TestDynamicGroupsAndUnknownCorruption(t *testing.T) {
	s, c := store(t)
	ctx := context.Background()
	enqueue(t, s, "a1", "a")
	enqueue(t, s, "a2", "a")
	enqueue(t, s, "b1", "b")
	a, err := s.Dequeue(ctx, time.Minute)
	if err != nil || a.ID != "a1" {
		t.Fatal(a, err)
	}
	enqueue(t, s, "c1", "c")
	for _, want := range []string{"b1", "a2", "c1"} {
		a, err = s.Dequeue(ctx, time.Minute)
		if err != nil || a.ID != want {
			t.Fatal(a, err)
		}
	}
	enqueue(t, s, "b2", "b")
	enqueue(t, s, "a3", "a")
	for _, want := range []string{"b2", "a3"} {
		a, err = s.Dequeue(ctx, time.Minute)
		if err != nil || a.ID != want {
			t.Fatal(a, err)
		}
	}
	enqueue(t, s, "badgroup", "a")
	c.HSet(ctx, rdb.TaskKey("badgroup"), "group", "different")
	g, _ := base.Group("a")
	if _, err = s.Dequeue(ctx, time.Minute); !fairq.IsCode(err, fairq.Internal) {
		t.Fatal(err)
	}
	if c.LLen(ctx, rdb.PendingKey(g)).Val() != 1 || c.LLen(ctx, rdb.BadRefs).Val() != 0 {
		t.Fatal("unknown ownership was modified")
	}
}
