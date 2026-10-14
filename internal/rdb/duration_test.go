package rdb_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"fairq"
	"fairq/internal/base"
	"fairq/internal/rdb"
)

func TestDequeueRejectsInvalidMetadataBeforeWrite(t *testing.T) {
	for _, field := range []string{"retry_count", "max_retry", "timeout_ms", "retention_ms"} {
		for _, value := range []string{"missing", "invalid"} {
			t.Run(field+"/"+value, func(t *testing.T) {
				s, c := store(t)
				ctx := context.Background()
				enqueue(t, s, "metadata", "g")
				var err error
				if value == "missing" {
					err = c.HDel(ctx, rdb.TaskKey("metadata"), field).Err()
				} else {
					err = c.HSet(ctx, rdb.TaskKey("metadata"), field, value).Err()
				}
				if err != nil {
					t.Fatal(err)
				}
				before := dumpDB(t, c)
				if _, err := s.Dequeue(ctx, time.Minute); !fairq.IsCode(err, fairq.Internal) {
					t.Fatal("invalid metadata accepted", err)
				}
				if !reflect.DeepEqual(before, dumpDB(t, c)) {
					t.Fatal("invalid metadata changed task/indexes")
				}
			})
		}
	}
}

func TestCorruptDurationRejectedBeforeWrite(t *testing.T) {
	for _, tc := range []struct{ operation, field string }{
		{"dequeue", "timeout_ms"}, {"dequeue", "retention_ms"}, {"done", "retention_ms"},
	} {
		for _, value := range []int64{int64((1<<63-1)/time.Millisecond) + 1, base.MaxExactInteger} {
			t.Run(fmt.Sprintf("%s/%s/%d", tc.operation, tc.field, value), func(t *testing.T) {
				s, c := store(t)
				ctx := context.Background()
				enqueue(t, s, "duration", "g")
				var a rdb.Attempt
				var err error
				if tc.operation == "done" {
					a, err = s.Dequeue(ctx, time.Minute)
					if err != nil {
						t.Fatal(err)
					}
				}
				if err = c.HSet(ctx, rdb.TaskKey("duration"), tc.field, value).Err(); err != nil {
					t.Fatal(err)
				}
				before := dumpDB(t, c)
				if tc.operation == "done" {
					err = s.Done(ctx, a)
				} else {
					a, err = s.Dequeue(ctx, time.Minute)
				}
				if !fairq.IsCode(err, fairq.Internal) {
					t.Errorf("expected INTERNAL, got %v (timeout=%v)", err, a.Timeout)
				}
				if !reflect.DeepEqual(before, dumpDB(t, c)) {
					t.Fatal("invalid duration changed task/indexes")
				}
			})
		}
	}
}

func TestMaximumDurationRoundTrip(t *testing.T) {
	s, _ := store(t)
	ctx := context.Background()
	d := time.Duration((1<<63-1)/time.Millisecond) * time.Millisecond
	task := fairq.Task{ID: "duration", Type: "test"}
	opts := fairq.EnqueueOptions{GroupKey: "g", Timeout: d, Retention: d}
	g, msg, err := base.Validate(task, opts, base.MaxPayload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Enqueue(ctx, task, opts, g, msg); err != nil {
		t.Fatal(err)
	}
	a, err := s.Dequeue(ctx, time.Minute)
	if err != nil || a.Timeout != d {
		t.Fatal(a, err)
	}
	if err = s.Done(ctx, a); err != nil {
		t.Fatal(err)
	}
	info, err := s.GetTask(ctx, task.ID)
	if err != nil || info.State != "completed" || info.Timeout != d || info.Retention != d {
		t.Fatal(info, err)
	}
}
