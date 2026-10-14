package rdb

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"
	"time"

	"github.com/icewisb/FairQ"
	"github.com/icewisb/FairQ/internal/base"
)

var enqueueScript = script("enqueue")

func (s *Store) Enqueue(ctx context.Context, task fairq.Task, opts fairq.EnqueueOptions, group string, msg []byte) (fairq.TaskInfo, error) {
	info := fairq.TaskInfo{Task: task, GroupKey: opts.GroupKey}
	if opts.MaxRetry == 0 {
		opts.MaxRetry = 25
	}
	r, err := s.run(ctx, "enqueue", enqueueScript, task.ID, group, msg, opts.MaxRetry, opts.Timeout.Milliseconds(), opts.Retention.Milliseconds())
	if err != nil {
		return info, err
	}
	if len(r) != 2 {
		return info, &fairq.Error{Code: fairq.Internal, Op: "enqueue"}
	}
	at, err := integer(r[1])
	if err != nil {
		return info, &fairq.Error{Code: fairq.Internal, Op: "enqueue", Err: err}
	}
	info.State = "pending"
	info.MaxRetry = opts.MaxRetry
	info.Timeout = opts.Timeout
	info.Retention = opts.Retention
	info.EnqueuedAt = time.UnixMilli(at)
	info.PendingSince = info.EnqueuedAt
	return info, nil
}

func (s *Store) GetTask(ctx context.Context, id string) (fairq.TaskInfo, error) {
	info := fairq.TaskInfo{Task: fairq.Task{ID: id}}
	h, err := s.Redis.HGetAll(ctx, TaskKey(id)).Result()
	if err != nil {
		return info, &fairq.Error{Code: fairq.Internal, Op: "get", Err: err}
	}
	if len(h) == 0 {
		return info, fairq.ErrTaskNotFound
	}
	group, err := base64.RawURLEncoding.DecodeString(h["group"])
	if err != nil {
		return info, &fairq.Error{Code: fairq.Internal, Op: "get", Err: err}
	}
	info.GroupKey = string(group)
	info.State = h["state"]
	info.Error = h["error"]
	for name, dst := range map[string]*int{"retry_count": &info.RetryCount, "max_retry": &info.MaxRetry} {
		n, err := strconv.Atoi(h[name])
		if err != nil || n < 0 {
			return info, &fairq.Error{Code: fairq.Internal, Op: "get", Err: fmt.Errorf("invalid %s", name)}
		}
		*dst = n
	}
	for name, dst := range map[string]*time.Duration{"timeout_ms": &info.Timeout, "retention_ms": &info.Retention} {
		n, err := strconv.ParseInt(h[name], 10, 64)
		if err != nil || n < 0 || n > int64((1<<63-1)/time.Millisecond) {
			return info, &fairq.Error{Code: fairq.Internal, Op: "get", Err: fmt.Errorf("invalid %s", name)}
		}
		*dst = time.Duration(n) * time.Millisecond
	}
	for name, dst := range map[string]*time.Time{"enqueued_at": &info.EnqueuedAt, "pending_since": &info.PendingSince, "started_at": &info.StartedAt, "finished_at": &info.FinishedAt} {
		if h[name] == "" {
			continue
		}
		n, err := strconv.ParseInt(h[name], 10, 64)
		if err != nil {
			return info, &fairq.Error{Code: fairq.Internal, Op: "get", Err: err}
		}
		*dst = time.UnixMilli(n)
	}
	msg, err := base.Decode([]byte(h["msg"]))
	if err != nil {
		return info, &fairq.Error{Code: fairq.DecodeFailed, Op: "get", Err: err}
	}
	info.Type = msg.Type
	info.Payload = msg.Payload
	return info, nil
}
