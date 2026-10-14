// Package client implements FairQ producers and basic task queries.
package client

import (
	"context"
	"fmt"

	"fairq"
	"fairq/internal/base"
	"fairq/internal/rdb"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type Client struct {
	store      *rdb.Store
	maxPayload int
}

type Config struct {
	Redis      redis.UniversalOptions
	MaxPayload int
}

func New(ctx context.Context, config Config) (*Client, error) {
	if config.MaxPayload == 0 {
		config.MaxPayload = base.MaxPayload
	}
	if config.MaxPayload < 0 {
		return nil, fmt.Errorf("fairq: MaxPayload must be positive")
	}
	s, err := rdb.New(ctx, config.Redis)
	if err != nil {
		return nil, err
	}
	return &Client{store: s, maxPayload: config.MaxPayload}, nil
}
func (c *Client) Close() error { return c.store.Close() }

func (c *Client) Enqueue(ctx context.Context, task fairq.Task, opts fairq.EnqueueOptions) (fairq.TaskInfo, error) {
	if task.ID == "" {
		task.ID = uuid.NewString()
	}
	info := fairq.TaskInfo{Task: task, GroupKey: opts.GroupKey}
	group, msg, err := base.Validate(task, opts, c.maxPayload)
	if err != nil {
		return info, err
	}
	if opts.Retention > 0 {
		return info, fmt.Errorf("fairq: retention is not enabled in this release")
	}
	return c.store.Enqueue(ctx, task, opts, group, msg)
}
func (c *Client) GetTask(ctx context.Context, id string) (fairq.TaskInfo, error) {
	return c.store.GetTask(ctx, id)
}
