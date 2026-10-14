// basic enqueues one demo task and runs a worker until interrupted.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/icewisb/FairQ"
	"github.com/icewisb/FairQ/client"
	"github.com/icewisb/FairQ/server"
	"github.com/redis/go-redis/v9"
)

func main() {
	addr := flag.String("redis", "", "dedicated demo Redis address (required)")
	flag.Parse()
	if *addr == "" {
		log.Fatal("-redis is required; use a dedicated demo instance")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := run(ctx, redis.UniversalOptions{Addrs: []string{*addr}}); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, options redis.UniversalOptions) error {
	producer, err := client.New(ctx, client.Config{Redis: options})
	if err != nil {
		return err
	}
	defer producer.Close()
	worker, err := server.New(ctx, server.Config{
		Redis: options, Concurrency: 2, ShutdownTimeout: 5 * time.Second,
	})
	if err != nil {
		return err
	}
	defer worker.Shutdown()
	info, err := producer.Enqueue(ctx, fairq.Task{Type: "demo"},
		fairq.EnqueueOptions{GroupKey: "tenant-a", Timeout: time.Second, Retention: time.Minute})
	if err != nil {
		return fmt.Errorf("enqueue task %s (check this ID before retrying): %w", info.ID, err)
	}
	return worker.Run(ctx, func(ctx context.Context, task fairq.Task) error {
		// Real handlers must make external side effects idempotent by task/business ID.
		fmt.Printf("handled task %s\n", task.ID)
		return ctx.Err()
	})
}
