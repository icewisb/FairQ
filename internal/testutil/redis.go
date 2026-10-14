// Package testutil operates only on explicitly selected, isolated test databases.
package testutil

import (
	"context"
	"os"
	"testing"

	"github.com/redis/go-redis/v9"
)

func Options(t testing.TB, db int) redis.UniversalOptions {
	t.Helper()
	addr := os.Getenv("FAIRQ_TEST_REDIS")
	if addr == "" {
		t.Fatal("FAIRQ_TEST_REDIS must point to an isolated real Redis; tests do not skip")
	}
	if db < 1 || db > 15 {
		t.Fatal("test DB must be 1..15")
	}
	return redis.UniversalOptions{Addrs: []string{addr}, DB: db, MaxRetries: -1, ContextTimeoutEnabled: true}
}
func Redis(t testing.TB, db int) *redis.Client {
	t.Helper()
	opts := Options(t, db)
	c := redis.NewClient(opts.Simple())
	if err := c.FlushDB(context.Background()).Err(); err != nil {
		c.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { c.FlushDB(context.Background()); c.Close() })
	return c
}
