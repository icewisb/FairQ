// Package testutil operates only on explicitly selected, isolated test databases.
package testutil

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
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
	// Separate go test invocations must not flush each other's fixed-namespace database.
	name := fmt.Sprintf("fairq-test-%x-%d.lock", sha256.Sum256([]byte(opts.Addrs[0])), db)
	f, err := os.OpenFile(filepath.Join(os.TempDir(), name), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { syscall.Flock(int(f.Fd()), syscall.LOCK_UN); f.Close() })
	c := redis.NewClient(opts.Simple())
	if err := c.FlushDB(context.Background()).Err(); err != nil {
		c.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { c.FlushDB(context.Background()); c.Close() })
	return c
}
