package testutil

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ChildBarrier synchronizes OS processes without adding queue Redis keys.
func ChildBarrier(t testing.TB) {
	t.Helper()
	dir := os.Getenv("FAIRQ_TEST_BARRIER")
	if dir == "" {
		t.Fatal("missing child barrier")
	}
	if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("ready-%d", os.Getpid())), nil, 0600); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(dir, "release")); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("parent did not release process barrier")
		}
		time.Sleep(time.Millisecond)
	}
}
func ReleaseBarrier(t testing.TB, dir string, children int) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		ready := 0
		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), "ready-") {
				ready++
			}
		}
		if ready == children {
			break
		}
		if time.Now().After(deadline) {
			os.WriteFile(filepath.Join(dir, "release"), nil, 0600)
			t.Fatalf("only %d/%d child processes ready", ready, children)
		}
		time.Sleep(time.Millisecond)
	}
	if err := os.WriteFile(filepath.Join(dir, "release"), nil, 0600); err != nil {
		t.Fatal(err)
	}
}
