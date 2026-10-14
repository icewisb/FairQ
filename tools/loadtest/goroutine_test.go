package main

import (
	"runtime"
	"runtime/debug"
	"sync"
	"testing"
	"time"
)

func TestGoroutineCountExcludesDeadGoroutinesDuringGC(t *testing.T) {
	previousGC := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(previousGC)
	if previous := runtime.GOMAXPROCS(0); previous < 2 {
		runtime.GOMAXPROCS(2)
		defer runtime.GOMAXPROCS(previous)
	}
	baseline := profileGoroutineCount()
	for trial := 0; trial < 3; trial++ {
		var warm sync.WaitGroup
		release := make(chan struct{})
		for i := 0; i < 8192; i++ {
			warm.Add(1)
			go func() {
				defer warm.Done()
				<-release
			}()
		}
		close(release)
		warm.Wait()
		waitForProfileGoroutineCount(t, baseline)

		// GC temporarily removes dead goroutines from the runtime's free lists.
		done := make(chan struct{})
		go func() {
			runtime.GC()
			close(done)
		}()
		peak := 0
	collect:
		for {
			select {
			case <-done:
				break collect
			default:
				if count := goroutineCount(); count > peak {
					peak = count
				}
			}
		}
		waitForProfileGoroutineCount(t, baseline)
		if peak > baseline+1 {
			t.Fatalf("trial %d: peak = %d, live bound = %d (baseline plus GC caller)", trial, peak, baseline+1)
		}
	}
}

func TestGoroutineCountTracksBlockedGoroutines(t *testing.T) {
	baseline := profileGoroutineCount()
	const workers = 80
	var started, finished sync.WaitGroup
	started.Add(workers)
	finished.Add(workers)
	release := make(chan struct{})
	for i := 0; i < workers; i++ {
		go func() {
			defer finished.Done()
			started.Done()
			<-release
		}()
	}
	started.Wait()
	active := goroutineCount()
	close(release)
	finished.Wait()
	waitForProfileGoroutineCount(t, baseline)
	final := goroutineCount()
	if active != baseline+workers {
		t.Fatalf("blocked count = %d, want %d", active, baseline+workers)
	}
	if final != baseline {
		t.Fatalf("released count = %d, want baseline %d", final, baseline)
	}
}

func profileGoroutineCount() int {
	records := make([]runtime.StackRecord, 128)
	for {
		count, ok := runtime.GoroutineProfile(records)
		if ok {
			return count
		}
		records = make([]runtime.StackRecord, count+1)
	}
}

func waitForProfileGoroutineCount(t *testing.T, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		count := profileGoroutineCount()
		if count == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("profile count = %d, want baseline %d", count, want)
		}
		time.Sleep(time.Millisecond)
	}
}
