// loadtest generates synthetic tasks to test FairQ against an isolated empty Redis DB.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"fairq"
	"fairq/client"
	"fairq/inspector"
	"fairq/server"
	"github.com/redis/go-redis/v9"
)

var addr = flag.String("redis", "", "isolated Redis address; selected DB must be empty")
var db = flag.Int("db", 5, "isolated DB")
var duration = flag.Duration("duration", 30*time.Second, "sustained load duration, excluding seed/drain")
var groups = flag.Int("groups", 1000, "number of simulated groups; not a queue configuration or limit")
var backlog = flag.Int("backlog", 100000, "target pending backlog")
var processes = flag.Int("processes", 10, "independent worker processes")
var slots = flag.Int("slots", 80, "worker concurrency per process")
var handlerTime = flag.Duration("handler-time", time.Second, "simulated handler latency")
var child = flag.Bool("worker", false, "internal child mode")
var reportPath = flag.String("report", "", "JSON report path (required in parent mode)")

type event struct {
	ID         string `json:"id,omitempty"`
	Metrics    string `json:"metrics,omitempty"`
	Goroutines int    `json:"goroutines,omitempty"`
	Error      string `json:"error,omitempty"`
}
type sample struct {
	ElapsedSeconds    float64
	Accepted, Handled uint64
	UsedMemory        int64
	WorkerGoroutines  []int
}
type report struct {
	Started                                         time.Time
	DurationSeconds, DrainSeconds                   float64
	Groups, Backlog, Processes, Slots, PayloadBytes int
	Accepted, Handled, Duplicates                   uint64
	PeakRedisBytes, MemoryBudgetBytes               int64
	CommandCount, CommandAbove30ms                  uint64
	Samples                                         []sample
	Errors                                          []string
	Inspector                                       inspector.Report
	Passed                                          bool
}

func options() redis.UniversalOptions {
	return redis.UniversalOptions{Addrs: []string{*addr}, DB: *db, MaxRetries: -1, ContextTimeoutEnabled: true}
}
func main() {
	flag.Parse()
	if *addr == "" || *db < 1 || *db > 15 || *slots < 1 || *groups < 1 || *processes < 1 || *backlog < 1 || *duration <= 0 || *handlerTime <= 0 {
		log.Fatal("explicit isolated Redis and positive settings required")
	}
	if *child {
		if err := worker(); err != nil {
			log.Fatal(err)
		}
		return
	}
	if *reportPath == "" {
		log.Fatal("-report required")
	}
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
func worker() error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	s, err := server.New(ctx, server.Config{Redis: options(), Concurrency: *slots, ShutdownTimeout: 30 * time.Second, PollInterval: 100 * time.Millisecond})
	if err != nil {
		return err
	}
	var output sync.Mutex
	emit := func(e event) { output.Lock(); defer output.Unlock(); json.NewEncoder(os.Stdout).Encode(e) }
	if err = s.Start(func(ctx context.Context, t fairq.Task) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(*handlerTime):
			emit(event{ID: t.ID})
			return nil
		}
	}); err != nil {
		return err
	}
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	scrape := func() {
		r := httptest.NewRecorder()
		s.MetricsHandler().ServeHTTP(r, httptest.NewRequest("GET", "/metrics", nil))
		if r.Code != 200 {
			emit(event{Error: "metrics scrape failed"})
			return
		}
		emit(event{Metrics: r.Body.String(), Goroutines: runtime.NumGoroutine()})
	}
	for {
		select {
		case <-ticker.C:
			scrape()
		case <-ctx.Done():
			scrape()
			s.Shutdown()
			return nil
		}
	}
}
func infoInt(raw, key string) int64 {
	for _, line := range strings.Split(raw, "\n") {
		if strings.HasPrefix(line, key+":") {
			n, _ := strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(line, key+":")), 10, 64)
			return n
		}
	}
	return 0
}
func commandCounts(raw string) (all, under uint64) {
	for _, line := range strings.Split(raw, "\n") {
		parts := strings.Fields(line)
		if len(parts) != 2 {
			continue
		}
		n, _ := strconv.ParseUint(parts[1], 10, 64)
		if strings.HasPrefix(parts[0], "fairq_redis_command_duration_seconds_count{") {
			all += n
		}
		if strings.HasPrefix(parts[0], "fairq_redis_command_duration_seconds_bucket{") && strings.Contains(parts[0], `le="0.03"`) {
			under += n
		}
	}
	return
}
func operationErrors(raw string) []string {
	var failures []string
	for _, line := range strings.Split(raw, "\n") {
		if !strings.HasPrefix(line, "fairq_operations_total{") || strings.Contains(line, `result="OK"`) || strings.Contains(line, `result="EMPTY"`) {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) == 2 && parts[1] != "0" {
			failures = append(failures, line)
		}
	}
	return failures
}
func run() (result error) {
	ctx := context.Background()
	opts := options()
	c := redis.NewClient(opts.Simple())
	defer c.Close()
	n, err := c.DBSize(ctx).Result()
	if err != nil {
		return err
	}
	if n != 0 {
		return fmt.Errorf("refusing nonempty DB %d", *db)
	}
	memory, err := c.Info(ctx, "memory").Result()
	if err != nil {
		return err
	}
	budget := infoInt(memory, "maxmemory")
	if budget <= 0 || !strings.Contains(memory, "maxmemory_policy:noeviction") {
		return fmt.Errorf("explicit maxmemory and noeviction required")
	}
	r := report{Started: time.Now().UTC(), Groups: *groups, Backlog: *backlog, Processes: *processes, Slots: *slots, PayloadBytes: 256, MemoryBudgetBytes: budget}
	defer func() {
		if result != nil {
			r.Errors = append(r.Errors, result.Error())
		}
		data, e := json.MarshalIndent(r, "", "  ")
		if e == nil {
			e = os.WriteFile(*reportPath, append(data, '\n'), 0600)
		}
		if e != nil {
			result = fmt.Errorf("report write: %w", e)
		}
	}()
	p, err := client.New(ctx, client.Config{Redis: options()})
	if err != nil {
		return err
	}
	defer p.Close()
	payload := []byte(strings.Repeat("x", r.PayloadBytes))
	enqueue := func(count int) error {
		for j := 0; j < count; j++ {
			id := r.Accepted
			_, e := p.Enqueue(ctx, fairq.Task{ID: fmt.Sprintf("load-%d", id), Type: "load", Payload: payload}, fairq.EnqueueOptions{GroupKey: fmt.Sprintf("group-%d", id%uint64(*groups))})
			if e != nil {
				return fmt.Errorf("enqueue %d: %w", id, e)
			}
			r.Accepted++
		}
		return nil
	}
	if err = enqueue(*backlog + *processes**slots); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "seeded %d tasks\n", r.Accepted)
	var mu sync.Mutex
	seen := map[uint64]bool{}
	latest := make([]event, *processes)
	var streamErrors []string
	var duplicates uint64
	var readers sync.WaitGroup
	commands := make([]*exec.Cmd, 0, *processes)
	defer func() {
		for _, cmd := range commands {
			cmd.Process.Signal(os.Interrupt)
		}
		readers.Wait()
		for _, cmd := range commands {
			if e := cmd.Wait(); e != nil {
				r.Errors = append(r.Errors, e.Error())
			}
		}
		readers.Wait()
	}()
	for k := 0; k < *processes; k++ {
		cmd := exec.Command(os.Args[0], "-worker", "-redis", *addr, "-db", strconv.Itoa(*db), "-slots", strconv.Itoa(*slots), "-handler-time", handlerTime.String())
		out, e := cmd.StdoutPipe()
		if e != nil {
			return e
		}
		cmd.Stderr = os.Stderr
		if e = cmd.Start(); e != nil {
			return e
		}
		commands = append(commands, cmd)
		readers.Add(1)
		go func(index int) {
			defer readers.Done()
			scan := bufio.NewScanner(out)
			scan.Buffer(make([]byte, 4096), 1024*1024)
			for scan.Scan() {
				var ev event
				if e := json.Unmarshal(scan.Bytes(), &ev); e != nil {
					mu.Lock()
					streamErrors = append(streamErrors, e.Error())
					mu.Unlock()
					continue
				}
				mu.Lock()
				if ev.ID != "" {
					id, e := strconv.ParseUint(strings.TrimPrefix(ev.ID, "load-"), 10, 64)
					if e != nil {
						streamErrors = append(streamErrors, e.Error())
					} else {
						if seen[id] {
							duplicates++
						}
						seen[id] = true
					}
				} else if ev.Error != "" {
					streamErrors = append(streamErrors, ev.Error)
				} else {
					latest[index] = ev
				}
				mu.Unlock()
			}
			if e := scan.Err(); e != nil {
				mu.Lock()
				streamErrors = append(streamErrors, e.Error())
				mu.Unlock()
			}
		}(k)
	}
	started := time.Now()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	nextSample := time.Duration(0)
	for time.Since(started) < *duration {
		<-ticker.C
		mu.Lock()
		handled := uint64(len(seen))
		eCount := len(streamErrors)
		mu.Unlock()
		if eCount > 0 {
			return fmt.Errorf("worker stream errors")
		}
		outstanding := int(r.Accepted - handled)
		if err = enqueue(max(0, *backlog+*processes**slots-outstanding)); err != nil {
			return err
		}
		if time.Since(started) >= nextSample {
			mem, e := c.Info(ctx, "memory").Result()
			if e != nil {
				return e
			}
			used := infoInt(mem, "used_memory_peak")
			r.PeakRedisBytes = max(r.PeakRedisBytes, used)
			mu.Lock()
			gs := make([]int, *processes)
			for i, v := range latest {
				gs[i] = v.Goroutines
			}
			mu.Unlock()
			r.Samples = append(r.Samples, sample{time.Since(started).Seconds(), r.Accepted, handled, used, gs})
			nextSample += 30 * time.Second
			fmt.Fprintf(os.Stderr, "elapsed=%.0fs accepted=%d handled=%d outstanding=%d redis_bytes=%d\n", time.Since(started).Seconds(), r.Accepted, handled, r.Accepted-handled, used)
		}
	}
	r.DurationSeconds = time.Since(started).Seconds()
	drainStart := time.Now()
	deadline := time.Now().Add(10 * time.Minute)
	for {
		// Successful tasks delete their Hash; no namespace data should remain after drain.
		size, e := c.DBSize(ctx).Result()
		if e != nil {
			return e
		}
		if size == 0 {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("drain timeout with %d keys", size)
		}
		time.Sleep(time.Second)
	}
	r.DrainSeconds = time.Since(drainStart).Seconds()
	// Final live scrapes are triggered by child interrupt, then graceful shutdown joins streams.
	for _, cmd := range commands {
		cmd.Process.Signal(os.Interrupt)
	}
	readers.Wait()
	for _, cmd := range commands {
		if e := cmd.Wait(); e != nil {
			return e
		}
	}
	commands = nil
	readers.Wait()
	mu.Lock()
	r.Handled = uint64(len(seen))
	r.Duplicates = duplicates
	r.Errors = append(r.Errors, streamErrors...)
	for id := uint64(0); id < r.Accepted; id++ {
		if !seen[id] {
			r.Errors = append(r.Errors, fmt.Sprintf("missing ID %d", id))
			break
		}
	}
	for index, v := range latest {
		a, u := commandCounts(v.Metrics)
		r.CommandCount += a
		r.CommandAbove30ms += a - u
		r.Errors = append(r.Errors, operationErrors(v.Metrics)...)
		if a == 0 {
			r.Errors = append(r.Errors, fmt.Sprintf("no metrics for worker %d", index))
		}
	}
	mu.Unlock()
	rec := httptest.NewRecorder()
	p.MetricsHandler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	a, u := commandCounts(rec.Body.String())
	r.CommandCount += a
	r.CommandAbove30ms += a - u
	r.Errors = append(r.Errors, operationErrors(rec.Body.String())...)
	ins, e := inspector.New(ctx, options())
	if e != nil {
		return e
	}
	defer ins.Close()
	r.Inspector, e = ins.Check(ctx)
	if e != nil {
		return e
	}
	stats, e := c.Info(ctx, "stats").Result()
	if e != nil {
		return e
	}
	if infoInt(stats, "evicted_keys") != 0 {
		r.Errors = append(r.Errors, "Redis evicted keys")
	}
	for i := 0; i < *processes; i++ {
		baseline := 0
		for _, s := range r.Samples {
			if s.ElapsedSeconds >= 60 && s.WorkerGoroutines[i] > 0 {
				if baseline == 0 {
					baseline = s.WorkerGoroutines[i]
				}
				if s.WorkerGoroutines[i] > baseline+32 {
					r.Errors = append(r.Errors, fmt.Sprintf("worker %d goroutines grew %d -> %d", i, baseline, s.WorkerGoroutines[i]))
					break
				}
			}
		}
	}
	r.Passed = r.DurationSeconds >= 3600 && r.Groups == 1000 && r.Backlog >= 100000 && r.Processes == 10 && r.Slots == 80 && r.Handled == r.Accepted && r.Duplicates == 0 && len(r.Errors) == 0 && r.PeakRedisBytes < r.MemoryBudgetBytes*3/4 && r.CommandCount > 0 && r.CommandAbove30ms*100 < r.CommandCount && r.Inspector.Complete && len(r.Inspector.Issues) == 0
	fmt.Fprintf(os.Stderr, "finished accepted=%d handled=%d duplicate=%d errors=%d qualified_soak=%v\n", r.Accepted, r.Handled, r.Duplicates, len(r.Errors), r.Passed)
	if len(r.Errors) > 0 || r.Handled != r.Accepted || r.Duplicates != 0 || !r.Inspector.Complete || len(r.Inspector.Issues) > 0 {
		return fmt.Errorf("load correctness checks failed")
	}
	return nil
}
