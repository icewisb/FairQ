// benchmark measures isolated Dequeue Lua and a synthetic 100,000-Lease recovery burst.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/icewisb/FairQ"
	"github.com/icewisb/FairQ/client"
	"github.com/icewisb/FairQ/inspector"
	"github.com/icewisb/FairQ/internal/rdb"
	"github.com/redis/go-redis/v9"
)

type result struct {
	Started                                                   time.Time
	Tasks, Groups, PayloadBytes                               int
	DequeueSeconds, DequeueCommandP99MS, DequeueLuaP99UpperMS float64
	RecoverySeconds, RecoveryCommandP99MS                     float64
	Scanned, Converted, Ticks, MaxCandidatesPerTick           int
	PeakRedisBytes, MemoryBudgetBytes                         int64
	RedisVersion                                              string
	DequeueHistogram                                          any
	Inspector                                                 inspector.Report
	Errors                                                    []string
	Passed                                                    bool
}

func percentile(values []time.Duration) time.Duration {
	if len(values) == 0 {
		return 0
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return values[(len(values)*99+99)/100-1]
}
func luaP99(raw []any) (int64, error) {
	if len(raw) != 2 {
		return 0, fmt.Errorf("invalid histogram %v", raw)
	}
	fields, ok := raw[1].([]any)
	if !ok {
		return 0, fmt.Errorf("invalid histogram fields")
	}
	var total int64
	var bins []any
	for i := 0; i+1 < len(fields); i += 2 {
		switch fields[i] {
		case "calls":
			total, _ = fields[i+1].(int64)
		case "histogram_usec":
			bins, _ = fields[i+1].([]any)
		}
	}
	if total == 0 || len(bins) == 0 {
		return 0, fmt.Errorf("empty histogram")
	}
	for i := 0; i+1 < len(bins); i += 2 {
		bound, b := bins[i].(int64)
		count, c := bins[i+1].(int64)
		if !b || !c {
			return 0, fmt.Errorf("invalid histogram bin")
		}
		if count*100 >= total*99 {
			return bound, nil
		}
	}
	return 0, fmt.Errorf("histogram incomplete")
}
func infoNumber(raw, key string) int64 {
	for _, l := range strings.Split(raw, "\n") {
		if strings.HasPrefix(l, key+":") {
			n, _ := strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(l, key+":")), 10, 64)
			return n
		}
	}
	return 0
}
func main() {
	addr := flag.String("redis", "", "isolated Redis, DB5 must be empty")
	out := flag.String("report", "", "JSON output path")
	flag.Parse()
	if *addr == "" || *out == "" {
		log.Fatal("-redis and -report required")
	}
	if err := run(*addr, *out); err != nil {
		log.Fatal(err)
	}
}
func run(addr, path string) (err error) {
	ctx := context.Background()
	opts := redis.UniversalOptions{Addrs: []string{addr}, DB: 5, Protocol: 2, MaxRetries: -1, ContextTimeoutEnabled: true}
	s, err := rdb.New(ctx, opts)
	if err != nil {
		return err
	}
	defer s.Close()
	if size, e := s.Redis.DBSize(ctx).Result(); e != nil || size != 0 {
		return fmt.Errorf("requires empty DB5: size=%d err=%v", size, e)
	}
	r := result{Started: time.Now().UTC(), Tasks: 100000, Groups: 1000, PayloadBytes: 256}
	defer func() {
		if err != nil {
			r.Errors = append(r.Errors, err.Error())
		}
		data, e := json.MarshalIndent(r, "", "  ")
		if e == nil {
			e = os.WriteFile(path, append(data, '\n'), 0600)
		}
		if e != nil {
			err = e
		}
	}()
	memory, err := s.Redis.Info(ctx, "memory").Result()
	if err != nil {
		return err
	}
	r.MemoryBudgetBytes = infoNumber(memory, "maxmemory")
	if r.MemoryBudgetBytes <= 0 || !strings.Contains(memory, "maxmemory_policy:noeviction") {
		return fmt.Errorf("requires explicit maxmemory and noeviction")
	}
	version, err := s.Redis.Info(ctx, "server").Result()
	if err != nil {
		return err
	}
	for _, line := range strings.Split(version, "\n") {
		if strings.HasPrefix(line, "redis_version:") {
			r.RedisVersion = strings.TrimSpace(strings.TrimPrefix(line, "redis_version:"))
		}
	}
	p, err := client.New(ctx, client.Config{Redis: opts})
	if err != nil {
		return err
	}
	defer p.Close()
	payload := []byte(strings.Repeat("x", r.PayloadBytes))
	for i := 0; i < r.Tasks; i++ {
		if _, err = p.Enqueue(ctx, fairq.Task{ID: fmt.Sprint(i), Type: "perf", Payload: payload}, fairq.EnqueueOptions{GroupKey: fmt.Sprint(i % r.Groups)}); err != nil {
			return err
		}
	}
	// Warm script cache; reset stats on this isolated instance before Dequeue-only sampling.
	if _, err = s.Dequeue(ctx, time.Hour); err != nil {
		return err
	}
	if err = s.Redis.ConfigResetStat(ctx).Err(); err != nil {
		return err
	}
	times := make([]time.Duration, 0, r.Tasks)
	started := time.Now()
	for i := 1; i < r.Tasks; i++ {
		at := time.Now()
		_, err = s.Dequeue(ctx, time.Hour)
		times = append(times, time.Since(at))
		if err != nil {
			return err
		}
	}
	r.DequeueSeconds = time.Since(started).Seconds()
	r.DequeueCommandP99MS = float64(percentile(times)) / float64(time.Millisecond)
	hist, err := s.Redis.Do(ctx, "LATENCY", "HISTOGRAM", "evalsha").Slice()
	if err != nil {
		return err
	}
	r.DequeueHistogram = hist
	bound, err := luaP99(hist)
	if err != nil {
		return err
	}
	r.DequeueLuaP99UpperMS = float64(bound) / 1000
	fmt.Printf("Dequeue 99999: seconds=%.3f API_p99_ms=%.3f Lua_p99_upper_ms=%.3f\n", r.DequeueSeconds, r.DequeueCommandP99MS, r.DequeueLuaP99UpperMS)
	// This intentionally exceeds the normal 800-slot scale; fixture changes only Lease deadlines.
	for i := 0; i < r.Tasks; i += 100 {
		entries := make([]redis.Z, 100)
		for j := range entries {
			entries[j] = redis.Z{Member: fmt.Sprint(i + j), Score: 0}
		}
		if err = s.Redis.ZAdd(ctx, rdb.Lease, entries...).Err(); err != nil {
			return err
		}
	}
	times = times[:0]
	started = time.Now()
	cursor := 0
	for r.Converted < r.Tasks {
		scanned := 0
		r.Ticks++
		for batch := 0; batch < 5; batch++ {
			at := time.Now()
			page, e := s.Scan(ctx, "lease", cursor, 100, 0)
			times = append(times, time.Since(at))
			if e != nil {
				return e
			}
			cursor = page.Next
			scanned += page.Scanned
			for _, candidate := range page.Candidates {
				at = time.Now()
				_, e = s.Recover(ctx, candidate, 15*time.Second)
				times = append(times, time.Since(at))
				if e != nil {
					return e
				}
				r.Converted++
			}
			if page.Next == 0 || page.Scanned == 0 {
				break
			}
			if batch < 4 {
				time.Sleep(10 * time.Millisecond)
			}
		}
		r.Scanned += scanned
		r.MaxCandidatesPerTick = max(r.MaxCandidatesPerTick, scanned)
		if scanned == 0 && r.Converted < r.Tasks {
			return fmt.Errorf("recovery stalled at %d", r.Converted)
		}
	}
	r.RecoverySeconds = time.Since(started).Seconds()
	r.RecoveryCommandP99MS = float64(percentile(times)) / float64(time.Millisecond)
	memory, err = s.Redis.Info(ctx, "memory").Result()
	if err != nil {
		return err
	}
	r.PeakRedisBytes = infoNumber(memory, "used_memory_peak")
	ins, err := inspector.New(ctx, opts)
	if err != nil {
		return err
	}
	defer ins.Close()
	r.Inspector, err = ins.Check(ctx)
	if err != nil {
		return err
	}
	if s.Redis.ZCard(ctx, rdb.Retry).Val() != int64(r.Tasks) || s.Redis.SCard(ctx, rdb.Active).Val() != 0 || s.Redis.ZCard(ctx, rdb.Lease).Val() != 0 {
		return fmt.Errorf("recovery counts do not conserve tasks")
	}
	stats, err := s.Redis.Info(ctx, "stats").Result()
	if err != nil {
		return err
	}
	if infoNumber(stats, "evicted_keys") != 0 {
		return fmt.Errorf("unexpected eviction")
	}
	r.Passed = r.DequeueLuaP99UpperMS < 10 && r.DequeueCommandP99MS < 30 && r.RecoveryCommandP99MS < 30 && r.PeakRedisBytes < r.MemoryBudgetBytes*3/4 && r.MaxCandidatesPerTick <= 500 && r.Converted == r.Tasks && r.Inspector.Complete && r.Inspector.CheckedTasks == r.Tasks && len(r.Inspector.Issues) == 0
	fmt.Printf("Recovery: tasks=%d scanned=%d ticks=%d max_tick=%d seconds=%.3f API_p99_ms=%.3f memory=%d passed=%v\n", r.Converted, r.Scanned, r.Ticks, r.MaxCandidatesPerTick, r.RecoverySeconds, r.RecoveryCommandP99MS, r.PeakRedisBytes, r.Passed)
	if !r.Passed {
		return fmt.Errorf("performance acceptance failed; thresholds unchanged")
	}
	return nil
}
