package rdb

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var latencyBounds = []float64{.0005, .001, .0025, .005, .01, .025, .03, .05, .1, .25, .5, 1, 3, 10}

type histogram struct {
	Buckets []uint64
	Count   uint64
	Sum     float64
}
type metrics struct {
	mu          sync.Mutex
	operations  map[string]uint64
	commands    map[string]*histogram
	maintenance map[string][4]uint64
}

func newMetrics() *metrics {
	return &metrics{operations: map[string]uint64{}, commands: map[string]*histogram{}, maintenance: map[string][4]uint64{}}
}
func (m *metrics) operation(op, result string) {
	m.mu.Lock()
	m.operations[op+"/"+result]++
	m.mu.Unlock()
}
func (m *metrics) observe(command string, d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	h := m.commands[command]
	if h == nil {
		h = &histogram{Buckets: make([]uint64, len(latencyBounds)+1)}
		m.commands[command] = h
	}
	sec := d.Seconds()
	bucket := sort.SearchFloat64s(latencyBounds, sec)
	h.Buckets[bucket]++
	h.Count++
	h.Sum += sec
}

type metricsHook struct{ m *metrics }

func (h metricsHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}
func (h metricsHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		start := time.Now()
		err := next(ctx, cmd)
		h.m.observe(cmd.Name(), time.Since(start))
		return err
	}
}
func (h metricsHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		start := time.Now()
		err := next(ctx, cmds)
		h.m.observe("pipeline", time.Since(start))
		return err
	}
}

func (s *Store) RecordMaintenance(kind string, scanned, converted, isolated, failed int) {
	s.metrics.mu.Lock()
	defer s.metrics.mu.Unlock()
	v := s.metrics.maintenance[kind]
	for i, n := range []int{scanned, converted, isolated, failed} {
		v[i] += uint64(n)
	}
	s.metrics.maintenance[kind] = v
}

func (m *metrics) write(w io.Writer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	fmt.Fprintln(w, "# TYPE fairq_operations_total counter")
	for key, n := range m.operations {
		parts := strings.SplitN(key, "/", 2)
		fmt.Fprintf(w, "fairq_operations_total{operation=%q,result=%q} %d\n", parts[0], parts[1], n)
	}
	fmt.Fprintln(w, "# TYPE fairq_redis_command_duration_seconds histogram")
	for cmd, h := range m.commands {
		var cumulative uint64
		for i, n := range h.Buckets {
			cumulative += n
			bound := "+Inf"
			if i < len(latencyBounds) {
				bound = strconv.FormatFloat(latencyBounds[i], 'g', -1, 64)
			}
			fmt.Fprintf(w, "fairq_redis_command_duration_seconds_bucket{command=%q,le=%q} %d\n", cmd, bound, cumulative)
		}
		fmt.Fprintf(w, "fairq_redis_command_duration_seconds_sum{command=%q} %g\nfairq_redis_command_duration_seconds_count{command=%q} %d\n", cmd, h.Sum, cmd, h.Count)
	}
	for n, field := range []string{"scanned", "converted", "isolated", "failed"} {
		fmt.Fprintf(w, "# TYPE fairq_maintenance_%s_total counter\n", field)
		for kind, values := range m.maintenance {
			fmt.Fprintf(w, "fairq_maintenance_%s_total{operation=%q} %d\n", field, kind, values[n])
		}
	}
}

// MetricsHandler can be mounted on the application's HTTP server; it starts no listener.
func (s *Store) MetricsHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		gauges, err := s.Gauges(ctx)
		if err != nil {
			http.Error(w, "Redis metrics unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		s.metrics.write(w)
		for name, value := range gauges {
			fmt.Fprintf(w, "# TYPE fairq_%s gauge\nfairq_%s %g\n", name, name, value)
		}
	})
}

// Gauges uses batched reads. Values observed during concurrent writes are not a snapshot.
func (s *Store) Gauges(ctx context.Context) (map[string]float64, error) {
	p := s.Redis.Pipeline()
	active := p.SCard(ctx, Active)
	lease := p.ZCard(ctx, Lease)
	retry := p.ZCard(ctx, Retry)
	arch := p.ZCard(ctx, Archived)
	completed := p.ZCard(ctx, Completed)
	ready := p.LLen(ctx, Ready)
	bad := p.LLen(ctx, BadRefs)
	if _, err := p.Exec(ctx); err != nil {
		return nil, err
	}
	g := map[string]float64{"active": float64(active.Val()), "lease": float64(lease.Val()), "retry": float64(retry.Val()), "archived": float64(arch.Val()), "completed": float64(completed.Val()), "ready_groups": float64(ready.Val()), "bad_refs": float64(bad.Val())}
	var cursor uint64
	oldest := int64(0)
	seen := map[string]bool{}
	for {
		keys, next, err := s.Redis.Scan(ctx, cursor, Prefix+"pending:*", 100).Result()
		if err != nil {
			return nil, err
		}
		pipe := s.Redis.Pipeline()
		tails := map[string]*redis.StringCmd{}
		for _, key := range keys {
			if !seen[key] {
				seen[key] = true
				tails[key] = pipe.LIndex(ctx, key, -1)
			}
		}
		if _, err = pipe.Exec(ctx); err != nil && err != redis.Nil {
			return nil, err
		}
		pipe = s.Redis.Pipeline()
		times := []*redis.StringCmd{}
		for _, tail := range tails {
			if tail.Err() == redis.Nil {
				continue
			}
			if tail.Err() != nil {
				return nil, tail.Err()
			}
			times = append(times, pipe.HGet(ctx, TaskKey(tail.Val()), "pending_since"))
		}
		if _, err = pipe.Exec(ctx); err != nil && err != redis.Nil {
			return nil, err
		}
		for _, at := range times {
			if at.Err() == redis.Nil {
				continue
			}
			if at.Err() != nil {
				return nil, at.Err()
			}
			n, err := strconv.ParseInt(at.Val(), 10, 64)
			if err != nil {
				return nil, err
			}
			if oldest == 0 || n < oldest {
				oldest = n
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	now, err := s.Redis.Time(ctx).Result()
	if err != nil {
		return nil, err
	}
	wait := float64(0)
	if oldest != 0 {
		wait = max(0, float64(now.UnixMilli()-oldest)/1000)
	}
	g["oldest_pending_wait_seconds"] = wait
	for name, key := range map[string]string{"lease": Lease, "retry": Retry} {
		first, err := s.Redis.ZRangeWithScores(ctx, key, 0, 0).Result()
		if err != nil {
			return nil, err
		}
		age := float64(0)
		if len(first) > 0 {
			age = max(0, (float64(now.UnixMilli())-first[0].Score)/1000)
		}
		g[name+"_overdue_seconds"] = age
	}
	return g, nil
}
