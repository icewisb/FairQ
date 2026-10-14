// Package server runs FairQ handlers and the lease/retry maintenance loops.
package server

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"

	"github.com/icewisb/FairQ"
	"github.com/icewisb/FairQ/internal/clock"
	"github.com/icewisb/FairQ/internal/rdb"
)

type Server struct {
	store                        *rdb.Store
	cfg                          Config
	clock                        clock.Clock
	random                       func() float64
	mu                           sync.Mutex
	state                        int                   // 0=new, 1=running, 2=closing, 3=closed; local lifecycle only.
	active                       map[string]*execution // keyed by attempt token, not reusable task ID
	pollCancel, backgroundCancel context.CancelFunc
	workers, background          sync.WaitGroup
	closed                       chan struct{}
	logMu                        sync.Mutex
	lastError                    time.Time
}

func (s *Server) MetricsHandler() http.Handler { return s.store.MetricsHandler() }

func New(ctx context.Context, cfg Config) (*Server, error) {
	if err := cfg.defaults(); err != nil {
		return nil, err
	}
	s, err := rdb.New(ctx, cfg.Redis)
	if err != nil {
		return nil, err
	}
	return &Server{store: s, cfg: cfg, clock: clock.Real{}, random: rand.Float64, active: make(map[string]*execution), closed: make(chan struct{})}, nil
}

func (s *Server) Start(handler fairq.Handler) error {
	if handler == nil {
		return fmt.Errorf("fairq: handler is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != 0 {
		return fmt.Errorf("fairq: server already started or closed")
	}
	s.state = 1
	poll, pollCancel := context.WithCancel(context.Background())
	bg, bgCancel := context.WithCancel(context.Background())
	s.pollCancel = pollCancel
	s.backgroundCancel = bgCancel
	s.background.Add(4)
	go func() { defer s.background.Done(); s.heartbeat(bg) }()
	go func() { defer s.background.Done(); s.cleaner(bg) }()
	go func() { defer s.background.Done(); s.maintain(bg, "lease", s.cfg.RecoverInterval) }()
	go func() { defer s.background.Done(); s.maintain(bg, "retry", s.cfg.ForwarderInterval) }()
	s.workers.Add(s.cfg.Concurrency)
	for i := 0; i < s.cfg.Concurrency; i++ {
		go func() { defer s.workers.Done(); s.worker(poll, handler) }()
	}
	return nil
}

func (s *Server) Run(ctx context.Context, handler fairq.Handler) error {
	if err := s.Start(handler); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		s.Shutdown()
	case <-s.closed:
	}
	return nil
}

func (s *Server) Shutdown() {
	s.mu.Lock()
	if s.state >= 2 {
		s.mu.Unlock()
		<-s.closed
		return
	}
	if s.state == 0 {
		s.state = 3
		s.store.Close()
		close(s.closed)
		s.mu.Unlock()
		return
	}
	s.state = 2
	s.pollCancel()
	s.mu.Unlock()
	done := make(chan struct{})
	go func() { s.workers.Wait(); close(done) }()
	select {
	case <-done:
	case <-s.clock.After(s.cfg.ShutdownTimeout):
		for _, e := range s.executions() {
			e.abandon()
		}
		<-done
	}
	s.backgroundCancel()
	s.background.Wait()
	s.store.Close()
	s.mu.Lock()
	s.state = 3
	close(s.closed)
	s.mu.Unlock()
}

func (s *Server) executions() []*execution {
	s.mu.Lock()
	defer s.mu.Unlock()
	items := make([]*execution, 0, len(s.active))
	for _, e := range s.active {
		items = append(items, e)
	}
	return items
}
func (s *Server) wait(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-s.clock.After(d):
		return true
	}
}
func (s *Server) logError(op string, err error) {
	s.logMu.Lock()
	defer s.logMu.Unlock()
	now := s.clock.Now()
	if now.Sub(s.lastError) >= time.Second {
		s.lastError = now
		s.cfg.Logger.Error("queue operation failed", "operation", op, "error", err)
	}
}
func (s *Server) maintain(ctx context.Context, kind string, interval time.Duration) {
	m := &maintenance{store: s.store, kind: kind, clock: s.clock, random: s.random, batchSize: s.cfg.BatchSize, maxBatches: s.cfg.MaxBatchesPerTick, pause: s.cfg.BatchPause, logger: s.cfg.Logger}
	if !s.wait(ctx, time.Duration(s.random()*float64(interval))) {
		return
	}
	for {
		m.tick(ctx)
		if !s.wait(ctx, interval) {
			return
		}
	}
}
