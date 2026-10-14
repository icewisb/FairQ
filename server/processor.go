package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"fairq"
	"fairq/internal/base"
	"fairq/internal/rdb"
)

type execution struct {
	attempt rdb.Attempt
	ctx     context.Context
	cancel  context.CancelFunc
	mu      sync.Mutex
	until   time.Time
	lost    bool
	renewed chan struct{}
}

func (e *execution) abandon() { e.mu.Lock(); e.lost = true; e.cancel(); e.mu.Unlock() }
func (e *execution) proof(now time.Time) (time.Time, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.until, !e.lost && now.Before(e.until)
}
func (e *execution) extend(now, until time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.lost {
		return
	}
	if !now.Before(e.until) || !now.Before(until) {
		e.lost = true
		e.cancel()
		return
	}
	e.until = until
	select {
	case e.renewed <- struct{}{}:
	default:
	}
}

func corruptDelay(n int) time.Duration {
	if n < 10 {
		return 0
	}
	shift := n - 10
	if shift > 4 {
		shift = 4
	}
	d := 100 * time.Millisecond * (1 << shift)
	if d > time.Second {
		return time.Second
	}
	return d
}

func (s *Server) worker(ctx context.Context, handler fairq.Handler) {
	corrupt := 0
	for ctx.Err() == nil {
		a, err := s.store.Dequeue(ctx, s.cfg.LeaseDuration)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			delay := s.cfg.PollInterval
			switch {
			case fairq.IsCode(err, fairq.CorruptReference):
				corrupt++
				delay = corruptDelay(corrupt)
			case fairq.IsCode(err, fairq.Empty):
			default:
				delay = time.Second
				s.logError("dequeue", err)
			}
			if delay > 0 && !s.wait(ctx, delay) {
				return
			}
			continue
		}
		corrupt = 0
		if ctx.Err() != nil {
			return
		}
		if !s.clock.Now().Before(a.ValidUntil) {
			continue
		}
		s.process(a, handler)
	}
}

func (s *Server) process(a rdb.Attempt, handler fairq.Handler) {
	ctx, cancel := context.WithCancel(context.Background())
	e := &execution{attempt: a, ctx: ctx, cancel: cancel, until: a.ValidUntil, renewed: make(chan struct{}, 1)}
	s.mu.Lock()
	if s.state != 1 {
		s.mu.Unlock()
		cancel()
		return
	}
	s.active[a.Token] = e
	s.mu.Unlock()
	defer func() { e.abandon(); s.mu.Lock(); delete(s.active, a.Token); s.mu.Unlock() }()
	msg, err := base.Decode(a.Message)
	if err != nil {
		s.finish(e, nil, true)
		return
	}
	handlerCtx := ctx
	handlerCancel := func() {}
	if a.Timeout > 0 {
		handlerCtx, handlerCancel = context.WithTimeout(ctx, a.Timeout)
	}
	defer handlerCancel()
	result := make(chan error, 1)
	go func() {
		result <- perform(handlerCtx, handler, fairq.Task{ID: a.ID, Type: msg.Type, Payload: msg.Payload})
	}()
	timer := time.NewTimer(time.Until(a.ValidUntil))
	defer timer.Stop()
	for {
		select {
		case err := <-result:
			s.finish(e, err, false)
			return
		case <-handlerCtx.Done():
			if ctx.Err() == nil {
				s.finish(e, handlerCtx.Err(), false)
			}
			return
		case <-timer.C:
			until, valid := e.proof(s.clock.Now())
			if !valid {
				e.abandon()
				return
			}
			timer.Reset(time.Until(until))
		case <-e.renewed:
			until, valid := e.proof(s.clock.Now())
			if !valid {
				e.abandon()
				return
			}
			timer.Reset(time.Until(until))
		}
	}
}

func perform(ctx context.Context, handler fairq.Handler, task fairq.Task) (err error) {
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("handler panic")
		}
	}()
	return handler(ctx, task)
}

func (s *Server) finish(e *execution, handlerErr error, decode bool) {
	until, valid := e.proof(s.clock.Now())
	if !valid || e.ctx.Err() != nil {
		return
	}
	ctx, cancel := context.WithDeadline(e.ctx, until)
	defer cancel()
	var err error
	if handlerErr == nil && !decode {
		err = s.store.Done(ctx, e.attempt)
	} else {
		var delay time.Duration
		if !decode {
			delay, err = base.Backoff(e.attempt.RetryCount+1, s.random)
		}
		if err == nil {
			reason := "DECODE_FAILED"
			if handlerErr != nil {
				reason = handlerErr.Error()
			}
			_, err = s.store.Fail(ctx, e.attempt, delay, reason, decode)
		}
	}
	if err != nil {
		s.cfg.Logger.Error("task state submission failed", "task_id", e.attempt.ID, "group", e.attempt.Group, "attempt", e.attempt.Token[:min(8, len(e.attempt.Token))], "error", err)
	}
}
