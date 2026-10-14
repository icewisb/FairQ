package rdb

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/icewisb/FairQ"
)

type Attempt struct {
	ID         string
	Group      string // Encoded Redis group.
	Message    []byte
	Token      string
	Deadline   time.Time // Redis wall time; never compare to the Go wall clock for ownership.
	ValidUntil time.Time // Conservative local monotonic bound, including request latency.
	Timeout    time.Duration
	RetryCount int
	MaxRetry   int
}

var dequeueScript = script("dequeue")

func (s *Store) Dequeue(ctx context.Context, duration time.Duration) (Attempt, error) {
	var a Attempt
	if duration <= 0 || duration%time.Millisecond != 0 {
		return a, fmt.Errorf("fairq: lease must be positive integer milliseconds")
	}
	until := time.Now().Add(duration)
	r, err := s.run(ctx, "dequeue", dequeueScript, uuid.NewString(), duration.Milliseconds())
	if err != nil {
		return a, err
	}
	if len(r) != 8 {
		return a, &fairq.Error{Code: fairq.Internal, Op: "dequeue"}
	}
	str := make([]string, 4)
	for i := range str {
		v, ok := r[i].(string)
		if !ok {
			return a, &fairq.Error{Code: fairq.Internal, Op: "dequeue"}
		}
		str[i] = v
	}
	n := make([]int64, 4)
	for i := range n {
		n[i], err = integer(r[i+4])
		if err != nil {
			return a, &fairq.Error{Code: fairq.Internal, Op: "dequeue", Err: err}
		}
	}
	a = Attempt{ID: str[0], Group: str[1], Message: []byte(str[2]), Token: str[3], Deadline: time.UnixMilli(n[0]), ValidUntil: until, Timeout: time.Duration(n[1]) * time.Millisecond, RetryCount: int(n[2]), MaxRetry: int(n[3])}
	return a, nil
}
