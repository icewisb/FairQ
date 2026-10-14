package rdb

import (
	"context"
	"fmt"
	"time"

	"github.com/icewisb/FairQ"
)

var heartbeatScript = script("heartbeat")
var doneScript = script("done")

type LeaseResult struct {
	ID         string
	Token      string
	Deadline   time.Time
	ValidUntil time.Time
	Err        error
}

func (s *Store) Heartbeat(ctx context.Context, attempts []Attempt, duration time.Duration) ([]LeaseResult, error) {
	if len(attempts) > 100 || duration <= 0 || duration%time.Millisecond != 0 {
		return nil, fmt.Errorf("fairq: invalid heartbeat batch or duration")
	}
	args := []any{duration.Milliseconds()}
	for _, a := range attempts {
		args = append(args, a.ID, a.Token)
	}
	until := time.Now().Add(duration)
	r, err := s.run(ctx, "heartbeat", heartbeatScript, args...)
	if err != nil {
		return nil, err
	}
	if len(r) != len(attempts) {
		return nil, &fairq.Error{Code: fairq.Internal, Op: "heartbeat"}
	}
	results := make([]LeaseResult, len(r))
	for i, v := range r {
		row, ok := v.([]any)
		if !ok || len(row) != 3 || row[0] != attempts[i].ID {
			return nil, &fairq.Error{Code: fairq.Internal, Op: "heartbeat"}
		}
		res := LeaseResult{ID: attempts[i].ID, Token: attempts[i].Token}
		if row[1] != "OK" {
			res.Err = &fairq.Error{Code: fairq.Code(fmt.Sprint(row[1])), Op: "heartbeat"}
		} else {
			deadline, err := integer(row[2])
			if err != nil {
				return nil, &fairq.Error{Code: fairq.Internal, Op: "heartbeat", Err: err}
			}
			res.Deadline = time.UnixMilli(deadline)
			res.ValidUntil = until
		}
		results[i] = res
	}
	return results, nil
}

func (s *Store) Done(ctx context.Context, a Attempt) error {
	_, err := s.run(ctx, "done", doneScript, a.ID, a.Token)
	return err
}
