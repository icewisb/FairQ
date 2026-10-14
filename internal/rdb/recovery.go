package rdb

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/icewisb/FairQ"
)

var retryScript = script("retry")
var recoverScript = script("recover")
var forwardScript = script("forward")
var scanScript = script("scan")

type Transition struct {
	State      string
	RetryCount int
	At         time.Time
}
type Candidate struct {
	ID, Token, Score, Group string
	RetryCount              int
}
type Page struct {
	Next, Scanned int
	Candidates    []Candidate
}

func transition(r []any, err error) (Transition, error) {
	var tr Transition
	if err != nil {
		return tr, err
	}
	if len(r) != 3 {
		return tr, &fairq.Error{Code: fairq.Internal, Op: "transition"}
	}
	k, e1 := integer(r[1])
	at, e2 := integer(r[2])
	if e1 != nil || e2 != nil {
		return tr, &fairq.Error{Code: fairq.Internal, Op: "transition"}
	}
	tr = Transition{State: fmt.Sprint(r[0]), RetryCount: int(k), At: time.UnixMilli(at)}
	return tr, nil
}

func (s *Store) Fail(ctx context.Context, a Attempt, delay time.Duration, reason string, decode bool) (Transition, error) {
	if delay < 0 || delay%time.Millisecond != 0 {
		return Transition{}, fmt.Errorf("fairq: invalid retry delay")
	}
	mode := "error"
	if decode {
		mode = "decode"
	}
	return transition(s.run(ctx, "retry", retryScript, a.ID, a.Token, a.RetryCount, delay.Milliseconds(), reason, mode))
}
func (s *Store) Recover(ctx context.Context, c Candidate, delay time.Duration) (Transition, error) {
	if delay < 0 || delay%time.Millisecond != 0 {
		return Transition{}, fmt.Errorf("fairq: invalid recovery delay")
	}
	return transition(s.run(ctx, "recover", recoverScript, c.ID, c.Token, c.RetryCount, c.Score, delay.Milliseconds()))
}
func (s *Store) Forward(ctx context.Context, c Candidate) error {
	_, err := s.run(ctx, "forward", forwardScript, c.ID, c.Token, c.RetryCount, c.Score, c.Group)
	return err
}
func (s *Store) Scan(ctx context.Context, kind string, offset, limit int, retention time.Duration) (Page, error) {
	var page Page
	r, err := s.run(ctx, "scan", scanScript, kind, offset, limit, retention.Milliseconds())
	if err != nil {
		return page, err
	}
	if len(r) != 3 {
		return page, &fairq.Error{Code: fairq.Internal, Op: "scan"}
	}
	n, e1 := integer(r[0])
	sc, e2 := integer(r[1])
	rows, ok := r[2].([]any)
	if e1 != nil || e2 != nil || !ok {
		return page, &fairq.Error{Code: fairq.Internal, Op: "scan"}
	}
	page.Next = int(n)
	page.Scanned = int(sc)
	for _, v := range rows {
		row, ok := v.([]any)
		if !ok || len(row) != 5 {
			return page, &fairq.Error{Code: fairq.Internal, Op: "scan"}
		}
		count, err := strconv.Atoi(fmt.Sprint(row[2]))
		if err != nil {
			count = -1
		}
		page.Candidates = append(page.Candidates, Candidate{ID: fmt.Sprint(row[0]), Token: fmt.Sprint(row[1]), RetryCount: count, Score: fmt.Sprint(row[3]), Group: fmt.Sprint(row[4])})
	}
	return page, nil
}
