package rdb

import (
	"context"
	"time"
)

var cleanScript = script("clean")

func (s *Store) Clean(ctx context.Context, kind string, c Candidate, retention time.Duration) error {
	_, err := s.run(ctx, "clean", cleanScript, c.ID, kind, c.Token, c.Score, retention.Milliseconds())
	return err
}
