package server

import (
	"context"

	"fairq"
	"fairq/internal/rdb"
)

func (s *Server) heartbeat(ctx context.Context) {
	for s.wait(ctx, s.cfg.HeartbeatInterval) {
		items := s.executions()
		for offset := 0; offset < len(items); offset += 100 {
			end := min(offset+100, len(items))
			batch := items[offset:end]
			attempts := make([]rdb.Attempt, 0, len(batch))
			for _, e := range batch {
				if _, valid := e.proof(s.clock.Now()); !valid {
					e.abandon()
					continue
				}
				attempts = append(attempts, e.attempt)
			}
			if len(attempts) == 0 {
				continue
			}
			results, err := s.store.Heartbeat(ctx, attempts, s.cfg.LeaseDuration)
			if err != nil {
				s.logError("heartbeat", err)
				continue
			}
			byToken := make(map[string]*execution, len(batch))
			for _, e := range batch {
				byToken[e.attempt.Token] = e
			}
			for _, res := range results {
				e := byToken[res.Token]
				if res.Err == nil {
					e.extend(s.clock.Now(), res.ValidUntil)
				} else if fairq.IsCode(res.Err, fairq.StateConflict) || fairq.IsCode(res.Err, fairq.LeaseExpired) {
					e.abandon()
				} else {
					s.logError("heartbeat", res.Err)
				}
			}
		}
	}
}
