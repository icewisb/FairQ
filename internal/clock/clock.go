// Package clock controls Go-side waits; Redis time is never replaced by this clock.
package clock

import "time"

type Clock interface {
	Now() time.Time
	After(time.Duration) <-chan time.Time
}
type Real struct{}

func (Real) Now() time.Time                         { return time.Now() }
func (Real) After(d time.Duration) <-chan time.Time { return time.After(d) }
