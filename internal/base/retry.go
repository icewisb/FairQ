package base

import (
	"fmt"
	"time"
)

// Backoff uses this failure's one-based count. random must return a value in [0,1).
func Backoff(count int, random func() float64) (time.Duration, error) {
	if count < 1 {
		return 0, fmt.Errorf("fairq: failure count must be positive")
	}
	d := 15 * time.Second
	for k := 1; k < count && d < time.Hour; k++ {
		d *= 2
		if d > time.Hour {
			d = time.Hour
		}
	}
	r := random()
	if !(r >= 0 && r < 1) {
		return 0, fmt.Errorf("fairq: invalid jitter sample")
	}
	return d + time.Duration(r*float64(d.Milliseconds()/10))*time.Millisecond, nil
}
