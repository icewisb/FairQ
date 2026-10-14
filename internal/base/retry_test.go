package base

import (
	"math"
	"testing"
	"time"
)

func TestBackoff(t *testing.T) {
	for _, tc := range []struct {
		k    int
		want time.Duration
	}{{1, 15 * time.Second}, {2, 30 * time.Second}, {9, time.Hour}, {25, time.Hour}, {26, time.Hour}, {math.MaxInt, time.Hour}} {
		d, err := Backoff(tc.k, func() float64 { return 0 })
		if err != nil || d != tc.want {
			t.Fatal(tc, d, err)
		}
		d, err = Backoff(tc.k, func() float64 { return .99999 })
		if err != nil || d < tc.want || d > tc.want+tc.want/10 {
			t.Fatal(tc, d, err)
		}
	}
	if _, err := Backoff(0, func() float64 { return 0 }); err == nil {
		t.Fatal("accepted zero")
	}
	for _, v := range []float64{-1, 1, math.NaN(), math.Inf(1)} {
		if _, err := Backoff(1, func() float64 { return v }); err == nil {
			t.Fatal("accepted invalid RNG")
		}
	}
}
