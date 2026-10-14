package main

import (
	"testing"
	"time"
)

func TestPercentiles(t *testing.T) {
	values := make([]time.Duration, 100)
	for i := range values {
		values[i] = time.Duration(100 - i)
	}
	if percentile(values) != 99 {
		t.Fatal("p99")
	}
	raw := []any{"evalsha", []any{"calls", int64(100), "histogram_usec", []any{int64(1), int64(98), int64(4), int64(99), int64(8), int64(100)}}}
	if n, e := luaP99(raw); e != nil || n != 4 {
		t.Fatal(n, e)
	}
	if _, e := luaP99(nil); e == nil {
		t.Fatal("accepted absent histogram")
	}
}
