package rdb

import (
	"bytes"
	"strings"
	"sync"
	"testing"
	"time"
)

type blockedMetricsWriter struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *blockedMetricsWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	return len(p), nil
}

func TestMetricsSlowWriterDoesNotBlockQueue(t *testing.T) {
	m := newMetrics()
	m.operation("dequeue", "OK")
	m.observe("evalsha", time.Millisecond)
	s := &Store{metrics: m}
	w := &blockedMetricsWriter{entered: make(chan struct{}), release: make(chan struct{})}
	written := make(chan struct{})
	go func() { defer close(written); m.write(w) }()
	updated := make(chan struct{})
	defer func() { close(w.release); <-written; <-updated }()
	<-w.entered
	go func() {
		defer close(updated)
		m.observe("evalsha", time.Millisecond)
		m.operation("dequeue", "OK")
		s.RecordMaintenance("lease", 1, 1, 0, 0)
	}()
	select {
	case <-updated:
	case <-time.After(time.Second):
		t.Fatal("slow metrics output blocked Redis/queue accounting")
	}
	var next bytes.Buffer
	m.write(&next)
	for _, want := range []string{
		`fairq_operations_total{operation="dequeue",result="OK"} 2`,
		`fairq_redis_command_duration_seconds_count{command="evalsha"} 2`,
		`fairq_maintenance_converted_total{operation="lease"} 1`,
	} {
		if !strings.Contains(next.String(), want) {
			t.Fatalf("missing updated metric %q", want)
		}
	}
}
