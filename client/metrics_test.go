package client_test

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"fairq"
	"fairq/client"
	"fairq/internal/testutil"
)

func TestMetrics(t *testing.T) {
	testutil.Redis(t, 1)
	c, err := client.New(context.Background(), client.Config{Redis: testutil.Options(t, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err = c.Enqueue(context.Background(), fairq.Task{ID: "secret-id", Type: "x", Payload: []byte("private-payload")}, fairq.EnqueueOptions{GroupKey: "private-group"}); err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRecorder()
	c.MetricsHandler().ServeHTTP(r, httptest.NewRequest("GET", "/metrics", nil))
	body := r.Body.String()
	if r.Code != 200 {
		t.Fatal(r.Code, body)
	}
	for _, s := range []string{`fairq_operations_total{operation="enqueue",result="OK"} 1`, `fairq_ready_groups 1`, `fairq_oldest_pending_wait_seconds`, `fairq_redis_command_duration_seconds_bucket`, `fairq_bad_refs 0`} {
		if !strings.Contains(body, s) {
			t.Fatal("missing metric", s, body)
		}
	}
	for _, secret := range []string{"secret-id", "private-payload", "private-group"} {
		if strings.Contains(body, secret) {
			t.Fatal("sensitive/high-cardinality metric", secret)
		}
	}
}
