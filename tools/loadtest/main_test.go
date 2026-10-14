package main

import "testing"

func TestMetricsParsing(t *testing.T) {
	raw := `fairq_redis_command_duration_seconds_count{command="evalsha"} 200
fairq_redis_command_duration_seconds_bucket{command="evalsha",le="0.03"} 198
fairq_redis_command_duration_seconds_count{command="eval"} 10
fairq_redis_command_duration_seconds_bucket{command="eval",le="0.03"} 10
fairq_operations_total{operation="done",result="OK"} 5
fairq_operations_total{operation="dequeue",result="EMPTY"} 3
fairq_operations_total{operation="done",result="INTERNAL"} 1
`
	all, under := commandCounts(raw)
	if all != 210 || under != 208 {
		t.Fatal(all, under)
	}
	if errors := operationErrors(raw); len(errors) != 1 {
		t.Fatal(errors)
	}
	if infoInt("# Memory\r\nused_memory:123\r\n", "used_memory") != 123 {
		t.Fatal("INFO parse")
	}
}
