package acceptance_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"fairq"
	"fairq/client"
	"fairq/inspector"
	"fairq/internal/rdb"
	"github.com/redis/go-redis/v9"
)

type node struct {
	t              *testing.T
	dir, bin, addr string
	port           int
	cmd            *exec.Cmd
	args           []string
	log            *os.File
}

func newNode(t *testing.T, extra ...string) *node {
	t.Helper()
	binary := os.Getenv("FAIRQ_REDIS_SERVER")
	if binary == "" {
		var err error
		binary, err = exec.LookPath("redis-server")
		if err != nil {
			t.Fatal("FAIRQ_REDIS_SERVER must name a real Redis binary for infrastructure acceptance")
		}
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	n := &node{t: t, dir: t.TempDir(), bin: binary, port: port, addr: fmt.Sprintf("127.0.0.1:%d", port)}
	n.args = []string{"--bind", "127.0.0.1", "--port", strconv.Itoa(port), "--dir", n.dir, "--save", "", "--appendonly", "yes", "--appendfsync", "always", "--maxmemory", "536870912", "--maxmemory-policy", "noeviction"}
	n.args = append(n.args, extra...)
	t.Cleanup(n.stop)
	return n
}
func (n *node) start() {
	n.t.Helper()
	var err error
	n.log, err = os.OpenFile(filepath.Join(n.dir, "process.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		n.t.Fatal(err)
	}
	n.cmd = exec.Command(n.bin, n.args...)
	n.cmd.Stdout = n.log
	n.cmd.Stderr = n.log
	if err = n.cmd.Start(); err != nil {
		n.t.Fatal(err)
	}
	c := redis.NewClient(&redis.Options{Addr: n.addr, MaxRetries: -1, DialTimeout: 100 * time.Millisecond, ReadTimeout: 100 * time.Millisecond})
	defer c.Close()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if c.Ping(context.Background()).Err() == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	data, _ := os.ReadFile(filepath.Join(n.dir, "process.log"))
	n.t.Fatalf("Redis failed startup: %s", data)
}
func (n *node) stop() {
	if n.cmd != nil {
		if n.cmd.Process != nil {
			n.cmd.Process.Kill()
			n.cmd.Wait()
		}
		n.cmd = nil
	}
	if n.log != nil {
		n.log.Close()
		n.log = nil
	}
}

func TestNodeCleanupAfterStartFailure(t *testing.T) {
	n := &node{cmd: exec.Command(filepath.Join(t.TempDir(), "missing-redis"))}
	t.Cleanup(n.stop)
	if err := n.cmd.Start(); err == nil {
		t.Fatal("unexpected process start")
	}
	n.stop()
	if n.cmd != nil {
		t.Fatal("failed command retained")
	}
}
func (n *node) options() redis.UniversalOptions {
	return redis.UniversalOptions{Addrs: []string{n.addr}, MaxRetries: -1, ContextTimeoutEnabled: true}
}
func healthy(t *testing.T, opts redis.UniversalOptions, expected int) {
	t.Helper()
	i, err := inspector.New(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer i.Close()
	r, err := i.Check(context.Background())
	if err != nil || !r.Complete || len(r.Issues) != 0 || r.CheckedTasks != expected {
		t.Fatal(r, err)
	}
}
func seedTasks(t *testing.T, opts redis.UniversalOptions, count int) {
	t.Helper()
	p, err := client.New(context.Background(), client.Config{Redis: opts})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	for j := 0; j < count; j++ {
		if _, err = p.Enqueue(context.Background(), fairq.Task{ID: fmt.Sprint(j), Type: "fixture"}, fairq.EnqueueOptions{GroupKey: fmt.Sprint(j % 10)}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestAOFRestartAndBackupRestore(t *testing.T) {
	n := newNode(t)
	n.start()
	ctx := context.Background()
	seedTasks(t, n.options(), 100)
	s, err := rdb.New(ctx, n.options())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for j := 0; j < 20; j++ {
		a, e := s.Dequeue(ctx, time.Minute)
		if e != nil {
			t.Fatal(e)
		}
		switch {
		case j < 5:
			_, e = s.Fail(ctx, a, 15*time.Second, "fixture", false)
		case j < 10:
			_, e = s.Fail(ctx, a, 0, "", true)
		case j < 15:
			e = s.Done(ctx, a)
		}
		if e != nil {
			t.Fatal(e)
		}
	}
	before := []string{}
	var cursor uint64
	for {
		keys, next, e := s.Redis.Scan(ctx, cursor, "fairq:v1:task:*", 100).Result()
		if e != nil {
			t.Fatal(e)
		}
		before = append(before, keys...)
		cursor = next
		if cursor == 0 {
			break
		}
	}

	if len(before) != 95 {
		t.Fatal(len(before))
	}
	s.Close()
	healthy(t, n.options(), 95)
	started := time.Now()
	n.stop()
	n.start()
	healthy(t, n.options(), 95)
	s, err = rdb.New(ctx, n.options())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, key := range before {
		if s.Redis.Exists(ctx, key).Val() != 1 {
			t.Fatalf("AOF lost %s", key)
		}
	}
	t.Logf("AOF kill/restart: confirmed_hashes=95 lost=0 observed_RPO_tasks=0 RTO=%s appendfsync=always", time.Since(started))
	if err = s.Redis.Save(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	restore := newNode(t, "--appendonly", "no")
	data, err := os.ReadFile(filepath.Join(n.dir, "dump.rdb"))
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(restore.dir, "dump.rdb"), data, 0600); err != nil {
		t.Fatal(err)
	}
	started = time.Now()
	restore.start()
	healthy(t, restore.options(), 95)
	t.Logf("RDB backup restore: snapshot_hashes=95 lost=0 RTO=%s bytes=%d", time.Since(started), len(data))
}

func TestSentinelFailover(t *testing.T) {
	master := newNode(t)
	master.start()
	replicas := []*node{newNode(t, "--replicaof", "127.0.0.1", strconv.Itoa(master.port)), newNode(t, "--replicaof", "127.0.0.1", strconv.Itoa(master.port))}
	for _, n := range replicas {
		n.start()
	}
	var sentinels []*node
	for j := 0; j < 3; j++ {
		n := newNode(t)
		conf := fmt.Sprintf("port %d\nbind 127.0.0.1\ndir %s\nsentinel monitor fairq-test 127.0.0.1 %d 2\nsentinel down-after-milliseconds fairq-test 500\nsentinel failover-timeout fairq-test 5000\nsentinel parallel-syncs fairq-test 1\n", n.port, n.dir, master.port)
		path := filepath.Join(n.dir, "sentinel.conf")
		if err := os.WriteFile(path, []byte(conf), 0600); err != nil {
			t.Fatal(err)
		}
		n.args = []string{path, "--sentinel"}
		n.start()
		sentinels = append(sentinels, n)
	}
	ctx := context.Background()
	opts := master.options()
	direct := redis.NewClient(opts.Simple())
	defer direct.Close()
	deadline := time.Now().Add(15 * time.Second)
	for {
		info, err := direct.Info(ctx, "replication").Result()
		if err == nil && strings.Contains(info, "connected_slaves:2") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("replicas not connected", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	failover := redis.UniversalOptions{MasterName: "fairq-test", MaxRetries: -1, ContextTimeoutEnabled: true, DialTimeout: 200 * time.Millisecond, ReadTimeout: 200 * time.Millisecond, WriteTimeout: 200 * time.Millisecond}
	for _, n := range sentinels {
		failover.Addrs = append(failover.Addrs, n.addr)
	}
	producer, err := client.New(ctx, client.Config{Redis: failover})
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	for j := 0; j < 100; j++ {
		if _, err = producer.Enqueue(ctx, fairq.Task{ID: fmt.Sprint(j), Type: "sentinel"}, fairq.EnqueueOptions{GroupKey: "g"}); err != nil {
			t.Fatal(err)
		}
	}
	// Prove the actual last acknowledged write reached each replica before this controlled failover.
	for _, replica := range replicas {
		o := replica.options()
		rc := redis.NewClient(o.Simple())
		deadline := time.Now().Add(10 * time.Second)
		for {
			if rc.Exists(ctx, rdb.TaskKey("99")).Val() == 1 {
				break
			}
			if time.Now().After(deadline) {
				rc.Close()
				t.Fatal("replica has not applied acknowledged watermark")
			}
			time.Sleep(20 * time.Millisecond)
		}
		rc.Close()
	}

	started := time.Now()
	master.stop()
	deadline = time.Now().Add(20 * time.Second)
	for {
		attemptCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		_, err = producer.GetTask(attemptCtx, "99")
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Sentinel did not recover", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	for j := 0; j < 100; j++ {
		if _, err = producer.GetTask(ctx, fmt.Sprint(j)); err != nil {
			t.Fatal("confirmed task missing", j, err)
		}
	}
	if _, err = producer.Enqueue(ctx, fairq.Task{ID: "after-failover", Type: "sentinel"}, fairq.EnqueueOptions{GroupKey: "new"}); err != nil {
		t.Fatal(err)
	}
	healthy(t, failover, 101)
	t.Logf("Sentinel failover: confirmed=100 lost=0 observed_RPO_tasks=0 RTO=%s replica watermark=99 verified; async production replication can lose writes", time.Since(started))
}
