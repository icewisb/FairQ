package rdb_test

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/icewisb/FairQ"
	"github.com/icewisb/FairQ/inspector"
	"github.com/icewisb/FairQ/internal/base"
	"github.com/icewisb/FairQ/internal/rdb"
	"github.com/icewisb/FairQ/internal/testutil"
	"github.com/redis/go-redis/v9"
)

// Read one RESP2 frame so the proxy can drop a response only after Redis executes it.
func respFrame(r *bufio.Reader) ([]byte, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 3 {
		return nil, fmt.Errorf("short RESP")
	}
	out := append([]byte(nil), line...)
	switch line[0] {
	case '$':
		n, e := strconv.Atoi(strings.TrimSpace(string(line[1:])))
		if e != nil {
			return nil, e
		}
		if n >= 0 {
			b := make([]byte, n+2)
			if _, e = io.ReadFull(r, b); e != nil {
				return nil, e
			}
			out = append(out, b...)
		}
	case '*':
		n, e := strconv.Atoi(strings.TrimSpace(string(line[1:])))
		if e != nil {
			return nil, e
		}
		for j := 0; j < n; j++ {
			b, e := respFrame(r)
			if e != nil {
				return nil, e
			}
			out = append(out, b...)
		}
	case '+', '-', ':':
	default:
		return nil, fmt.Errorf("unexpected RESP marker %q", line[0])
	}
	return out, nil
}
func commandArgs(frame []byte) ([]string, error) {
	r := bufio.NewReader(bytes.NewReader(frame))
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	n, err := strconv.Atoi(strings.TrimSpace(line[1:]))
	if err != nil {
		return nil, err
	}
	out := make([]string, n)
	for i := range out {
		part, e := respFrame(r)
		if e != nil {
			return nil, e
		}
		split := bytes.Index(part, []byte("\r\n"))
		if split < 0 || part[0] != '$' {
			return nil, fmt.Errorf("not a bulk")
		}
		out[i] = string(part[split+2 : len(part)-2])
	}
	return out, nil
}
func encodeCommand(args []string) []byte {
	var b bytes.Buffer
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, arg := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(arg), arg)
	}
	return b.Bytes()
}

func faultProxy(t *testing.T, upstream, sha, source, mode string) (string, *atomic.Int32) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var hit atomic.Int32
	var wg sync.WaitGroup
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop); ln.Close(); wg.Wait() })
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			down, e := ln.Accept()
			if e != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer down.Close()
				up, e := net.DialTimeout("tcp", upstream, time.Second)
				if e != nil {
					return
				}
				defer up.Close()
				dr, ur := bufio.NewReader(down), bufio.NewReader(up)
				for {
					down.SetReadDeadline(time.Now().Add(time.Second))
					frame, e := respFrame(dr)
					if e != nil {
						return
					}
					args, e := commandArgs(frame)
					if e != nil {
						return
					}
					target := len(args) > 1 && strings.EqualFold(args[0], "evalsha") && args[1] == sha
					inject := target && hit.CompareAndSwap(0, 1)
					if inject && mode == "disconnect-before" {
						return
					}
					if inject && strings.HasPrefix(mode, "lua-") {
						args[0] = "EVAL"
						args[1] = source
						frame = encodeCommand(args)
					}
					if _, e = up.Write(frame); e != nil {
						return
					}
					reply, e := respFrame(ur)
					if e != nil {
						return
					}
					if inject && mode == "disconnect-after" {
						return
					}
					if inject && mode == "timeout-after" {
						select {
						case <-time.After(300 * time.Millisecond):
						case <-stop:
						}
						return
					}
					if _, e = down.Write(reply); e != nil {
						return
					}
				}
			}()
		}
	}()
	return ln.Addr().String(), &hit
}

func TestTransitionFaultMatrix(t *testing.T) {
	for _, op := range []string{"enqueue", "dequeue", "heartbeat", "done", "retry", "forward", "recover", "clean"} {
		for _, mode := range []string{"disconnect-before", "disconnect-after", "timeout-after", "lua-before", "lua-partial", "wrong-type"} {
			t.Run(op+"/"+mode, func(t *testing.T) {
				s, c := store(t)
				ctx := context.Background()
				opts := testutil.Options(t, 2)
				opts.Protocol = 2
				common, e := os.ReadFile("lua/common.lua")
				if e != nil {
					t.Fatal(e)
				}
				body, e := os.ReadFile("lua/" + op + ".lua")
				if e != nil {
					t.Fatal(e)
				}
				source := string(common) + "\n" + string(body)
				sha, e := c.ScriptLoad(ctx, source).Result()
				if e != nil {
					t.Fatal(e)
				}
				before, after := "pending", "active"
				var a rdb.Attempt
				var candidate rdb.Candidate
				if op != "enqueue" {
					enqueue(t, s, "fault", "g")
				}
				if op != "enqueue" && op != "dequeue" {
					a, e = s.Dequeue(ctx, time.Minute)
					if e != nil {
						t.Fatal(e)
					}
					before = "active"
					after = "active"
				}
				switch op {
				case "enqueue":
					before = ""
					after = "pending"
				case "done":
					after = ""
				case "retry":
					after = "retry"
				case "forward":
					if _, e = s.Fail(ctx, a, 15*time.Second, "fixture", false); e != nil {
						t.Fatal(e)
					}
					c.ZAdd(ctx, rdb.Retry, redis.Z{Member: a.ID, Score: 0})
					page, e := s.Scan(ctx, "retry", 0, 100, 0)
					if e != nil || len(page.Candidates) != 1 {
						t.Fatal(page, e)
					}
					candidate = page.Candidates[0]
					before = "retry"
					after = "pending"
				case "recover":
					c.ZAdd(ctx, rdb.Lease, redis.Z{Member: a.ID, Score: 0})
					page, e := s.Scan(ctx, "lease", 0, 100, 0)
					if e != nil || len(page.Candidates) != 1 {
						t.Fatal(page, e)
					}
					candidate = page.Candidates[0]
					after = "retry"
				case "clean":
					c.HSet(ctx, rdb.TaskKey(a.ID), "retention_ms", 1)
					if e = s.Done(ctx, a); e != nil {
						t.Fatal(e)
					}
					c.ZAdd(ctx, rdb.Completed, redis.Z{Member: a.ID, Score: 0})
					page, e := s.Scan(ctx, "completed", 0, 100, 0)
					if e != nil || len(page.Candidates) != 1 {
						t.Fatal(page, e)
					}
					candidate = page.Candidates[0]
					before = "completed"
					after = ""
				}
				var beforeDump map[string]string
				if mode == "wrong-type" {
					if err := c.Set(ctx, rdb.Active, "wrong-type", 0).Err(); err != nil {
						t.Fatal(err)
					}
					beforeDump = dumpDB(t, c)
				}
				injected := source
				if mode == "lua-before" {
					injected = "error('injected before writes')\n" + source
				}
				if mode == "lua-partial" {
					injected = `local real=redis
local redis=setmetatable({}, {__index=real})
redis.call=function(command,...)
 local result=real.call(command,...)
 local mutating={HSET=true,HDEL=true,LPUSH=true,LPOP=true,RPOP=true,RPUSH=true,SADD=true,SREM=true,ZADD=true,ZREM=true,DEL=true}
 if mutating[string.upper(command)] then error('injected after first write') end
 return result
end
` + source
				}
				addr, hit := faultProxy(t, opts.Addrs[0], sha, injected, mode)
				opts.Addrs = []string{addr}
				opts.ReadTimeout = 100 * time.Millisecond
				opts.WriteTimeout = 100 * time.Millisecond
				faulty, e := rdb.New(ctx, opts)
				if e != nil {
					t.Fatal(e)
				}
				defer faulty.Close()
				switch op {
				case "enqueue":
					task := fairq.Task{ID: "fault", Type: "test"}
					o := fairq.EnqueueOptions{GroupKey: "g"}
					g, msg, _ := base.Validate(task, o, base.MaxPayload)
					_, e = faulty.Enqueue(ctx, task, o, g, msg)
				case "dequeue":
					_, e = faulty.Dequeue(ctx, time.Minute)
				case "heartbeat":
					_, e = faulty.Heartbeat(ctx, []rdb.Attempt{a}, time.Minute)
				case "done":
					e = faulty.Done(ctx, a)
				case "retry":
					_, e = faulty.Fail(ctx, a, 15*time.Second, "injected failure", false)
				case "forward":
					e = faulty.Forward(ctx, candidate)
				case "recover":
					_, e = faulty.Recover(ctx, candidate, 15*time.Second)
				case "clean":
					e = faulty.Clean(ctx, "completed", candidate, 0)
				}
				if !fairq.IsCode(e, fairq.Internal) || hit.Load() != 1 {
					t.Fatalf("expected injected INTERNAL, got %v hits=%d", e, hit.Load())
				}
				if mode == "wrong-type" {
					afterDump := dumpDB(t, c)
					if fmt.Sprint(beforeDump) != fmt.Sprint(afterDump) {
						t.Fatal("preflight type failure modified Redis")
					}
					return
				}
				inspect, e := inspector.New(ctx, testutil.Options(t, 2))
				if e != nil {
					t.Fatal(e)
				}
				defer inspect.Close()
				report, e := inspect.Check(ctx)
				if e != nil || !report.Complete {
					t.Fatal(report, e)
				}
				if mode == "lua-partial" && op != "heartbeat" {
					if len(report.Issues) == 0 {
						t.Fatal("partial mutation was not detected")
					}
					if c.Exists(ctx, rdb.TaskKey("fault")).Val() != 1 {
						t.Fatal("partial error lost original task Hash")
					}
				} else {
					expected := after
					if mode == "disconnect-before" || mode == "lua-before" {
						expected = before
					}
					if state := c.HGet(ctx, rdb.TaskKey("fault"), "state").Val(); state != expected {
						t.Fatalf("state=%s want=%s", state, expected)
					}
					if len(report.Issues) != 0 {
						t.Fatal(report.Issues)
					}
				}
			})
		}
	}
}

func dumpDB(t *testing.T, c *redis.Client) map[string]string {
	t.Helper()
	ctx := context.Background()
	out := map[string]string{}
	var cursor uint64
	for {
		keys, next, err := c.Scan(ctx, cursor, "*", 100).Result()
		if err != nil {
			t.Fatal(err)
		}
		for _, key := range keys {
			value, err := c.Dump(ctx, key).Result()
			if err != nil {
				t.Fatal(err)
			}
			out[key] = value
		}
		cursor = next
		if cursor == 0 {
			return out
		}
	}
}
