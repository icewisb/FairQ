// Package inspector provides read-only, batched queries. Online checks are observations,
// not a transactional snapshot; confirm findings in a quiet window before repair.
package inspector

import (
	"context"
	"encoding/base64"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/icewisb/FairQ"
	"github.com/icewisb/FairQ/internal/base"
	"github.com/icewisb/FairQ/internal/rdb"
	"github.com/redis/go-redis/v9"
)

type Inspector struct{ store *rdb.Store }
type Issue struct{ Kind, Key, TaskID, Detail string }
type Report struct {
	Complete                     bool
	CheckedTasks, CheckedPending int
	Issues                       []Issue
}
type Entry struct {
	ID    string
	Score float64
}
type Page struct {
	Next    uint64
	Entries []Entry
}

func New(ctx context.Context, options redis.UniversalOptions) (*Inspector, error) {
	s, err := rdb.New(ctx, options)
	if err != nil {
		return nil, err
	}
	return &Inspector{store: s}, nil
}
func (i *Inspector) Close() error { return i.store.Close() }
func (i *Inspector) Task(ctx context.Context, id string) (fairq.TaskInfo, error) {
	return i.store.GetTask(ctx, id)
}
func (i *Inspector) Group(ctx context.Context, group string, offset int64, count int) ([]string, error) {
	g, err := base.Group(group)
	if err != nil {
		return nil, err
	}
	if offset < 0 || count < 1 || count > 100 {
		return nil, fmt.Errorf("fairq: query page must be 1..100")
	}
	return i.store.Redis.LRange(ctx, rdb.PendingKey(g), offset, offset+int64(count)-1).Result()
}

func (i *Inspector) Index(ctx context.Context, name string, cursor uint64, count int) (Page, error) {
	var p Page
	if count < 1 || count > 100 {
		return p, fmt.Errorf("fairq: query page must be 1..100")
	}
	c := i.store.Redis
	sets := map[string]string{"members": rdb.Members, "active": rdb.Active}
	zs := map[string]string{"lease": rdb.Lease, "retry": rdb.Retry, "archived": rdb.Archived, "completed": rdb.Completed}
	lists := map[string]string{"ready": rdb.Ready, "bad_refs": rdb.BadRefs}
	if key, ok := sets[name]; ok {
		values, next, err := c.SScan(ctx, key, cursor, "", int64(count)).Result()
		p.Next = next
		for _, v := range values {
			p.Entries = append(p.Entries, Entry{ID: v})
		}
		return p, err
	}
	if key, ok := zs[name]; ok {
		values, next, err := c.ZScan(ctx, key, cursor, "", int64(count)).Result()
		if err != nil {
			return p, err
		}
		p.Next = next
		for n := 0; n < len(values); n += 2 {
			score, err := strconv.ParseFloat(values[n+1], 64)
			if err != nil {
				return p, err
			}
			p.Entries = append(p.Entries, Entry{ID: values[n], Score: score})
		}
		return p, nil
	}
	if key, ok := lists[name]; ok {
		if cursor > uint64(1<<63-1)-uint64(count) {
			return p, fmt.Errorf("fairq: invalid list cursor")
		}
		values, err := c.LRange(ctx, key, int64(cursor), int64(cursor)+int64(count)-1).Result()
		if err != nil {
			return p, err
		}
		size, err := c.LLen(ctx, key).Result()
		if err != nil {
			return p, err
		}
		if cursor+uint64(len(values)) < uint64(size) {
			p.Next = cursor + uint64(len(values))
		}
		for _, v := range values {
			p.Entries = append(p.Entries, Entry{ID: v})
		}
		return p, nil
	}
	return p, fmt.Errorf("fairq: unknown index")
}

func (i *Inspector) Check(ctx context.Context) (Report, error) {
	report := Report{Complete: true}
	c := i.store.Redis
	add := func(kind, key, id, detail string) {
		report.Issues = append(report.Issues, Issue{kind, key, id, detail})
	}
	for key, expected := range map[string]string{rdb.Ready: "list", rdb.Members: "set", rdb.Active: "set", rdb.Lease: "zset", rdb.Retry: "zset", rdb.Archived: "zset", rdb.Completed: "zset", rdb.BadRefs: "list"} {
		typ, err := c.Type(ctx, key).Result()
		if err != nil {
			return report, err
		}
		if typ != "none" && typ != expected {
			add("TYPE", key, "", "expected "+expected)
			report.Complete = false
		}
	}
	if !report.Complete {
		return report, nil
	}
	indexes := map[string]map[string]bool{}
	readyCounts := map[string]int{}
	for _, name := range []string{"ready", "members", "active", "lease", "retry", "archived", "completed"} {
		values := map[string]bool{}
		var cursor uint64
		for {
			page, err := i.Index(ctx, name, cursor, 100)
			if err != nil {
				return report, err
			}
			for _, e := range page.Entries {
				values[e.ID] = true
				if name == "ready" {
					readyCounts[e.ID]++
				}
				switch name {
				case "lease", "retry", "archived", "completed":
					if math.IsNaN(e.Score) || e.Score < 0 || e.Score > 1<<53-1 || e.Score != math.Trunc(e.Score) {
						add("SCORE", rdb.Prefix+name, e.ID, "expected nonnegative exact integer milliseconds")
					}
				}
			}
			cursor = page.Next
			if cursor == 0 {
				break
			}
		}
		indexes[name] = values
	}
	for group, n := range readyCounts {
		if n != 1 {
			add("READY_DUPLICATE", rdb.Ready, "", group)
		}
		if !indexes["members"][group] {
			add("READY_MEMBERS", rdb.Members, "", group)
		}
	}
	for group := range indexes["members"] {
		if !indexes["ready"][group] {
			add("READY_MEMBERS", rdb.Ready, "", group)
		}
	}
	pending := map[string][]string{}
	groups := map[string]int{}
	seenPendingKeys := map[string]bool{}
	seenTasks := map[string]bool{}
	// Pending references are read separately so task checks can compare all main indexes.
	var cursor uint64
	for {
		keys, next, err := c.Scan(ctx, cursor, rdb.Prefix+"pending:*", 100).Result()
		if err != nil {
			return report, err
		}
		for _, key := range keys {
			if seenPendingKeys[key] {
				continue
			}
			seenPendingKeys[key] = true
			g := strings.TrimPrefix(key, rdb.Prefix+"pending:")
			typ, err := c.Type(ctx, key).Result()
			if err != nil {
				return report, err
			}
			if typ == "none" {
				continue
			}
			if typ != "list" {
				add("TYPE", key, "", "expected list")
				report.Complete = false
				continue
			}
			for off := int64(0); ; off += 100 {
				ids, err := c.LRange(ctx, key, off, off+99).Result()
				if err != nil {
					return report, err
				}
				for _, id := range ids {
					pending[id] = append(pending[id], g)
					groups[g]++
					report.CheckedPending++
				}
				if len(ids) < 100 {
					break
				}
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	for g, n := range groups {
		if n > 0 && (!indexes["ready"][g] || !indexes["members"][g]) {
			add("UNREACHABLE_PENDING", rdb.PendingKey(g), "", g)
		}
	}
	for g := range readyCounts {
		if groups[g] == 0 {
			add("EMPTY_GROUP", rdb.Ready, "", g)
		}
	}
	for {
		keys, next, err := c.Scan(ctx, cursor, rdb.Prefix+"task:*", 100).Result()
		if err != nil {
			return report, err
		}
		pipe := c.Pipeline()
		cmds := make([]*redis.MapStringStringCmd, len(keys))
		for n, key := range keys {
			cmds[n] = pipe.HGetAll(ctx, key)
		}
		_, pipeErr := pipe.Exec(ctx)
		if pipeErr != nil && !strings.HasPrefix(pipeErr.Error(), "WRONGTYPE") {
			return report, pipeErr
		}
		for n, key := range keys {
			id := strings.TrimPrefix(key, rdb.Prefix+"task:")
			if seenTasks[id] {
				continue
			}
			seenTasks[id] = true
			report.CheckedTasks++
			h, err := cmds[n].Result()
			if err != nil {
				add("TYPE", key, id, "expected hash")
				report.Complete = false
				continue
			}
			if len(h) == 0 {
				add("CHANGED_DURING_SCAN", key, id, "")
				continue
			}
			state := h["state"]
			refs := pending[id]
			main := 0
			if len(refs) > 0 {
				main += len(refs)
			}
			for _, name := range []string{"active", "retry", "archived", "completed"} {
				if indexes[name][id] {
					main++
				}
			}
			match := false
			if state == "pending" {
				match = len(refs) == 1 && refs[0] == h["group"]
			} else if set, ok := indexes[state]; ok && (state == "active" || state == "retry" || state == "archived" || state == "completed") {
				match = set[id]
			}
			if main != 1 || !match {
				add("MAIN_STATE", key, id, "state="+state)
			}
			if indexes["lease"][id] != (state == "active") || indexes["active"][id] != (state == "active") || (state == "active" && h["attempt_token"] == "") {
				add("ACTIVE_LEASE", key, id, "")
			}
			group, err := base64.RawURLEncoding.DecodeString(h["group"])
			canonical, gerr := base.Group(string(group))
			if err != nil || gerr != nil || canonical != h["group"] {
				add("GROUP", key, id, "")
			}
			fields := []string{"retry_count", "max_retry", "timeout_ms", "retention_ms", "enqueued_at"}
			if state == "pending" {
				fields = append(fields, "pending_since")
			}
			for _, field := range fields {
				v, err := strconv.ParseInt(h[field], 10, 64)
				limit := int64(base.MaxExactInteger)
				if field == "timeout_ms" || field == "retention_ms" {
					limit = int64((1<<63 - 1) / time.Millisecond)
				}
				if err != nil || v < 0 || v > limit {
					add("FIELD", key, id, field)
				}
			}
			if _, err := base.Decode([]byte(h["msg"])); err != nil {
				add("MESSAGE", key, id, "decode failed")
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	for id := range pending {
		if !seenTasks[id] {
			add("MISSING_HASH", "pending", id, "")
		}
	}
	for _, name := range []string{"active", "lease", "retry", "archived", "completed"} {
		for id := range indexes[name] {
			if !seenTasks[id] {
				add("MISSING_HASH", name, id, "")
			}
		}
	}
	depth, err := c.LLen(ctx, rdb.BadRefs).Result()
	if err != nil {
		return report, err
	}
	if depth > 0 {
		add("BAD_REFS", rdb.BadRefs, "", strconv.FormatInt(depth, 10))
	}
	return report, nil
}
