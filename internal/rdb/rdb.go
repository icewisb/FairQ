package rdb

import (
	"context"
	"embed"
	"fmt"
	"strconv"
	"strings"

	"fairq"
	"github.com/redis/go-redis/v9"
)

//go:embed lua/*.lua
var scripts embed.FS

type Store struct{ Redis *redis.Client }

func New(ctx context.Context, options redis.UniversalOptions) (*Store, error) {
	if options.IsClusterMode || (options.MasterName == "" && len(options.Addrs) > 1) {
		return nil, fmt.Errorf("fairq: Redis Cluster is unsupported")
	}
	options.MaxRetries = -1 // Mutating scripts cannot transparently retry an unknown result.
	options.ContextTimeoutEnabled = true
	var c *redis.Client
	if options.MasterName != "" {
		c = redis.NewFailoverClient(options.Failover())
	} else {
		c = redis.NewClient(options.Simple())
	}
	info, err := c.Info(ctx, "cluster").Result()
	if err != nil {
		c.Close()
		return nil, err
	}
	if strings.Contains(info, "cluster_enabled:1") {
		c.Close()
		return nil, fmt.Errorf("fairq: Redis Cluster is unsupported")
	}
	s := &Store{Redis: c}
	return s, nil
}
func (s *Store) Close() error { return s.Redis.Close() }

func script(name string) *redis.Script {
	common, err := scripts.ReadFile("lua/common.lua")
	if err != nil {
		panic(err)
	}
	body, err := scripts.ReadFile("lua/" + name + ".lua")
	if err != nil {
		panic(err)
	}
	return redis.NewScript(string(common) + "\n" + string(body))
}

func (s *Store) run(ctx context.Context, name string, script *redis.Script, args ...any) ([]any, error) {
	r, err := script.Run(ctx, s.Redis, scriptKeys, args...).Slice()
	if err != nil {
		return nil, &fairq.Error{Code: fairq.Internal, Op: name, Err: err}
	}
	if len(r) == 0 {
		return nil, &fairq.Error{Code: fairq.Internal, Op: name, Err: fmt.Errorf("empty script reply")}
	}
	code, ok := r[0].(string)
	if !ok {
		return nil, &fairq.Error{Code: fairq.Internal, Op: name, Err: fmt.Errorf("invalid script reply")}
	}
	if code != "OK" {
		return nil, &fairq.Error{Code: fairq.Code(code), Op: name}
	}
	return r[1:], nil
}

func integer(v any) (int64, error) { return strconv.ParseInt(fmt.Sprint(v), 10, 64) }
