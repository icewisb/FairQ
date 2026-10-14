# FairQ

Go + Redis 多 Group 公平任务队列。Group 内按成功入队顺序 FIFO，活跃 Group 全局 Round Robin。领取、续租、完成、失败和恢复使用 Lua 原子转换；只支持单机 Redis / Sentinel。需求基线与边界见 [v1.4](docs/fairq-final-technical-design-v1.4.md)。

## 使用

```go
ctx := context.Background()
redisConfig := redis.UniversalOptions{Addrs: []string{"127.0.0.1:6379"}}
producer, err := client.New(ctx, client.Config{Redis: redisConfig})
if err != nil { return err }
defer producer.Close()
info, err := producer.Enqueue(ctx, fairq.Task{Type: "send", Payload: []byte(`{"id":42}`)},
    fairq.EnqueueOptions{GroupKey: "tenant-42", Timeout: time.Minute})
// 即使 err != nil，仍保存 info.ID 以核查未知结果，勿自动生成新 ID 重投。
_ = info
if err != nil { return err }

worker, err := server.New(ctx, server.Config{
    Redis: redisConfig, Concurrency: 80, ShutdownTimeout: 30*time.Second,
})
if err != nil { return err }
// ctx 通常来自 signal.NotifyContext；Run 在取消时执行优雅关闭。
return worker.Run(ctx, func(ctx context.Context, task fairq.Task) error {
    return handleIdempotently(ctx, task.ID, task.Payload)
})
```

导入模块 `fairq`、`fairq/client`、`fairq/server` 和 `github.com/redis/go-redis/v9`。这是库，不启动 HTTP 服务。将 `producer.MetricsHandler()` / `worker.MetricsHandler()` 挂载到应用已有的受保护 HTTP 服务；分别采集各实例的进程计数器，避免把重复采集的 Redis 深度求和。

默认 Lease 30s、Heartbeat 10s、Recover 10s、Forward 1s、空队列轮询 1s。Concurrency 和 ShutdownTimeout 必须显式为正。默认最多重试 25 次（第 26 次失败归档），指数退避从 15s 开始、基数上限 1h，另加最多 10% 正抖动。`MaxRetry=0` 使用默认值；`Timeout=0` 不限制 Handler 时长；成功默认删除，`Retention>0` 保留 Completed 至到期。Group 原值为非空 UTF-8，最多 256 bytes；Payload 默认最多 256KiB。

业务必须用稳定业务键实现幂等：FairQ 保证 At-least-once，过期 Handler 可能继续产生外部副作用。重复 TaskID 只在 Hash 存续期间拒绝。Handler 必须响应 Context 取消；Shutdown 超时保留 Active/Lease，后续通过 Recover → Retry → Forward 恢复。普通任务继续用 Asynq，两个队列数据互不兼容。

## 查询与运维

`client.GetTask` 查询单任务；`inspector.New(ctx, redisConfig)` 提供 `Task`、`Group`、`Index`、`Check`。查询不暴露 attempt_token，不自动修复。Check 使用分页读，在线结果不是事务快照；精确核对先停写。SCAN 系列的 COUNT 为提示，不是响应条数硬上限。Inspector 全量核对在客户端保存 ID 集合，内存随任务数增长，应离峰执行。

部署使用专用 Redis、`maxmemory-policy noeviction`，配置 AOF/备份和容量预算。ACL、TLS 通过 `redis.UniversalOptions` 配置；Sentinel 设置 MasterName、Addrs、SentinelUsername/SentinelPassword。客户端禁用未知结果的透明写重试；连接或脚本失败必须核查状态。

见 [契约](docs/contracts.md)、[验收矩阵](docs/acceptance.md)、[运维手册](docs/runbook.md)、[开发进度](docs/development-progress.md)。生产验收未完成前不能仅凭单元测试通过上线。

## 开发验证

Go 1.24+；真实 Redis 8.0.2 为当前验证版本。测试必须显式指定一次性 Redis，测试会清空 DB 1、2、3、4，禁止使用共享或生产地址。

```sh
FAIRQ_TEST_REDIS=127.0.0.1:16380 FAIRQ_REDIS_SERVER=/path/to/redis-server go test -race ./...
go vet ./...
```

无 Redis 环境时集成测试明确失败，不跳过。[测试环境记录](docs/test-environment.md) 包含本机与 Docker 的实测差异。故障、性能与长稳结果单独记录，未执行的项目不算通过。
