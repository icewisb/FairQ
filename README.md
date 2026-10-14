# FairQ

FairQ 是一个 Go + Redis 任务队列，按 Group 公平轮转领取任务。每个 Group 有独立的 FIFO；Worker 从共享轮转队列中逐组领取，Group 随入队和排空动态加入、退出，无需预先配置。

普通单 FIFO 队列中，一个租户的大量积压可能把其他租户排在后面。FairQ 让持续有任务的 Group 交替获得领取机会。例如 A、B、C 都非空时，领取顺序循环为 A → B → C；A 有更多任务也不会占据整个队首。公平性按 **Redis 成功领取顺序** 判断，不保证执行槽位、CPU 时间或完成顺序均分。

**项目状态：开发中。** 尚未发布稳定版本；历史 60 分钟长稳测试失败，完整复验和生产灰度尚未完成。

## 特性

- 动态 Group Round Robin；组内按成功入队顺序 FIFO。
- Lua 原子领取及状态转换，多进程共享同一调度顺序。
- TaskID 存续期去重、租约与心跳、旧执行尝试保护。
- 失败重试、租约过期恢复、归档；可选成功任务保留。
- 支持 Context 取消与优雅关闭；只读 Inspector 和 Prometheus 指标。

## 快速开始

需要 Go 1.24+ 和独立的 Redis 单实例或 Sentinel。当前真实验证版本是 **Redis 8.0.2（含 TLS）**；不声明未测试版本的兼容性，不支持 Redis Cluster。Redis 使用 `noeviction`，并按业务需要配置持久化、备份和 ACL/TLS。

公开模块路径尚未确定，当前通过本地源码试用。在仓库根目录下载依赖：

```sh
go mod download
```

在单独终端启动一个可丢弃的 Redis 实例：

```sh
redis-server --bind 127.0.0.1 --port 16380 --save '' --appendonly no --maxmemory 256mb --maxmemory-policy noeviction
```

回到仓库目录运行示例：

```sh
go run ./examples/basic -redis 127.0.0.1:16380
```

看到 `handled task ...` 后按 Ctrl-C，Worker 会优雅关闭。这个 Redis 命令仅用于可丢弃的本地演示，未启用持久化。完整源码见 [examples/basic](examples/basic/main.go)。

在另一个本地 Go 项目试用时，可以使用本地替换（路径换成实际克隆目录）：

```sh
go mod edit -require=fairq@v0.0.0 -replace=fairq=/absolute/path/to/FairQ
go mod tidy
```

## 使用

导入 `fairq`、`fairq/client`、`fairq/server` 和 `github.com/redis/go-redis/v9`。生产者和 Worker 可以放在不同进程。下面的函数可嵌入业务程序，完整 main、信号处理及连接清理见上述可运行示例：

```go
func serve(ctx context.Context, options redis.UniversalOptions) error {
    producer, err := client.New(ctx, client.Config{Redis: options})
    if err != nil { return err }
    defer producer.Close()

    worker, err := server.New(ctx, server.Config{
        Redis: options, Concurrency: 2, ShutdownTimeout: 5 * time.Second,
    })
    if err != nil { return err }
    defer worker.Shutdown()

    info, err := producer.Enqueue(ctx, fairq.Task{Type: "demo"},
        fairq.EnqueueOptions{GroupKey: "tenant-a", Timeout: time.Second})
    if err != nil {
        // 保存 info.ID；结果未知时查询该 ID，不能直接生成新 ID 重投。
        return fmt.Errorf("enqueue task %s: %w", info.ID, err)
    }
    return worker.Run(ctx, func(ctx context.Context, task fairq.Task) error {
        fmt.Printf("handled task %s\n", task.ID)
        return ctx.Err()
    })
}
```

示例仅打印任务 ID。业务 Handler 应按任务或业务请求 ID 幂等执行副作用，并响应 Context 取消。Handler 的错误文本会存入 Redis Task Hash，应由业务脱敏。

常用配置：

| 配置 | 行为 |
|---|---|
| `GroupKey` | 必填，1–256 字节 UTF-8；通常使用租户或业务分组 ID |
| `Concurrency` / `ShutdownTimeout` | 必须显式设置为正数 |
| `MaxRetry` | `0` 使用默认 25 次重试，第 26 次失败归档 |
| `Timeout` | `0` 不限制 Handler 时长 |
| `Retention` | `0` 在成功后删除任务；正值保留成功记录至到期 |

Payload 默认上限为 256 KiB；duration 必须是整数毫秒。租约、重试与状态转换详见 [架构说明](docs/architecture.md)。

## 可靠性与限制

- **At-least-once**：租约或连接故障可能导致再次执行。attempt token 保护 Redis 状态，不能阻止外部副作用重复；业务必须幂等。
- TaskID 去重只持续到 Task Hash 删除；相同 ID 在删除后可再次入队。未知写结果须按原 ID 和业务幂等记录核查。
- FIFO 指领取顺序；重试恢复后排在该组已有 Pending 后面，不保留原名次。
- Shutdown 先停领取并继续为在途任务续租；超时取消 Handler、停止续租，保留 Active/Lease 供恢复。忽略取消的 Handler 仍可能继续运行、占用资源和产生副作用。`Concurrency` 限制受管理的执行尝试；运行期间发生任务超时或失权后，槽位会继续领取任务，残留的 Handler 可能累计超过这个数量，因此业务必须响应取消。
- Sentinel 异步复制不保证零 RPO；Lua 运行时出错不会回滚已执行写入。
- 固定 `fairq:v1:` 命名空间；多个部署共用一个 DB 就共享队列。无 Cluster、优先级、权重、Cron、DAG、Web UI 或自动修复平台。

`client.GetTask` 查询单任务；`inspector.New` 提供 `Task`、`Group`、`Index`、`Check`，不暴露 attempt token、不修改数据。在线检查不是事务快照，全量 Check 的客户端内存随任务数增长，精确核对需停写窗口。

将 `producer.MetricsHandler()` / `worker.MetricsHandler()` 挂到应用已有的受保护 HTTP 服务；库不启动监听器。进程计数器分别采集，共享 Redis 深度不要重复求和。Redis ACL/TLS、连接超时及 Sentinel 认证通过 `redis.UniversalOptions` 配置。详见 [运维手册](docs/runbook.md)。

## 开发与测试

测试需要可丢弃的真实 Redis，**会清空 DB1–4、DB6**；基础设施测试另外启动临时 Redis/Sentinel，需支持 TLS 的 `redis-server`。不能使用共享或生产地址；缺失环境会失败，不跳过。

```sh
export FAIRQ_TEST_REDIS=127.0.0.1:16380
export FAIRQ_REDIS_SERVER=/path/to/redis-server
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
```

普通 `go test` 不启动长稳测试。提交改动时，请为行为变化提供可复现用例，并运行受影响的测试；涉及并发和共享状态时运行 race 检查。完整环境说明见 [开发测试指南](docs/test-environment.md)。

## 文档

- [架构说明](docs/architecture.md)：公平调度、任务状态与可靠性边界。
- [运维手册](docs/runbook.md)：部署、监控、故障处理和备份恢复。
- [开发测试指南](docs/test-environment.md)：本地测试环境与回归命令。
- [测试工具](tools/README.md)：可选负载与性能测试。

## License

本项目采用 [MIT License](LICENSE)。
