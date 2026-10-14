# FairQ

FairQ 是一个 Go + Redis 任务队列，按 Group 公平轮转领取任务。每个 Group 有独立的 FIFO；Worker 从共享轮转队列中逐组领取，Group 随入队和排空动态加入、退出，无需预先配置。

普通单 FIFO 队列中，一个租户的大量积压可能把其他租户排在后面。FairQ 让持续有任务的 Group 交替获得领取机会。例如 A、B、C 都非空时，领取顺序循环为 A → B → C；A 有更多任务也不会占据整个队首。公平性按 **Redis 成功领取顺序** 判断，不保证执行槽位、CPU 时间或完成顺序均分。

当前为发布前代码：本地功能、并发和故障测试有记录，但历史 60 分钟长稳失败，完整复验及生产灰度未完成。不要将仓库公开或短测通过理解为生产可靠性已经验证。当前状态见 [发布审计](docs/release-audit.md) 和 [验收报告](docs/acceptance-report.md)。

## 特性

- 动态 Group Round Robin；组内按成功入队顺序 FIFO。
- Lua 原子领取及状态转换，多进程共享同一调度顺序。
- TaskID 存续期去重、租约与心跳、旧执行尝试保护。
- 失败重试、租约过期恢复、归档；可选成功任务保留。
- 支持 Context 取消与优雅关闭；只读 Inspector 和 Prometheus 指标。

## 安装与 Quick Start

需要 Go 1.24+ 和独立的 Redis 单实例或 Sentinel。当前真实验证版本是 **Redis 8.0.2（含 TLS）**；不声明未测试版本的兼容性，不支持 Redis Cluster。Redis 使用 `noeviction`，并按业务需要配置持久化、备份和 ACL/TLS。

当前 `go.mod` 为本地模块 `fairq`，尚未确定公开模块路径或发布版本，因此暂不提供 `go get` 安装命令。在仓库根目录运行：

```sh
go mod download
# 另一个终端启动专用演示 Redis，不要连接已有业务实例。
redis-server --bind 127.0.0.1 --port 16380 --save '' --appendonly no --maxmemory 256mb --maxmemory-policy noeviction
go run ./examples/basic -redis 127.0.0.1:16380
```

看到 `handled task ...` 后按 Ctrl-C，Worker 会优雅关闭。这个 Redis 命令仅用于可丢弃的本地演示，未启用持久化。完整可运行源码在 [examples/basic](examples/basic/main.go)，其入队、处理、Completed 状态和关闭路径也有真实 Redis 测试。

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

默认 Lease 30s、Heartbeat 10s、Recover 10s、Forward 1s、空轮询 1s。`Concurrency` 和 `ShutdownTimeout` 必须显式为正。`MaxRetry=0` 使用默认25次重试，第26次失败归档；退避从15s指数增长到1h，再加最多10%正抖动。`Timeout=0` 不限制 Handler 时长；成功默认删除，`Retention>0` 保留至到期。duration 必须是整数毫秒；Group 为1–256字节 UTF-8，Payload 默认上限256KiB。

## 可靠性与限制

- **At-least-once**：租约或连接故障可能导致再次执行。attempt token 保护 Redis 状态，不能阻止外部副作用重复；业务必须幂等。
- TaskID 去重只持续到 Task Hash 删除；相同 ID 在删除后可再次入队。未知写结果须按原 ID 和业务幂等记录核查。
- FIFO 指领取顺序；重试恢复后排在该组已有 Pending 后面，不保留原名次。
- Shutdown 先停领取并继续为在途任务续租；超时取消 Handler、停止续租，保留 Active/Lease 供恢复。忽略取消的 Handler 仍可能继续运行、占用资源和产生副作用。
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

普通 `go test` 不启动 60 分钟长稳。[测试指南](docs/test-environment.md) 说明环境、DB隔离和故障演练；[工具说明](tools/README.md) 区分负载/性能工具与业务库。更多细节见 [实现契约](docs/contracts.md)、[验收矩阵](docs/acceptance.md)、[v1.4 历史设计基线](docs/fairq-final-technical-design-v1.4.md)。设计中的 API 为示意，当前 API 以源码为准；原文保留，不据其复选框推断当前验证状态。

## License

本项目采用 [MIT License](LICENSE)。公开模块路径待 GitHub 仓库确定后修改。
