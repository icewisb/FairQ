# FairQ

FairQ 是一个 Go + Redis 任务队列，按 Group 公平轮转领取任务。每个 Group 有独立的 FIFO；Worker 从共享轮转队列中逐组领取，Group 随入队和排空动态加入、退出，无需预先配置。

普通单 FIFO 队列中，一个租户的大量积压可能把其他租户排在后面。FairQ 让持续有任务的 Group 交替获得领取机会。例如 A、B、C 都非空时，领取顺序循环为 A → B → C；A 有更多任务也不会占据整个队首。公平性按 **Redis 成功领取顺序** 判断，不保证执行槽位、CPU 时间或完成顺序均分。

**项目状态：实验阶段。** 尚未发布稳定版本；当前验收结论和未完成项见 [发布验收](docs/release-readiness.md)。

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

## 限制

- **At-least-once**：业务副作用必须幂等；TaskID 去重只持续到 Task Hash 删除。
- FIFO/RR 保证成功领取顺序，重试回到组尾，不保证完成顺序或执行资源均分。
- Handler 必须响应 Context 取消；忽略取消会残留执行、占用资源并可能超过 `Concurrency`。
- 固定 `fairq:v1:` 命名空间，同一 Redis DB 即共享队列；不支持 Cluster、优先级、权重、Cron、DAG 或 Web UI。
- Redis 持久化与复制决定数据丢失窗口；Sentinel 不保证零 RPO，Lua 运行时错误不回滚。

接入、配置、未知写结果及查询见 [使用指南](docs/usage.md)，精确调度与状态契约见 [架构说明](docs/architecture.md)，部署与故障处理见 [运维手册](docs/runbook.md)。

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

- [使用指南](docs/usage.md)：公共 API、配置、Handler 和查询。
- [发布验收](docs/release-readiness.md)：唯一测试矩阵、实测结果与发布边界。
- [架构说明](docs/architecture.md)：公平调度、任务状态与可靠性边界。
- [运维手册](docs/runbook.md)：部署、监控、故障处理和备份恢复。
- [开发测试指南](docs/test-environment.md)：本地测试环境与回归命令。
- [测试工具](tools/README.md)：可选负载与性能测试。

## License

本项目采用 [MIT License](LICENSE)。
