# 负载与性能测试工具

这些程序生成模拟任务，用于测试 FairQ 的负载和性能。业务通过 Go 库提交任务、注册 Handler，见 [业务接入示例](../docs/usage.md)。工具不提供业务 API，也不会被导入 `client`、`server` 的应用自动启动。

| 目录 | 用途 | 数据与结果 |
|---|---|---|
| `loadtest/` | 多进程 Worker 持续负载、任务守恒、排空及指标采样 | 默认使用空 DB5，生成模拟任务，输出 JSON 报告 |
| `benchmark/` | Dequeue Lua/API p99、10万 Lease 集中恢复与容量验收 | 使用空 DB5，结束时保留10万 Retry 供核查，输出 JSON 报告 |

## 运行前

在仓库根目录执行命令，使用**专用、可丢弃的 Redis 实例**，不要连接生产或共享业务实例。两个工具都拒绝非空目标数据库，不自动 FlushDB。Redis 必须配置显式 `maxmemory` 和 `maxmemory-policy noeviction`；按测试规模设置内存预算，并使用与目标部署相同的持久化策略。

`benchmark` 还会重置整个 Redis 实例的命令统计，并直接修改测试任务的 Lease 时间，所以仅隔离一个 DB 不足以避免影响其它用户。测试结束后只清理自己创建的实例。

查看参数无需 Redis：

```sh
go run ./tools/loadtest -h
go run ./tools/benchmark -h
```

## 日常短测

先启动满足上述配置的独立 Redis，再替换下面的地址运行：

```sh
go run ./tools/loadtest -redis 127.0.0.1:16400 \
  -groups 10 -backlog 100 -processes 2 -slots 4 \
  -handler-time 10ms -duration 30s -report smoke.json
```

`-groups` 是**模拟的 Group 数量**：工具轮流生成 `group-0` 到 `group-N-1` 的 GroupKey。它不是队列容量、Group 注册表或业务配置；FairQ 仍根据 Pending 动态维护活跃 Group。

默认持续负载时间为30秒，预置和排空另计。默认规模仍为1000 Group、10万积压、10进程×80槽，日常验证请使用上面的小规模参数。更长的持续负载需显式设置 `-duration`；不会由普通 `go test` 自动启动。

退出码 0 表示本次运行的正确性检查通过。报告中的 `Passed` 表示完整长稳门槛是否通过，短测保持 false；30 秒短测也不覆盖 60 秒后才开始的 goroutine 增长判断。不能用短测结果宣称长稳通过。

## 性能验收

这是显式执行的10万任务测试，不是日常快速检查：

```sh
go run ./tools/benchmark -redis 127.0.0.1:16401 -report perf.json
```

报告包含 Dequeue Lua p99 桶上界、API p99、恢复扫描/转换数、耗时、内存和 Inspector 检查结果。恢复每批最多100条、每轮最多500条，批间等待10ms；轮间省略生产默认的恢复等待，因此不能把耗时直接当作线上恢复 SLA。`Passed` 表示此性能用例的门槛通过，不代表全部生产门禁通过。具体判定逻辑见 [benchmark 源码](benchmark/main.go)。

## 工具自身回归

下面只运行解析和统计逻辑测试，不连接 Redis、不启动压测：

```sh
go test -race ./tools/...
go vet ./tools/...
```

真实 Redis 集成、故障注入和基础设施演练见 [开发测试指南](../docs/test-environment.md)。
