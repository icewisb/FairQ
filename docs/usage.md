# FairQ 使用指南

从 [README Quick Start](../README.md#快速开始) 和 [完整可运行示例](../examples/basic/main.go) 开始。公开 Module 路径尚未确定，目前导入 `fairq`、`fairq/client`、`fairq/server`；本地 replace 方法只在 README 维护。生产者与 Worker 可以独立部署。

## 生命周期与配置

`client.New(ctx, client.Config{Redis: options})` 创建生产者，退出时调用 `Close()`。`server.New(ctx, config)` 初始化连接，`Start(handler)` 启动后台处理后立即返回；`Run(ctx, handler)` 启动后等待 Context 取消或 Shutdown，取消时执行优雅关闭。New 的 Context 仅用于初始化，Start 不继承它作为运行生命周期。

Server 必须调用 `Shutdown()` 释放连接和维护器；即使未 Start 也可调用。重复/并发 Shutdown 会等待同一次关闭完成；关闭后不能重新 Start。Handler 不应从自身同步调用 Shutdown，否则关闭会等待这个 Handler，直到配置的超时。

| 配置 | 调用约束与含义 |
|---|---|
| `GroupKey` | 入队必填，1–256 字节 UTF-8；通常为租户或业务分组 ID |
| `Concurrency` / `ShutdownTimeout` | Server 必须显式设置正数；并发上限作用于受管理的执行尝试 |
| `MaxRetry` | `0` 使用默认 25 次重试，不表示禁用重试；不能为负 |
| `Timeout` | `0` 不限制 Handler 时长 |
| `Retention` | `0` 成功后删除；正值保留成功记录至到期 |
| `client.Config.MaxPayload` | `0` 使用默认 256 KiB，正值覆盖 Payload 字节上限 |

所有 duration 必须为整数毫秒；入队 duration 可为零，Server 间隔零值选择默认。HeartbeatInterval 必须小于 LeaseDuration，BatchSize 为 1–100，MaxBatchesPerTick 为 1–5（零值选择默认）。维护间隔、归档保留期和退避规则统一见 [架构说明](architecture.md#租约与维护)。Redis 地址、认证、TLS、连接超时和 Sentinel 选项通过 `redis.UniversalOptions` 提供；[运维手册](runbook.md#部署与安全) 说明部署要求。

## Handler 与写入结果

Handler 签名为 `func(context.Context, fairq.Task) error`。成功返回 nil；错误、超时和 panic 的状态转换见 [任务生命周期](architecture.md#任务生命周期)。Handler 必须按任务 ID 或业务请求 ID 幂等执行外部副作用，并及时响应 Context 取消。Go 无法强制终止不合作的 Handler；任务超时或失权后槽位可继续领取，残留 Handler 可能超过配置并发数。

Handler 错误文本会存入 Task Hash，业务需在返回错误前脱敏。内置 panic 错误不包含 panic 内容；不要在业务日志输出 Payload、口令或完整 attempt token。

`Enqueue` 对空 ID 生成 UUID，错误时也返回这个 ID。保存 `TaskInfo.ID`；网络错误或超时可能发生在 Redis 已执行之后，不得直接换新 ID 重投。用原 ID 查询并核对业务幂等记录；Hash 已删除也不能证明任务从未执行。错误码用 `fairq.IsCode` 判断；底层错误可用 `errors.Unwrap` 检查，输出前注意敏感信息。

`TASK_ID_CONFLICT` 表示当前 Hash 已存在；库不会比较 Payload 后替换任务。Hash 删除后 ID 可以重用，但业务幂等记录的有效期仍由业务决定。

## 查询与观测

`client.GetTask` 和 `inspector.Task` 返回 `TaskInfo`，不存在时返回 `fairq.ErrTaskNotFound`，不暴露 attempt token。Inspector 用 `inspector.New` 创建并用 `Close` 释放；只读 API 包括：

- `Group(ctx, group, offset, count)`：Pending 的 Redis List 顺序（新任务在前，最先领取的任务在尾），count 为 1–100。
- `Index(ctx, name, cursor, count)`：支持 ready、members、active、lease、retry、archived、completed、bad_refs；使用返回的 Next 继续，Next=0 结束。Set/ZSet 的 count 是 SCAN 提示而非严格返回上限。
- `Check(ctx)`：分批对账全部索引与 Task Hash。先检查 error，再检查 Complete 和 Issues。在线读取不是事务快照；全量检查客户端内存随任务和引用数增长，精确诊断需停写窗口。

把 `producer.MetricsHandler()` / `worker.MetricsHandler()` 挂到已有的受保护 HTTP 服务；库不启动监听器。指标含义、聚合和抓取成本统一见 [运维手册](runbook.md#观测与告警)。
