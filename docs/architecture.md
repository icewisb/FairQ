# 架构说明

FairQ 使用 Redis 保存任务和调度状态，由 Go Worker 执行业务 Handler。生产者、Worker 和只读 Inspector 可以独立运行；共享同一 Redis DB 的进程参与同一个队列。支持单实例和 Sentinel，不支持 Redis Cluster。

## 公平调度

每个 Group 有一个 Pending FIFO，所有非空 Group 组成共享轮转队列。一次领取通过 Lua 原子完成：

1. 取出轮转队首 Group，再取出该组最早入队的任务。
2. 将任务转为 Active，生成本次执行的 token 并建立租约。
3. 如果该组仍有 Pending，将 Group 放回轮转队尾；否则移除其轮转资格。

对固定且持续非空的 G 个 Group，在没有损坏引用和动态入退组的窗口内，任意连续 G 次 Redis 成功领取各组恰好一次。

新建或重新有任务的 Group 加入轮转队尾，不需要预先注册。持续非空的 A、B、C 按 A → B → C 轮流领取，即使 A 的积压远大于其他组，也不会占据整个队首。重试任务重新入队时排在该组已有 Pending 后面。

公平性以 Redis 成功领取顺序衡量。它不保证 CPU 时间、执行槽位或完成顺序均分；并发 Handler 的开始和结束顺序也可能不同。

## 任务生命周期

| 状态 | 进入条件 | 后续处理 |
|---|---|---|
| `pending` | 首次入队或到期重试转发 | Worker 领取后进入 `active` |
| `active` | 已领取，持有 token 和租约 | 成功后删除或进入 `completed`；失败或租约过期进入 `retry` / `archived` |
| `retry` | 失败且未超过重试上限 | 到期后回到原 Group 的 `pending` |
| `archived` | 超过重试上限，或任务消息无法解码 | 默认保留 90 天，再清理 |
| `completed` | 成功且设置了 `Retention` | 保留期结束后清理；默认成功直接删除 |

Handler 返回错误、超时、panic，以及租约过期恢复都会增加失败计数。`MaxRetry=0` 使用默认 25 次重试，第 26 次失败归档。退避从 15 秒指数增长至 1 小时，再加最多 10% 的正向随机抖动。无法解码的消息保留原始数据并直接归档，不增加 Handler 失败计数。

## 租约与维护

每次领取生成独立的 attempt token。Heartbeat、完成和失败提交都校验当前状态、token、索引和未过期租约，阻止旧执行尝试覆盖新尝试。Redis `TIME` 决定绝对时间，租约截止时间等于当前时间时即视为过期。

Worker 同时运行心跳、到期重试转发、过期租约恢复和终态清理。默认间隔为：

| 配置 | 默认值 |
|---|---|
| Lease / Heartbeat | 30 秒 / 10 秒 |
| Recover / Forward | 10 秒 / 1 秒 |
| 空队列轮询 / Cleaner | 1 秒 / 1 分钟 |

维护扫描默认且最多每批 100 个候选、每轮 5 批，默认批间等待 10 毫秒；这些上限可调小。启动有随机抖动，Cleaner 在 Archived/Completed 间共享每轮预算并交替优先扫描。扫描结果只是候选，Lua 在变更前重新校验状态和索引快照。多个 Worker 可以竞争维护同一任务，无需选主；过期任务先进入 Retry，并不会立即执行。

关闭时先停止领取，再继续心跳并等待在途任务。`ShutdownTimeout` 到期后取消 Handler、停止续租，留下 Active/Lease 供其他 Worker 恢复。业务 Handler 必须响应 Context 取消；Go 无法强制终止忽略取消的函数，残留 Handler 可能继续产生副作用，并使实际运行数量超过 `Concurrency`。

## Redis 存储

所有 Key 使用固定前缀 `fairq:v1:`。GroupKey 使用无填充 URL-safe Base64 编码；Task Hash 保存任务内容、状态、配置和执行信息。

| Key 后缀 | 类型 | 用途 |
|---|---|---|
| `groups:ready` / `groups:members` | List / Set | 轮转顺序和成员去重 |
| `pending:<encoded-group>` | List | 组内 FIFO |
| `task:<id>` | Hash | 任务内容和状态 |
| `active` / `lease` | Set / ZSet | 执行中任务和租约到期时间 |
| `retry` | ZSet | 下次可转发时间 |
| `archived` / `completed` | ZSet | 归档时间 / 成功保留截止时间 |
| `bad_refs` | List | 损坏引用的最小证据，不自动清理 |

正常任务的 Hash 状态与唯一主索引必须一致；Active 必须同时拥有 Lease 和当前 token。ready 不重复且与 members 集合一致，非空 Pending 必须能从轮转队列到达，空组立即退出。不要直接修改这些 Key；诊断和修复流程见 [运维手册](runbook.md)。

## 可靠性边界

- **At-least-once**：连接故障、租约丢失或恢复可能再次执行任务。token 只保护 Redis 状态，外部副作用需要业务幂等。
- **有限期去重**：TaskID 只在 Task Hash 存在时去重，删除后可再次使用。未知入队结果应使用返回的原 ID 核查；Hash 不存在也不能证明任务从未执行。
- **Lua 不提供错误回滚**：脚本在写入前检查可预测错误，但运行时异常仍可能留下部分写入。未知结果不能盲目重试写操作或重新执行 Handler。
- **持久性取决于 Redis**：AOF、备份和复制配置决定数据丢失窗口；Sentinel 异步复制不保证零 RPO。
- **损坏数据保留现场**：仅对可证明错误的引用先记录 `bad_refs` 再隔离；矛盾或未知状态保留并报告，不自动猜测修复。

损坏 Pending 每次 Dequeue 最多隔离一条：先写 bad_refs，再移除错误引用；组内仍有任务时保留 ready 队首位置，空组才退出。连续第 10 次 CORRUPT_REFERENCE 开始阻塞领取循环，按 100/200/400/800/1000ms 退避，成功领取后清零。INTERNAL 按 1 秒限速；EMPTY 使用 PollInterval。bad_refs 达 10000 条仅告警，写证据失败则保留原引用并返回 INTERNAL；Archived 数量阈值同样不导致自动裁剪。

本文是当前设计与状态语义的权威说明，沿用 v1.4 契约；原始冻结文本可在 Git 提交 `fe901d5` 的 `docs/fairq-final-technical-design-v1.4.md` 中查阅。公开 API 以源码为准（使用具体 Client/Server 类型，Inspector 为公开包）；接入细节见 [使用指南](usage.md)，验收范围和证据见 [发布验收](release-readiness.md)。

返回 [README](../README.md)。
