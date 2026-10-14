# FairQ 技术设计与生产验收规范

**版本**：v1.4 精简生产编码基线候选｜**日期**：2026-10-14  
**性质**：完整替代 v1.0–v1.3；通过测试与运维演练前不宣称生产可用。

## 1. 目标与边界

FairQ 是与 Asynq 并行部署的独立 Go + Redis 任务队列。普通任务仍使用 Asynq；需多用户公平领取的任务进入 FairQ。业务只传 `GroupKey` 和任务，不维护本地待投递队列，也不等待 Worker 空闲后再提交。

**P0 能力**：每 Group 独立 FIFO、全局 Round Robin、多 Worker 原子领取、TaskID 去重、Active/Lease/Heartbeat、失败重试与归档、租约过期恢复、优雅关闭、基本查询与异常证据保留。

**公平性契约**：对固定且持续有 Pending 的 G 个 Group，在无损坏引用、无动态入退组的条件下，任意连续 G 次 Redis 成功领取各 Group 恰好一次。动态新 Group 排轮转尾部，空 Group 立即退出，恢复入队重新排尾。只保证**领取机会**公平，不保证同时运行的任务数、CPU 时间或完成顺序公平。

**可靠性契约**：At-least-once，业务必须按 TaskID/业务请求 ID 幂等。`attempt_token` 只防旧执行尝试误改 Redis 状态，不阻止外部副作用重复。Sentinel 异步复制可能丢失已确认但未复制的写入；不承诺零 RPO。

**非目标**：Class/优先级、权重、Cron、DAG、任务聚合、Work Stealing、Web UI、Redis Cluster、严格执行槽位均分、Group Idle TTL/GC、独立 Incident 管理平台、自动批量 Repair。

## 2. 成熟实现借鉴与选择

| 项目 | 参考内容 | 明确不照搬 |
|---|---|---|
| [Asynq v0.25.1](https://github.com/hibiken/asynq/tree/v0.25.1) | Task Hash、Lua 状态转换、Lease/Heartbeat、Retry/Archive、Recoverer、Processor、测试与关闭 | Queue 维度改为 Group Pending；不引入聚合/Cron/优先级 |
| [GQM](https://github.com/benedict-erwin/gqm) | 多队列 RR 对照、Worker Pool、重试、Lua 与负载测试 | 固定配置队列轮转不等于动态 Group 全局轮转；不引入 DAG/UI |
| [FairQueue/fairque](https://github.com/myui/fairque) | 多租户公平选择、Group 隔离 | Python、优先级、Work Stealing、依赖；仓库已归档，仅参考 |
| [Redis Lua 官方文档](https://redis.io/docs/latest/develop/programmability/eval-intro/) | 脚本隔离、短脚本、减少网络往返 | 不将原子执行误认为运行时错误回滚 |
| [Redis 无回滚说明](https://redis.io/blog/you-dont-need-transaction-rollbacks-in-redis/) | 预校验、幂等、异常处理 | 不声称 Lua 具有数据库式回滚 |

优先复用/移植 Asynq 已验证的执行生命周期与测试模式，重点改造 Enqueue、Dequeue、Retry Forward、Recoverer 的 Group 索引维护。是否 Fork 取决于许可证、代码耦合和长期维护成本；借鉴不代表自动获得生产可靠性。

## 3. 架构

```text
Client.Enqueue(GroupKey, Task)
         │
         ▼
Group Pending List（每 Group 一条 FIFO）
         │
Global ready List + members Set（全局轮转）
         │
Worker → 单 Lua Dequeue → Shared Active + Lease
         │
Handler → Done / Retry / Archived
         │
Forwarder（Retry 到期） / Recoverer（Lease 过期） / Cleaner（保留到期）
```

不引入独立预投递调度器。后台维护器可以多实例运行；正确性来自每次原子条件转换，而非选主。

## 4. Redis Key（精简最终版）

| Key | 类型 | 用途 |
|---|---|---|
| `fairq:v1:groups:ready` | List | 活跃 Group 轮转，LPOP/RPUSH |
| `fairq:v1:groups:members` | Set | Ready 去重，不是永久注册表 |
| `fairq:v1:pending:<encoded-group>` | List | 每 Group FIFO，LPUSH/RPOP |
| `fairq:v1:task:<id>` | Hash | msg、group、state、retry_count、token、时间、错误 |
| `fairq:v1:active` | Set | 全局执行中的 TaskID |
| `fairq:v1:lease` | ZSet | TaskID → 租约到期毫秒 |
| `fairq:v1:retry` | ZSet | TaskID → 重试到期毫秒 |
| `fairq:v1:archived` | ZSet | TaskID → 归档时间 |
| `fairq:v1:bad_refs` | List | **最小异常证据**，不引入 incident-id/Hash/新任务主状态 |
| `fairq:v1:completed` | ZSet | 可选：成功任务 Retention 到期时间（默认关闭） |
| `fairq:v1:stats:*` | String/Hash（可选） | 默认不创建；优先使用 Prometheus 指标 |

空 Pending List 被 Redis 自动删除；最后一条被领取时 Group 立即退出 ready/members。**不做** LastUsed、Idle ZSet、TTL、Group GC。`bad_refs` 的证据必须先持久化再移除错误引用；未处理证据不自动删除，容量阈值告警；只要证据仍可写入就继续隔离；写入失败时保留原引用、返回 INTERNAL，禁止静默丢失。

`GroupKey` 建议原值 ≤256 bytes，使用 base64url 无填充编码。TaskID 全域唯一，Go 生成 UUID 或业务指定稳定 ID；Lua 内原子校验冲突。`attempt_token` 每次 Dequeue 由 Go 生成 UUID，成功领取时写 Hash，并随返回结果交给 Worker；Inspector 不暴露他人的 token。只支持 Redis 单实例/Sentinel，初始化拒绝 Cluster。

### 4.1 数据不变量

1. ready 中 Group 不重复；members 与 ready 的 Group 集合一致。
2. 非空 Pending 必须对应 ready/members；空 Pending 不得占据轮转。
3. 正常任务只在 Pending、Active、Retry、Archived、Completed（可选）之一拥有主状态；成功删除后 Hash 不存在。
4. Active 同时存在 Lease 和 Hash.state=active，Hash 保存当前 attempt_token。
5. Retry Forward/Recoverer 重新写入 Pending 时必须原子激活 Group。
6. 异常处理不得破坏其他合法 Active/Lease/Hash；无法判定时保留原状态、告警、交人工处理。

## 5. 任务状态转换

| 原状态 | 事件 | 新状态 | 同一 Lua 内修改 |
|---|---|---|---|
| 不存在 | Enqueue | pending | HSET Hash、LPUSH Pending、SADD members，新增才 RPUSH ready |
| pending | Dequeue | active | LPOP ready、RPOP Pending、HSET token/state、SADD active、ZADD lease、Group 归队或退出 |
| active | Done（Retention=0） | 删除 | 校验 token/Lease；SREM active、ZREM lease、DEL Hash、计数 |
| active | Done（Retention>0） | completed | 校验 token/Lease；移除 active/lease、HSET state、ZADD completed |
| active | Handler error | retry/archived | retry_count+1、移除 active/lease、写 Retry/Archived |
| active | Lease 过期 | retry/archived | 校验过期与当前 state，retry_count+1、移除 active/lease、写 Retry/Archived |
| retry | Forwarder 到期 | pending | ZREM retry、HSET pending、LPUSH 原 Group Pending、激活 Group |
| completed | Retention 到期 | 删除 | ZREM completed、DEL Hash |
| archived | 到期/容量清理 | 删除 | ZREM archived、DEL Hash，遵守保留策略 |

**不引入 `quarantined` 主状态**。Go 领取后消息解码失败，使用本次 attempt_token 调用受控 `active → archived`，原因 `DECODE_FAILED`，保留原始 msg；不再重试。索引损坏则记录 `bad_refs`，只隔离确认错误的引用，不改变合法任务状态。

### 5.1 Enqueue（单 Lua）

Go 校验编码与大小；Lua 先验证 TaskID 不存在、Key 类型及前置条件，然后创建 Hash、LPUSH Pending、SADD members，新增才 RPUSH ready。已活跃 Group 不改变轮转位置。请求结果未知时使用**同一 TaskID**查询/重试，不生成新 ID。

### 5.2 Dequeue（单 Lua，架构不变量）

```text
1. ready 为空 → EMPTY
2. 使用 LINDEX ready 0 和 LINDEX pending -1 只读取（**绝不提前 LPOP ready**）
3. 校验 Hash 存在且 state=pending，并验证必要主索引不冲突
4. 错误引用：先写 bad_refs 证据，再 RPOP 移除已确认错误的 Pending 队尾引用；
   若 Pending 已空：LPOP ready、SREM members；若仍非空：ready 队首原位保留，**不 LPOP、不 RPUSH**；
   返回 CORRUPT_REFERENCE，本次不继续取任务
5. 正常：LPOP ready、RPOP Pending
6. HSET state=active, attempt_token=<Go UUID>、started_at
7. SADD active、ZADD lease
8. Pending 非空 → RPUSH ready Group；否则 SREM members Group
9. 返回 TaskID、Group、msg、attempt_token、lease_deadline
```

Dequeue 的 Group 选择、Pending 移除、Active/Lease 写入、轮转更新**不得拆成多个 Lua**。`members` 表示轮转资格，不在 Lua 中途临时 SREM。Redis Lua 执行期间其他命令不能交错，但运行时错误不回滚。Go 的消息格式解码不在 Lua 内完成。

损坏引用一次最多隔离一条并返回，不在 Lua 内无限循环；若无法可靠写入证据，不移除引用，返回 INTERNAL 并告警；仅达到容量预警阈值但写入成功时，继续隔离并告警，不切换错误码。错误引用指向合法 Active 时只移除错误 Pending 引用，绝不能删除合法 Active/Lease。

### 5.3 Worker 异常领取退避

`CORRUPT_REFERENCE` 不计 Handler 失败，连续第 10 次开始阻塞当前领取循环，延迟 `min(100ms × 2^min(n-10,4),1s)`；等待期间**不调用 Dequeue**，成功领取后计数清零。`EMPTY` 按 PollInterval 休眠；`INTERNAL` 限速重试并告警，不得忙等。

### 5.4 Heartbeat、Done 与过期 Worker

默认 LeaseDuration=30s，HeartbeatInterval=10s。Dequeue/Heartbeat/Recoverer 的 Lease 截止时间和过期判断统一使用 Lua 内 `redis.call("TIME")`（转换为 Unix 毫秒）；Go 仅传 LeaseDuration，不提供本地绝对时间。续租 Lua 验证 state=active、token 匹配、Active/Lease 存在、**当前 Lease 尚未过期**，然后更新到期时间；不可只用无条件 `ZADD XX`。Done/Retry 也验证 token 和未过期租约，防止租约过期但 Recoverer 尚未运行时旧 Worker 提交成功。

续租明确失去所有权时，Worker 取消 Handler Context，停止提交 Done/Retry；Handler 应响应取消。Redis 暂时不可用时不得把网络失败当成任务已取消或已完成，持续无法证明持有租约时取消 Handler，等待恢复器处理。外部副作用仍需业务幂等。

### 5.5 Retry、Recoverer 与 Forwarder

**FairQ 自有默认参数**：MaxRetry=25，base=15s，max_delay=1h，jitter=10%。首次执行 `retry_count=0`。每次 Handler 失败或 Lease 过期：`retry_count += 1`；若 `retry_count > 25` 归档，否则进入 Retry。第 25 次失败允许最后一次重试，第 26 次失败归档；Hash 保留最终 26。退避以本次失败次数 `k` 为输入：`capped=min(base×2^(k-1),max_delay)`，`delay=capped+Uniform(0,0.1×capped)`；抖动后总延迟可能达到 1.1h。计算须防整数溢出，注入 Clock/RNG 便于确定性测试。

Recoverer 发现过期 Lease 后在 Lua 内使用 Redis TIME 重新验证 Lease 时间、Hash.state、Active 存在与当前尝试；合法过期任务进入 Retry/Archived，**不立即放回 Pending**。Retry 到期时间必须是 `RedisNowAtRecovery + backoff(retry_count)`，不得从旧 Lease 到期时刻计算。Forwarder 到期后使用 `LPUSH` 将 Retry 放回原 Group Pending 的**写入端**，排在现有待领取任务之后（`RPOP` 从另一端消费）。原子激活：`if SADD members group == 1 then RPUSH ready group end`，**无需 SISMEMBER，已活跃 Group 不重复入 ready**。重试/恢复重新入队不保留原始 FIFO 排名。重复扫描允许，重复状态转换不允许。

### 5.6 后台维护器与清理

ForwarderInterval=1s，RecoverInterval=10s，RetentionCleanupInterval=60s，BatchSize=100，MaxBatchesPerTick=5，批间建议退避 10ms；每实例独立配置并加启动抖动。每 Tick 最多 500 条候选/转换尝试，避免单实例无界循环。多个实例可重复扫描，但 Lua 只有一个能成功转移；监控扫描次数、实际转换次数、过期滞留时长、Redis p99。异常 Lease 引用不能永远占据扫描队首：记录证据并只移除可确认错误的引用，无法确认时告警并由运维处理。

成功任务默认直接删除；启用 Retention 时 Completed 到期分批删除。Archived 默认按时间保留 90 天；10000 条是**容量告警阈值**，不是自动按数量裁剪或拒绝归档的条件。到期时按批删除 Hash 与索引；若业务要求审计保留更久，须调高保留期或可靠转储。`bad_refs` 不自动清理未处理证据，达到阈值告警，证据仍可写入则继续隔离；真正写入失败才返回 INTERNAL。没有 Group Idle Cleaner。

### 5.7 Shutdown

停止新领取，**在 ShutdownTimeout 内继续为在途 Handler 续租**并等待完成；ShutdownTimeout 到期取消 Context 并停止相应续租，不删除未完成 Active/Lease，等待租约恢复。优雅关闭不能保证外部操作不会重复；业务幂等仍必须实现。

## 6. 错误处理、异常证据与最小对账

### 6.1 Worker 错误码

| 错误码 | 含义 | 行为 |
|---|---|---|
| `EMPTY` | 当前没有可领取任务 | PollInterval 后重试，不计失败 |
| `TASK_ID_CONFLICT` | Enqueue ID 已存在 | 返回调用方；可查询原 Task |
| `CORRUPT_REFERENCE` | 一条错误索引引用已安全隔离 | 继续领取；连续异常限速 |
| `STATE_CONFLICT` | token、state 或所有权不匹配 | 放弃旧尝试，记录日志；不得强行重试该转换 |
| `LEASE_EXPIRED` | 本次执行租约已失效 | 取消 Handler；等待 Recoverer |
| `DECODE_FAILED` | Go 无法解码消息 | 当前有效尝试原子归档，保留原始消息 |
| `INTERNAL` | Lua/Redis/未知错误 | 限速、告警、检查，不得假定事务回滚 |

### 6.2 最小异常处理原则

**不开发完整 Quarantine/Incident/Repair 平台**，但生产底线不可删：

1. Pending 指向缺失 Hash、或指向其他合法状态的任务：同一 Dequeue Lua 先记录 `bad_refs` 证据，再删除**错误的 Pending 引用**，不改其他合法状态；Group 索引同步维护。若无法确认该引用错误，保留原数据并报警。
2. Recoverer 发现 Lease 孤儿：原子记录证据，只清除确认无合法所有者的孤儿索引；不能无条件删除仍可能合法的 Active。
3. 消息编码损坏：Dequeue 已合法领取，Go 解码失败，带当前 attempt_token 原子 `active → archived`，保留 Hash.msg 与错误原因；失败转换时保持现场并报警。
4. 只读 Inspector 提供按 TaskID、Group、Active/Lease/Retry 与轮转索引的查询及分批一致性检查。异常修复首版由**受控运维脚本**逐条完成，不提供自动批量修复 API；修复前备份证据、核验现态、记录操作者与结果。
5. `bad_refs` 无自动过期、不得 `LTRIM` 丢未处理记录。运维处理后导出并人工清理已确认条目。达到容量预警阈值时告警但继续保存证据与隔离；仅证据写入失败时停止该次隔离、返回 INTERNAL 并限速告警。不得以丢失证据换吞吐。

### 6.3 对账范围

分批扫描，不在 Dequeue 热路径全局遍历：ready/members 是否一致、非空 Pending 是否可达、Task Hash 与主状态索引是否对应、Active 与 Lease 是否匹配、Retry/Archived 是否存在孤儿、bad_refs 是否积压。Inspector **只读**；禁止在查询时自动修改 Redis。生产禁止 `KEYS` 全量扫描，使用 SCAN/SSCAN/ZSCAN 及分批 List 检查。对账是异常治理而非日常领取的前置条件。

## 7. API 与 Go 工程结构

```go
type Task struct {
    ID      string // optional; default UUID
    Type    string
    Payload []byte
}
type EnqueueOptions struct {
    GroupKey  string // required
    MaxRetry  int
    Timeout   time.Duration
    Retention time.Duration
}
type Handler func(context.Context, Task) error

type Client interface {
    Enqueue(context.Context, Task, EnqueueOptions) (TaskInfo, error)
    GetTask(context.Context, string) (TaskInfo, error)
}
type ServerConfig struct {
    Concurrency       int
    LeaseDuration     time.Duration
    HeartbeatInterval time.Duration
    RecoverInterval   time.Duration
    ShutdownTimeout   time.Duration
}
```

建议包结构：`client/`、`server/`、`internal/rdb/`、`internal/base/`、`internal/clock/`、`internal/inspector/`、`internal/testutil/`。Lua 按 `enqueue.lua`、`dequeue.lua`、`done.lua`、`retry.lua`、`forward.lua`、`recover.lua`、`heartbeat.lua` 分类；不要为了共享三行 Group 激活逻辑强行合并成一个巨型脚本。所有状态变更脚本必须遵循统一的不变量与错误码。

生产环境必须支持 Go context、连接超时、Redis ACL/TLS、日志关联 TaskID/Group/attempt（不打印完整 token）、结构化指标；禁止把 Payload 明文输出到日志。

## 8. Redis 一致性与故障边界

- Lua 的原子执行指脚本运行期间其他客户端命令不交错；**运行时命令错误不会自动回滚已完成写入**。脚本先校验可预测错误（状态、类型、存在性），再执行短小的写入，失败时告警与对账。
- Redis 7 的脚本命令效果传播不能被描述为“Lua 内每条命令独立持久化”；进程崩溃、AOF/RDB 恢复和 Sentinel 切换需分开演练。不能把客户端超时当作脚本未执行。
- 使用 Redis 单实例或 Sentinel，初始化显式拒绝 Cluster；Redis 使用 `noeviction`，评估 AOF fsync、备份、ACL、网络隔离与可接受 RPO/RTO。建议独立 Redis 实例或至少独立命名空间、访问权限与容量预算。
- 生产主机启用时间同步。时间戳统一 Unix 毫秒；所有 Redis 内 Lease/Retry 绝对截止时间均由 Lua 内 Redis TIME 计算，Go 传 duration；测试使用可控时钟接口和真实 Redis 进行确定性验证。续租与过期比较必须使用同一时间语义，避免机器时钟偏差。
- 恢复后先运行只读检查；不可通过清空 ready/active/lease 强制“修复”。对未知状态不得自动重新执行。

## 9. 默认参数与容量预算

| 参数 | 默认值 | 说明 |
|---|---|---|
| LeaseDuration | 30s | 租约有效期 |
| HeartbeatInterval | 10s | 应小于 LeaseDuration，批量续租 |
| MaxRetry | 25 | 首次执行之外允许 25 次重试 |
| RetryBase | 15s | 第一次失败退避基数 |
| RetryMaxDelay | 1h | 指抖动前指数退避上限 |
| RetryJitter | 10% | 正向抖动 |
| ForwarderInterval | 1s | 每实例 |
| RecoverInterval | 10s | 每实例 |
| RetentionCleanupInterval | 60s | 只清理保留的终态任务 |
| BatchSize | 100 | 单批候选上限 |
| MaxBatchesPerTick | 5 | 每实例每 tick 最多 500 候选 |
| BatchPause | 10ms | 批间退避 |
| ArchivedRetention | 90d | 到期清理 |
| ArchivedAlertCount | 10000 | 容量预警；不拒绝归档、不按数量自动裁剪 |
| MaxPayload | 256KiB | 任务消息体限制，可配置 |
| GroupKeyMaxBytes | 256 | 原始 GroupKey 最大长度 |
| BadRefsAlertThreshold | 10000 | 达阈值告警；只有证据写入失败才停止隔离 |

基准**目标负载而非实测能力**：1000 个活跃 Group、10 万 Pending、10 个 Worker 进程 × 80 并发。需要记录实际 Redis 规格、平均 Payload、网络 RTT、内存占用、吞吐和 p99；不凭经验承诺容量。

## 10. 可执行验收标准

### 10.1 功能与公平性（全部通过）

- [ ] 3 个持续非空 Group 各 100 个任务，任意连续 3 次**成功领取**各组恰好 1 次；组内按成功入队顺序 FIFO。
- [ ] A=10000、B=1000、C=1000；B/C 尚有 Pending 且无异常的固定活跃窗口中任意连续 3 次成功领取 A/B/C 各一次；B/C 耗尽后不再套用此约束。
- [ ] 新 Group 加入尾部、空 Group 立即退出、再入队重新激活；ready 无重复，members 与 ready 一致。
- [ ] 同一空 Group 100 个并发 Enqueue，Group 只出现一次；重复 TaskID 只创建一份任务。
- [ ] 800 个并发领取槽，单个 Pending 引用最多被成功领取一次；重试导致的后续再次执行另行计数。
- [ ] 正常 Done、Retry、Archived 查询一致；若启用可选 Retention，再验证 Completed 查询；第 25 次失败仍重试，第 26 次失败归档，Hash.retry_count=26。

### 10.2 故障与异常（全部通过）

- [ ] Enqueue/Dequeue/Done/Retry/Forwarder/Recoverer 注入断连、超时、Lua 错误，未发生静默丢失；状态不一致能被 Inspector 发现。
- [ ] `kill -9` Worker 后，Lease 过期 → Recoverer 检测 → Retry 退避到期 → Forwarder → 可重新领取；恢复时间包括**检测与退避**，不误写成仅 RecoverInterval。
- [ ] 旧 Worker 的 Done/Heartbeat/Retry 在 token 不匹配或租约已过期时拒绝；Handler Context 取消。
- [ ] Pending 缺 Hash、Pending 错指合法 Active、孤儿 Lease、消息解码失败：不误伤合法任务，记录可追溯证据，后续正常任务不永久阻塞。
- [ ] bad_refs 达预警阈值时告警并继续隔离；证据写入失败时不删除原引用、返回 INTERNAL；只读 Inspector 不修改任何 Key。
- [ ] 10 个**独立 Go 进程**并发运行 Forwarder/Recoverer，共享同一 Redis，注入 1000 个到期任务；每条状态转换最多一次，记录扫描/转换比。
- [ ] 使用可控 Clock/RNG 测试退避边界、续租过期、恢复时序；执行 `go test -race ./...`。
- [ ] Redis 重启、AOF 恢复、Sentinel 切换分别演练并记录实际 RPO/RTO；Cluster 初始化明确拒绝。

### 10.3 性能与稳定性（目标门槛，需环境确认）

- [ ] 1000 Group、10 万积压、800 执行槽持续 60 分钟，无永久丢失、无轮转索引破坏、无异常 goroutine 增长。
- [ ] 同机房基准下 Dequeue Lua p99 <10ms、端到端 Redis 命令 p99 <30ms；如硬件不满足，先实测并批准修订阈值。
- [ ] Redis 内存峰值低于可用预算 75%，无 eviction/OOM；恢复/转发批量不造成明显的 Redis p99 长尾。
- [ ] 停止生产者且没有永久异常任务后，Pending/Retry 最终清零；记录吞吐、积压清空时间、失败率、恢复延迟。

### 10.4 上线门禁

- [ ] Lua/状态机集成测试使用**真实 Redis**（redis-server 或 testcontainers），不能仅用 mock/miniredis；多进程竞争与故障注入通过。
- [ ] Prometheus 最小指标：enqueue/dequeue/done/retry/archive、active/lease/retry 深度、ready Group 数、bad_refs 深度、维护器扫描/转换次数、Redis p99、最老 Pending 等待时长。
- [ ] 完成 Redis ACL/TLS、备份恢复、故障切换、容量预算、告警、值班 Runbook。
- [ ] 灰度从少 Group、低并发开始；业务封装层支持新任务路由回 Asynq。**切换仅影响新任务**，不能直接把 FairQ Redis Key 交给 Asynq 消费；旧 FairQ 任务须继续排空或按审计迁移。

## 11. 实施顺序

1. Redis Key、Task 编码、Enqueue Lua、动态 Group 激活和去重测试。
2. 单 Lua Round Robin Dequeue、空 Group 退出与公平性多进程测试。
3. Active/Lease、attempt_token、Heartbeat、Done 与 Worker Context/Shutdown。
4. Retry/Archive、Forwarder/Recoverer、批次限制与确定性 Clock/RNG。
5. **最小异常证据与只读 Inspector**：损坏引用、解码失败归档、孤儿 Lease；不开发 Incident/Repair 平台。
6. 指标、真实 Redis 故障注入、长稳压测、备份恢复、灰度上线。

## 12. 变更管理与生产责任

本文件冻结核心架构、状态机、Key、错误码和验收契约。新增任务主状态、新 Key 或改变 Dequeue 原子边界必须走设计变更。**P0**（丢任务、重复领取、合法状态被误删、轮转不变量破坏、不可恢复阻塞）必须修复并重跑相关验收，**不得记录到后续版本后带病上线**。非关键性能/观测改进可排后续版本。

生产就绪不是文档属性，必须由真实 Redis 的并发、故障、持久化、长稳和运维演练结果证明。


## 13. v1.4 编码契约补充（本章与前文同等约束）

### 13.1 权威时钟与恢复基准

在 Enqueue 记录入队时间、Dequeue 创建 Lease、Heartbeat 延长 Lease、Done/Retry 判断 Lease 是否失效、Recoverer 判断过期和计算 Retry 截止时间、Forwarder 判断到期时，**涉及 Redis 内状态判定的绝对时间均来自同一 Redis 主节点的 `TIME`**。Go 传入 duration（毫秒）和随机退避时长，不传本地绝对到期时间。`retry_at = RedisNowAtRecovery + backoff(k)`；Handler error 时 `retry_at = RedisNowAtFailure + backoff(k)`。

### 13.2 bad_refs 两级容量与错误码

`BadRefsAlertThreshold=10000` 只是监控阈值，不是 Redis List 的硬上限。达到阈值继续写证据并告警。仅在 Redis 写入错误、内存耗尽或脚本无法保证证据保存时，**保留原引用**，返回 `INTERNAL`，Worker 限速重试并触发高优先级告警；成功写入证据并隔离引用返回 `CORRUPT_REFERENCE`。Redis 内存耗尽是全局存储故障，须暂停新 Enqueue、保护在途状态并扩容/恢复，不能靠阻止 Archived 状态转换解决。

### 13.3 Forwarder 的 Group 激活

Forwarder 与 Enqueue 使用相同原子规则：`LPUSH pending taskID` 后，`if redis.call('SADD', membersKey, group) == 1 then redis.call('RPUSH', readyKey, group) end`。不先 `SISMEMBER`；无论 Group 在 Retry 期间是否重新被 Enqueue 激活，ready 中始终最多出现一次。

### 13.4 Shutdown 与任务所有权

收到关闭信号先停止领取，继续 Heartbeat 直到在途 Handler 完成或 ShutdownTimeout 到期。超时后取消 Handler Context，停止为其续租，保留 Redis Active/Lease 等待恢复。旧 Handler 可能忽略取消并继续产生副作用，因此仍需业务幂等。

### 13.5 生产故障验收专项

- [ ] 构造 Group A 只有一条损坏 Pending 引用：隔离后 `ready` 与 `members` 都不含 A；若 A 仍有其他任务，则保留 A 在 ready 首位，下一次可领取。
- [ ] 并发 Enqueue + Forwarder 激活同一 Group（10 个独立进程、同一 Redis），ready 中 Group 只出现一次。
- [ ] 控制 Recoverer 检测延迟：Lease 在 t=0 到期、Recoverer 在 t=10s 处理，第一次退避 15s，`retry_at >= t=25s`（不含正向抖动）。
- [ ] 在 Worker 进入 Shutdown 后 Handler 仍运行期间，Heartbeat 持续续租；超时后停止续租并可恢复。
- [ ] bad_refs 达告警阈值但写入成功仍能隔离；写入失败时原引用不变、返回 INTERNAL。
- [ ] Archived 达数量告警阈值仍能归档；Redis noeviction 内存不足时明确报存储故障，不静默丢任务。
- [ ] 真实 Redis 下模拟 10 万条 Lease 候选并限制批量扫描，记录 Redis p99 与恢复耗时；这属于极端压力测试而非正常 800 并发规模。

### 13.6 实施优先级与变更控制

优先完成 Group Round Robin、单 Lua Dequeue、Lease/Heartbeat、Retry/Recoverer、最小 bad_refs 与并发测试。`Completed/Retention` 默认关闭；Redis 内持久累计 stats 可延后，使用 Prometheus。文档冻结后发现丢任务、重复领取、合法任务状态被破坏等 P0 问题，必须修复并重跑验收，不能仅记录到后续版本后上线。
