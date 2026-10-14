# FairQ v1.4 编码契约

唯一需求基线：[v1.4 原文](fairq-final-technical-design-v1.4.md)。本文件只细化实现，不增加 Redis Key、任务主状态或后台组件。生产就绪须通过 [验收](acceptance.md)。

## 1. 七项契约决议

| ID | 决议 | 验证 |
|---|---|---|
| C1 去重期限 | Task Hash 存在期间全域 TaskID 去重；默认 Done 删除 Hash，之后同 ID 可再次入队。不承诺永久去重。未知 Enqueue 结果且 Hash 已消失时，无法区分完成/未入队，业务使用稳定 ID 幂等后重试。不得添加墓碑 Key。 | A02、F01 |
| C2 未知入队结果 | 在任何 Redis 调用前生成 ID；Enqueue 即使返回错误也返回 TaskInfo.ID，状态仅在确认后填写。重复调用使用返回的同一 ID。输入非法则不调用 Redis。 | A02、F01 |
| C3 恢复候选 | 扫描只是提示。Recover Lua 重验 state、Active、当前 token、retry_count、Lease score 和 Redis TIME；Go 传扫描到的 token/count/score 与基于 count+1 的随机 duration，任一快照不符即 STATE_CONFLICT，无写入。 | R03 |
| C4 部分写入 | 所有可预测类型/字段/参数错误在第一笔写入前校验；Lua 不回滚。运行时错误返回 INTERNAL，保留 Hash/msg，告警、Inspector 对账、受控人工修复，不盲目补偿或重新执行 Handler。错误证据写入用 pcall，失败不得删引用。 | F01、F02 |
| C5 损坏分类 | 仅隔离可证明错误的引用，见 §5；矛盾/未知现场保留并告警。维护器有界分页并推进内存游标，不能每 tick 反复只查同一坏队首。 | X01–X06、R04 |
| C6 零值 | 保持 v1.4 API：MaxRetry=0 采用默认25，正数为指定重试上限，负数拒绝；本版不提供显式零次重试选项。Timeout=0 无 Handler 时间上限，Retention=0 成功删除，负数拒绝。Concurrency、ShutdownTimeout 必须显式为正。已有文档默认参数零值采用默认；PollInterval 实现默认1s。 | A01、W01 |
| C7 时钟测试 | Redis TIME 是唯一状态绝对时间。Go Clock/RNG 只控制等待/退避；真实 Redis 测试通过预置 score、Redis TIME 前后包围断言及短租约验证，禁止向生产 Lua 注入假绝对时间。 | L01、R01、R03 |

C1 是由原文“存在性去重＋成功删除＋At-least-once”直接得到的边界，不把永久去重作为新需求。C6 的未定义零值选用最小一致解释，不改变公开字段类型。发现实现无法保留可追踪现场、造成重复领取或破坏合法任务时，标记 P0 并阻断阶段验收，不能用上述边界掩盖缺陷。

## 2. 存储与编码

固定前缀 `fairq:v1:`，只支持单实例/Sentinel，初始化拒绝 Cluster。GroupKey 为非空 UTF-8，原始长度≤256 bytes，base64.RawURLEncoding 编码；ready、members、Hash.group 均存编码值，API 返回原值。TaskID 为有效非空 UTF-8 字符串，未指定则生成 UUID；不同业务任务不复用同一稳定 ID。

| 后缀 | 类型 | 内容 |
|---|---|---|
| groups:ready | List | Group；LPOP/RPUSH |
| groups:members | Set | 当前轮转 Group |
| pending:<encoded-group> | List | TaskID；LPUSH/RPOP |
| task:<id> | Hash | 下表字段 |
| active | Set | TaskID |
| lease | ZSet | TaskID → 到期 Unix 毫秒 |
| retry | ZSet | TaskID → 可转发 Unix 毫秒 |
| archived | ZSet | TaskID → 归档 Unix 毫秒 |
| bad_refs | List | JSON 最小证据，无自动清理 |
| completed | ZSet | 可选；TaskID → 保留到期 Unix 毫秒 |

不创建 stats:*、Group TTL、注册表或其它 Key。保留 completed 是原文可选能力，默认关闭。

Hash 必需字段：`msg`（JSON，type 字符串、payload 标准 base64）、`group`、`state`、`retry_count`（初始0）、`max_retry`、`timeout_ms`、`retention_ms`、`enqueued_at`、`pending_since`。后续字段：`attempt_token`、`started_at`、`failed_at`、`finished_at`、`error`。首次入队没有 token，领取生成新 UUID；后续转换保留最后 token 以识别旧候选。Retry/Lease/终态截止时间以对应 ZSet 为准，不在 Hash 复制截止时间。

Task.Type 非空；Payload 原始字节默认≤256KiB，JSON/base64 的额外内存计入容量测试。正 duration 必须可表示为正整数毫秒，Lua 数值计算不超过精确整数范围；禁止负数、溢出、非有限值。msg 的解码在 Go 进行，失败保留原始 msg。错误文本与日志不包含 Payload 或完整 token。

## 3. 数据不变量与状态表

- I1：ready 无重复；members 等于 ready 的 Group 集合。
- I2：非空 Pending 可从 ready/members 到达；空 Pending 不占轮转。
- I3：正常 Task 只有一个主索引；Hash.state 与其匹配；成功删除后 Hash 消失。
- I4：Active、Lease、Hash.state=active 和非空当前 token 同时成立。
- I5：Forward 原子 LPUSH 后用 SADD 的返回值决定是否 RPUSH ready；恢复先进入 Retry，不直接 Pending。
- I6：隔离只删除已确认错误引用，不改合法任务的 Hash/Active/Lease；未知现场保留。

| 原状态 | 操作 | 新状态 | 原子变更 |
|---|---|---|---|
| 不存在 | Enqueue | pending | Hash＋Pending＋必要 Group 激活 |
| pending | Dequeue | active | Pending 移除＋token/Hash＋Active/Lease＋轮转 |
| active | Heartbeat | active | 仅续 Lease |
| active | Done，Retention=0 | 删除 | Active/Lease/Hash 删除 |
| active | Done，Retention>0 | completed | 移除 Active/Lease，写 Hash/Completed |
| active | Handler error / Lease 过期 | retry 或 archived | 失败次数+1；移除 Active/Lease；写目标状态和索引 |
| active | 解码失败 | archived | 校验本次有效所有权；保留 msg，error=DECODE_FAILED；不再次重试，不增加 Handler 失败计数 |
| retry | Forward 到期 | pending | 原组写入端入队＋原子激活，FIFO 排在已有任务之后 |
| archived/completed | Cleaner 到期 | 删除 | 重验终态后删 Hash/对应索引 |

Lease 有效当且仅当 `deadline > RedisNow`，等于即过期；Retry/清理到期为 `score <= cutoff`。过期恢复与 Handler 失败均先 k=count+1，k>max_retry 归档，否则重试。退避 `min(15s*2^(k-1),1h)+Uniform(0,10%*capped)`，正向抖动后最多1.1h；先饱和再乘，防溢出。

## 4. Lua 通用协议

Go 使用固定脚本和参数列表，不拼接用户脚本。Lua RESP 数组首项为 code；`OK` 只表示成功标记，不是新增错误码。数据项为字符串/整数，禁止用 nil 造成数组截断。Redis 传输/脚本异常统一为 INTERNAL，保留 underlying error 和操作名供脱敏诊断。

每次脚本读取一次 TIME：`sec*1000+floor(usec/1000)`。先验证所有将读写 Key 类型（不存在可接受），参数、Hash 字段、目标索引不冲突，再写入。业务前置条件失败无写入。同一次脚本的写入不会被其它客户端插入；不可据此宣称异常时回滚。

所有变更脚本关闭客户端透明网络重试。NOSCRIPT 后加载再执行是缓存恢复，不重试未知已执行请求。结果未知时查询、告警或交 Recoverer；不能通过重新运行 Handler 获得结果。Heartbeat 未确认成功不延长本地保守所有权窗口；以请求发出时的单调时间＋LeaseDuration作为最晚本地有效界，超过即取消，迟到响应不得复活已取消尝试。Redis 判定仍是最终权威。

### 4.1 Enqueue / enqueue.lua

- 输入：KEYS=Task,Pending,Ready,Members,Active,Lease,Retry,Archived,Completed；ARGV=id,encoded_group,msg,max_retry,timeout_ms,retention_ms。
- 前置：Hash 不存在（存在返回 TASK_ID_CONFLICT）；此 ID 无已知主索引残留；Group 的本地 Pending/members 条件一致。全局 ready 重复由 Inspector 检查，热路径不全遍历。
- 修改：HSET pending/count=0/配置/Redis 入队时间；LPUSH Pending；SADD members 返回1才 RPUSH ready。
- 返回：OK,id,enqueued_at；冲突 TASK_ID_CONFLICT；类型或损坏 INTERNAL。
- 失败：校验失败不写；结果未知返回原 ID，按 C1/C2 查询；发现部分入队现场不得自动再 LPUSH。

### 4.2 Dequeue / dequeue.lua

- 输入：KEYS=Ready,Members,Active,Lease,Retry,Archived,Completed,BadRefs；ARGV=新token,lease_duration_ms。Task/Pending Key 在固定前缀内从当前头部引用派生（仅单机/Sentinel）。
- 前置：Ready 为空→EMPTY。先 LINDEX Ready 0 / Pending -1；Hash.state=pending、group匹配、字段可用于执行且无其它主索引。token 非空、duration>0。
- 修改：正常分支 LPOP ready、RPOP pending；HSET active/token/started_at；SADD active，ZADD lease=now+duration；Pending 非空则 RPUSH group，否则 SREM members。不得拆脚本。
- 返回：OK,id,encoded_group,msg,token,lease_deadline,timeout_ms,retry_count,max_retry；损坏走 §5 返回 CORRUPT_REFERENCE 或 INTERNAL。
- 失败：未知结果不再次运行同一领取请求；未确认返回的任务不执行，等待租约恢复。保留 Hash 作为现场。隔离一次后立即返回，不继续领取。

### 4.3 Heartbeat / heartbeat.lua

- 输入：KEYS=Active,Lease；ARGV=lease_duration_ms 及有界 (id,token) 列表；Task Key 固定前缀派生。
- 前置：逐任务校验 Hash active、token、Active/Lease存在、无冲突主索引、deadline>now。批量共享 Key 类型先统一检查。
- 修改：只更新通过校验任务的 Lease=now+duration；每个任务独立结果，不能把部分成功当作全批成功。
- 返回：每项 id,OK,new_deadline；否则 STATE_CONFLICT / LEASE_EXPIRED / INTERNAL。
- 失败：明确失权取消 Handler；网络错误不确认完成，也不确认续租。禁止单纯 ZADD XX。

### 4.4 Done / done.lua

- 输入：KEYS=Task,Active,Lease,Retry,Archived,Completed；ARGV=id,token。
- 前置：完整 active 所有权、token匹配、未过期；Retention 从 Hash 读取。
- 修改：移除 Active/Lease；retention=0 删 Hash，否则 HSET completed/finished_at，ZADD completed=now+retention。
- 返回：OK,deleted 或 OK,completed；失权 STATE_CONFLICT；到期 LEASE_EXPIRED。
- 失败：未知结果不宣称成功、不重复 Handler；查询原 Task。Hash 不存在无法证明是本次 Done，应记录结果未知；不得重建任务。

### 4.5 Retry / retry.lua

- 输入：KEYS=Task,Active,Lease,Retry,Archived,Completed；ARGV=id,token,expected_retry_count,backoff_ms,error,mode(error|decode)。
- 前置：与 Done 相同；count 必须等于快照；退避 duration 合法且与 k=count+1 的允许区间一致。decode 模式不依赖解码 msg。
- 修改：error 模式 count+1，移除 Active/Lease；写 retry+score=now+delay 或 archived+score=now；decode 模式直接 archived，保留 count/msg、error=DECODE_FAILED。不得按 Archived 数量裁剪或拒绝归档。
- 返回：OK,retry|archived,count,score；decode 成功仍为 OK（DECODE_FAILED 是归档原因）。
- 失败：STATE_CONFLICT/LEASE_EXPIRED 不写、不强制重试；INTERNAL 保留现场、告警。Worker 不与 Recoverer 共用允许过期提交的分支。

### 4.6 Recover / recover.lua

- 输入：与 Retry 的 Key 相同，另含 BadRefs；ARGV=id,expected_token,expected_count,expected_lease_score,backoff_ms。
- 前置：重验快照＋Hash active＋Active＋Lease到期；若当前 Lease 未过期返回 EMPTY；快照改变 STATE_CONFLICT；孤儿走 §5。
- 修改：与 Handler error 同一失败计数/归档规则，但 retry_at=本次 Redis TIME+delay；不触碰 Pending/Ready/Members。
- 返回：OK,retry|archived,count,score，或 EMPTY/STATE_CONFLICT/CORRUPT_REFERENCE/INTERNAL。
- 失败：不再消费旧候选；下次重新扫描；未知结果不能在 Go 手动改索引。

### 4.7 Forward / forward.lua

- 输入：KEYS=Task,Retry,Pending,Ready,Members,Active,Lease,Archived,Completed；ARGV=id,expected_token,expected_count,expected_retry_score,encoded_group。
- 前置：Hash retry、group和候选匹配；Retry score<=now；其它主索引无冲突；Group 前置条件与 Enqueue 相同。
- 修改：ZREM Retry，HSET pending/pending_since=now，LPUSH Pending；以 SADD 返回值激活 Group。
- 返回：OK,id；未到期 EMPTY；候选变化 STATE_CONFLICT；损坏 INTERNAL。
- 失败：未确认现场保留/告警，不能仅因 retry 索引存在就重复 LPUSH。多维护器竞争仅一个转换成功。

### 4.8 Cleaner / clean.lua

- 输入：KEYS=Task,目标终态ZSet,Active,Lease,Retry,另一终态ZSet；ARGV=id,state,expected_token,expected_score,archived_retention_ms。
- 前置：Hash 与索引均为目标终态、token/score 匹配、无 Active/Lease/Retry 冲突；archived score<=now-retention 或 completed score<=now。
- 修改：ZREM 对应终态索引、DEL Hash；不存在或矛盾现场不删除其它 Key。
- 返回：OK,id；未到期 EMPTY；候选变化 STATE_CONFLICT；损坏 INTERNAL。
- 失败：重新扫描，保留并报告矛盾；绝不因为一条过期索引就无条件 DEL Hash。

候选扫描使用有界只读命令/脚本，绝对到期过滤也用 Redis TIME。每批≤100，每 tick≤5批/500候选（含冲突、异常候选），批间10ms，启动抖动。分页位置保存在维护器内存，推进并周期回绕；可重复扫描，不允许重复转换。实现必须测试坏队首超过一批时仍能访问后续到期任务，不能误以为 SCAN COUNT 是硬上限。无选主、分布式锁或新增游标 Key。

## 5. 异常证据与安全隔离

bad_refs 每条为 JSON：Redis `at`、`operation`、`reason`、`source_key`、`task_id`（可空）、`group`（可空）、观测 `state`/`score`。不含完整 token 或 payload。证据必须在同一转换 Lua 中先 RPUSH 成功，再移除引用；“持久化”服从 Redis 的 AOF/复制边界，不额外承诺 fsync 或零 RPO。

| 现场 | 允许动作 |
|---|---|
| Pending 指向缺 Hash | 写证据后仅 RPOP 此队尾引用；维护当前 Group 空/非空规则 |
| Pending 指向其它完整合法主状态（特别是 Active） | 写证据后仅移除此 Pending 引用，不改 Task/Active/Lease |
| Ready 头部 Group 的 Pending 已空 | 写证据后移除该 ready 头和 members 资格，不改变任务 |
| Hash pending 但 group 不匹配、必要字段损坏、members 缺失、主索引矛盾 | 不能证明合法归属；原样保留，INTERNAL＋告警＋人工核验 |
| Lease 缺 Hash 且 Active 也不存在 | 写证据后仅 ZREM 该孤儿 Lease |
| Lease 指向完整合法非 Active 状态，且 Active 不含该 ID | 写证据后仅 ZREM 错误 Lease |
| Hash 缺失但 Active 存在，或 Hash active 而 Active 缺失 | 无法确认，保留全部现场，INTERNAL，分页继续检查其它任务 |
| msg 解码失败 | 已合法领取后执行受 token/未过期保护的 decode 归档，不移除其它尝试 |

阈值10000仅告警；证据成功且隔离完成才返回 CORRUPT_REFERENCE。证据写失败时返回 INTERNAL且原引用不变。证据重复允许，不自动 LTRIM/TTL。任何未知状态不自动重执行。

## 6. 错误码与 Worker 行为

| code | 写入含义 | 调用方行为 |
|---|---|---|
| EMPTY | 无可领取/到期对象，无转换 | Worker PollInterval 等待；维护器继续有界扫描 |
| TASK_ID_CONFLICT | Enqueue未创建新任务 | 返回原ID，查询现有任务 |
| CORRUPT_REFERENCE | 证据已存且仅错误引用已隔离 | 不计 Handler失败；连续第10次开始阻塞退避，成功领取清零 |
| STATE_CONFLICT | 业务校验不通过，无转换 | 旧尝试取消，不强制重试；维护器重扫 |
| LEASE_EXPIRED | Worker租约已到期，无转换 | 取消 Handler，停止提交，等待 Recoverer |
| DECODE_FAILED | Go解码错误原因 | 受控归档；归档失败保留现场并告警 |
| INTERNAL | 结果可能未知或部分写入 | 限速、告警、只读检查；不得假设回滚 |

CORRUPT_REFERENCE 延迟严格为 `min(100ms*2^min(n-10,4),1s)`；等待期间不调用 Dequeue。INTERNAL 使用可取消且有上限退避，避免忙等。Handler timeout/panic 作为失败进入 Retry；失权/Shutdown 超时不能冒充 Handler 失败提交。

关闭顺序：停止领取→继续 Heartbeat 等待在途 Handler→ShutdownTimeout 到期取消并停止相应续租→保留未完成 Active/Lease→Recoverer处理。完成/续租的本地登记按 (TaskID,token) 区分，旧尝试退出不能删除新尝试登记。忽略取消的 Handler 仍可能产生副作用，不能宣称 Exactly-once。

## 7. 验收与阻塞

阶段0完成仅表示上述文档契约自洽、可测试，不表示任何运行时验收已通过。目前没有必须新增 Key/主状态才能解决的已确认 P0 架构阻塞。部分写入、未知结果、并发恢复和长期负载仍必须由后续真实测试证明；测试发现 P0 必须修复并重验，不得标为“已知限制”上线。
