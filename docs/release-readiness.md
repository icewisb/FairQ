# FairQ 发布验收

日期：2026-10-14（Asia/Shanghai）。这是当前唯一发布报告；以源码为实现事实、v1.4 为契约、本次执行为测试证据。结论：**可以公开实验源码；可安装 v0.x、稳定版及生产就绪尚不通过**。

## 范围、发现与提交

从干净工作树 `1d0105e` 开始，通读全部 58 个 Go/Lua 文件（含测试、Fixture、示例和工具）、当前文档、依赖与许可证，并检查必要 Git 历史。v1.4 冻结文本来源为 `fe901d5:docs/fairq-final-technical-design-v1.4.md`；没有把此前报告的 PASS 计入本次。codebase-memory 索引启动失败（daemon coordination 无法确认），对全部 58 个路径的 coverage 查询同样不可用；以完整源码读取和调用点搜索补足，不宣称图索引验证成功。

| 级别 | 发现及实际影响 | 处理 |
|---|---|---|
| P0/P1 代码缺陷 | 本轮未确认可复现的任务丢失、重复原子领取、旧 token 覆写或合法状态被误删 | 不为凑数修改核心；不代表不存在未知缺陷 |
| P2 测试覆盖 | 原 RR 用例仅 10/2/2，不能作为 v1.4 指定样本已执行的证据 | 原函数改为 100/100/100 和 10000/1000/1000 两个子测试，逐次检查完整领取顺序 |
| P2 Fixture 可靠性 | Redis 命令启动失败时 `node.stop` 解引用 nil Process，清理 panic 掩盖启动错误；AOF 用例早退缺 Close | 保护未启动进程，新增失败清理回归，补充连接释放 |
| P2 测试生命周期 | 两处关闭测试靠 Sleep 推测进入关闭状态，失败时未释放阻塞 Handler | 等待实际 closing 状态和 Redis Lease 时间，失败也释放 Handler |
| P2 文档治理 | README 混入详细配置/API；缺唯一发布矩阵，历史门禁容易失去上下文 | 精简 README，接入内容移到 usage，设计与运维各归一处，本文件统一验收 |
| 发布门禁 | Module 为 `fairq` 且无 remote；正式 P01 复验和生产灰度未完成 | 保留原值，交维护者决定地址及后续验收，不推送或打 Tag |

本地提交：

- `3efd059 test: strengthen FIFO acceptance and lifecycle fixture cleanup`：仅三个测试文件，定向 race 与 Diff 检查后提交。
- `docs: consolidate usage and final release readiness`：README、使用/设计/测试指南与本报告；提交号由 `git log -1` 查询，避免报告自引用尚未生成的哈希。

无运行时代码、Redis Key、Lua 原子边界、公共 API、依赖或 LICENSE 变更；未创建 CI/CD、Release、Tag，未 push、重写历史或操作生产。缺少 CI 不作为阻塞项。

## 核心审计结论与边界

Enqueue 与 Forward 在同一 Lua 内入 Pending 并用 SADD 结果决定激活；Dequeue 的 Group 选择、FIFO 出队、Active/Lease/token 和轮转更新仍在一个脚本内。绝对时间来自 Redis TIME；Go 只传时长并采用保守的本地租约证明。Heartbeat/Done/Retry 验证 state、token、主索引和未过期 Lease；Recover 重新验证扫描快照，按恢复时刻计算退避。默认第 25 次失败仍重试、第 26 次归档；解码失败保留原消息直接归档。

bad_refs 先写证据再隔离已确认坏引用，失败则保留现场；未知冲突不猜测修复。Archived 只按保留期清理，数量阈值不裁剪。Shutdown 停领取、继续在途续租，超时取消后保留 Active/Lease；有缓冲的 Handler 结果通道和已停止 timer 避免正常退出路径悬挂。忽略取消的业务 Handler 仍可能累积，不能承诺强杀或严格限制这些残留执行。

Lua 热路径没有全局遍历；Heartbeat 单批最多 100，维护每轮最多 500 个扫描项，Cleaner 共享预算并交替处理两种终态。多个维护进程重复扫描属于设计成本；本次 race 的 10 进程 Recover 扫描/转换为 9827/1000，Forward 为 9864/1000，最终无重复转换。Inspector/指标使用分批扫描，但 Check 总内存随任务规模增长，指标抓取仍有全局扫描成本。没有本轮性能证据支持再加缓存、选主或改索引，保持原实现。

## 文档唯一归属

| 内容 | 权威位置 | 本次收敛 |
|---|---|---|
| 定位、安装、可运行 Quick Start、特性、限制、测试入口、License | [README](../README.md) | 删除重复嵌入式 serve 示例与详细 API；可运行源码统一指向 examples/basic |
| 公共 API、配置零值、Handler、未知写结果、Inspector 分页 | [使用指南](usage.md) | 从 README 迁入；补充 Start/Run 生命周期、List 顺序和 SCAN count 语义 |
| FIFO/RR、状态机、租约、存储、不变量与异常语义 | [架构说明](architecture.md) | 补精确 RR 窗口、维护预算和坏引用退避；不恢复另一份并行设计说明 |
| 部署、ACL/TLS、告警、受控修复、备份、灰度 | [运维手册](runbook.md) | 保持现有归属，不复制到 README |
| 测试环境、DB 隔离、日常命令 | [开发测试指南](test-environment.md) | 指向本报告的覆盖矩阵；不重复发布判定 |
| 显式负载与性能工具 | [tools/README](../tools/README.md) | 接入链接改到 usage；保留短测不等于长稳的限制 |
| 当前覆盖、结果、遗留项与 GO/NO-GO | 本文件 | 不新增分阶段或分轮次报告 |

按维护者本轮追加指令，历史失败证据和故障报告直接删除，不恢复、不归档；本轮基线中这些文件已由 `195c91b` 删除，因此无重复删除操作。Git 历史未改写。**历史 P01 仍为 FAIL，完整复验仍为 NOT_RUN**，删除附件不改变判定；此前采样修复不能替代正式长稳。

## 精简测试覆盖矩阵

COVERED 表示该行限定契约有直接断言；PARTIAL 表示仅覆盖部分场景；MISSING 表示没有所需用例。PASS/FAIL/BLOCKED/NOT_RUN 单独描述本次执行，不能互换。路径均相对仓库根目录。除单列门禁外，PASS 均指本轮普通及 race 两次全量执行；多进程 helper 的直接空返回不算独立验收。

| ID | 契约/风险 | 实现位置 | 测试函数 | 层级 | 覆盖状态 | 本次结果 | 证据/处理 |
| -- | ----- | ---- | ---- | -- | ---- | ---- | ----- |
| Q01 | 入队校验、并发去重、Hash 删除后 ID 可重用 | `client/client.go`、`internal/rdb/lua/enqueue.lua` | `client/client_test.go`: TestEnqueueConcurrentAndQuery、TestValidationAndUnknownID、TestTaskIDDedupEndsWhenHashIsDeleted | Redis | COVERED | PASS | 100 并发入队与同 ID 冲突，旧代 Done 被拒绝 |
| Q02 | 组内 FIFO、固定活跃窗口全局 RR | `internal/rdb/lua/dequeue.lua` | `internal/rdb/dequeue_test.go`: TestRoundRobinFIFO | Redis | COVERED | PASS | balanced=3×100；skewed=10000/1000/1000，逐次 ID 和排空索引断言 |
| Q03 | 原子领取不重复 | `internal/rdb/dequeue.go`、`internal/rdb/lua/dequeue.lua` | `internal/rdb/dequeue_test.go`: TestMultiProcessClaims | 多进程 | COVERED | PASS | barrier 同步 10×80 槽、1000 个唯一领取 |
| Q04 | Group 动态加入、退出、重新激活不重复 | `internal/rdb/lua/enqueue.lua`、`internal/rdb/lua/forward.lua` | `internal/rdb/roundrobin_test.go`: TestDynamicGroupsAndUnknownCorruption；`server/maintenance_test.go`: TestTenProcessMaintenance | Redis/多进程 | COVERED | PASS | Enqueue 与 Forward 同组竞争，ready/members 仅一份 |
| Q05 | Lease/Heartbeat 使用 Redis 时间，拒绝旧 token、过期及错误索引 | `internal/rdb/lua/common.lua`、`internal/rdb/lua/heartbeat.lua` | `internal/rdb/ownership_test.go`: TestOwnershipAndServerTime、TestExpiredCannotFinishOrRenew、TestMissingIndexAndNewAttemptProtection、TestOwnershipRejectsWrongIndexTypesBeforeWrites | Redis | COVERED | PASS | deadline≤now 失效；混合续租结果及类型错误前置检查 |
| Q06 | Done 删除或保留成功记录、拒绝重复提交 | `internal/rdb/lua/done.lua`、`internal/rdb/lua/clean.lua` | `internal/rdb/ownership_test.go`: TestOwnershipAndServerTime；`internal/rdb/clean_test.go`: TestRetentionAndSafeCleaner；`server/cleaner_test.go`: TestRetentionThroughPublicClientAndCleaner | Redis/端到端 | COVERED | PASS | Completed 可查询、到期删除、Cleaner 不误删新所有者 |
| Q07 | Retry 次数边界、退避和大整数 | `internal/base/retry.go`、`internal/rdb/lua/retry.lua` | `internal/base/retry_test.go`: TestBackoff；`internal/rdb/recovery_test.go`: TestRetryCountsAndForwardFIFO、TestFailureUsesCallerDelay、TestFailureRejectsInvalidDelayWithoutWrites；`internal/rdb/integer_test.go`: TestLargeRetryIntegersRoundTrip | 单元/Redis | COVERED | PASS | 25/26、1h 上限与 jitter、非法输入不写入 |
| Q08 | Forward 到期重排组尾且至多转换一次 | `internal/rdb/lua/forward.lua` | `internal/rdb/recovery_test.go`: TestRetryCountsAndForwardFIFO；`server/maintenance_test.go`: TestTenProcessMaintenance | Redis/多进程 | COVERED | PASS | 现有 Pending 先领取；1000 Retry +10 新任务，唯一 1010 Pending |
| Q09 | Recover 快照防竞态、从检测时刻计算退避 | `internal/rdb/lua/recover.lua`、`server/maintenance.go` | `internal/rdb/recovery_test.go`: TestRecoverChecksSnapshotAndRecoveryTime；`server/maintenance_test.go`: TestTenProcessMaintenance、TestBlockedHeadDoesNotStarveRecovery | Redis/多进程 | COVERED | PASS | 晚检测 10s 再加 15s；600 个坏队首不饿死后续任务；每 tick≤500 |
| Q10 | Archived 90 天保留、阈值不裁剪、终态清理不饥饿 | `internal/rdb/lua/clean.lua`、`server/cleaner.go` | `internal/rdb/clean_test.go`: TestArchiveThresholdDoesNotTrim、TestArchivedNinetyDayRetention；`server/cleaner_test.go`: TestCleanerDoesNotStarveCompletedBehindArchived | Redis/Worker | COVERED | PASS | 89/91 天边界与 10001 条不裁剪 |
| Q11 | bad_refs 先存证据、拒绝未知破坏、坏引用退避 | `internal/rdb/lua/common.lua`、`internal/rdb/lua/dequeue.lua`、`server/processor.go` | `internal/rdb/dequeue_test.go`: TestCorruptReference、TestEvidenceFailureAndThreshold；`internal/rdb/recovery_test.go`: TestOrphanLeaseAndDecodeArchive；`server/server_test.go`: TestCorruptionWaitReallyStopsDequeue | Redis/Worker | COVERED | PASS | 合法 Active 保留；写证据失败不移除；阈值后继续隔离；可控 Clock 验证等待 |
| Q12 | error/timeout/panic/decode 分流 | `server/processor.go`、`internal/rdb/lua/retry.lua` | `server/server_test.go`: TestFailureTimeoutPanicAndDecode | Worker | COVERED | PASS | 解码不达 Handler、原 msg 保留、panic 文本不泄露 |
| Q13 | Inspector 只读、分页和结构/数值异常诊断 | `inspector/inspector.go` | `inspector/inspector_test.go`: TestInspectorReadOnlyAndCoverage、TestCheckInvalidIndexScores、TestCheckInvalidNumericFields | Redis | COVERED | PASS | 205 任务跨页，DUMP 前后不变；在线非快照边界见 usage |
| Q14 | Shutdown 优雅续租、超时保留所有权、并发关闭 | `server/server.go`、`server/heartbeat.go` | `server/server_test.go`: TestShutdownKeepsHeartbeats、TestShutdownTimeoutLeavesOwnership、TestRunCancellationPreservesGracefulDrain | Worker/race | COVERED | PASS | 等待真实 closing 与原 Lease 截止时间，重复关闭完成 |
| Q15 | 失权/连接丢失取消与晚续租不可复活 | `server/processor.go`、`server/heartbeat.go` | `server/server_test.go`: TestLostOwnershipAndConnectionCancel、TestLateRenewalCannotRevive | Worker | COVERED | PASS | 不可证明租约时取消，不提交旧状态 |
| Q16 | Redis 未知写结果、断连、超时、Lua 部分写入 | `internal/rdb/rdb.go`、`internal/rdb/lua/` | `internal/rdb/fault_test.go`: TestTransitionFaultMatrix | 故障注入 | COVERED | PASS | 8 转换×6 模式=48；无透明写重试，部分写入可诊断而非假设回滚 |
| Q17 | kill Worker 与运行中 Redis 重启恢复 | `server/processor.go`、`server/maintenance.go` | `server/server_test.go`: TestKillWorkerRecovery；`tests/acceptance/restart_worker_test.go`: TestWorkerSurvivesRedisRestart | 进程故障 | COVERED | PASS | 包含真实 15s+jitter 退避，晚旧 Handler 不破坏新尝试 |
| Q18 | AOF/RDB/Sentinel 本地持久化演练 | `internal/rdb/rdb.go` | `tests/acceptance/infrastructure_test.go`: TestAOFRestartAndBackupRestore、TestSentinelFailover | 基础设施 | COVERED | PASS | 95 Hash 恢复、100 已复制确认任务切换后保留；不证明生产零 RPO |
| Q19 | ACL/TLS、noeviction OOM、拒绝 Cluster | `internal/rdb/rdb.go`、`internal/rdb/lua/enqueue.lua` | `tests/acceptance/security_test.go`: TestACLAndTLS、TestNoEvictionOOMAndClusterRejection；`client/client_test.go`: TestRejectClusterOptions | 基础设施 | COVERED | PASS | 外域 Key/错误主机名拒绝、实际 OOM 无驱逐、真实 Cluster 被拒 |
| Q20 | 时间与数值溢出不得先写后报错 | `internal/base/task.go`、`internal/rdb/lua/common.lua` | `internal/rdb/duration_test.go`: TestDequeueRejectsInvalidMetadataBeforeWrite、TestCorruptDurationRejectedBeforeWrite、TestMaximumDurationRoundTrip | Redis | COVERED | PASS | 缺字段、非法值、Go duration 上限及精确整数回归保留 |
| Q21 | goroutine 生命周期与采样准确性 | `server/processor.go`、`server/server.go`、`tools/loadtest/main.go` | `server/server_test.go`: TestWorkerLifecycle、TestShutdownTimeoutLeavesOwnership；`tools/loadtest/goroutine_test.go`: TestGoroutineCountExcludesDeadGoroutinesDuringGC、TestGoroutineCountTracksBlockedGoroutines | 单元/Worker | PARTIAL | PASS | 常规退出、GC 误计和真实阻塞计数通过；长期负载及慢拨号峰值未在本轮验收 |
| Q22 | Metrics 不泄露敏感标签、慢写不阻塞队列 | `internal/rdb/metrics.go` | `client/metrics_test.go`: TestMetrics；`internal/rdb/metrics_test.go`: TestMetricsSlowWriterDoesNotBlockQueue | 单元/Redis | COVERED | PASS | 旧慢 Writer 死锁回归保留 |
| Q23 | Quick Start 可运行、完成与关闭 | `examples/basic/main.go` | `examples/basic/main_test.go`: TestExampleCompletesAndShutsDown；另行 go run + SIGINT | 端到端/CLI | COVERED | PASS | Completed、退出码 0、Active/Lease 归零 |
| Q24 | 测试基础设施失败也可清理 | `tests/acceptance/infrastructure_test.go`: node.stop | 同文件 TestNodeCleanupAfterStartFailure | 单元 | COVERED | PASS | 模拟不存在二进制、重复清理无 panic |
| G01 | 正式 60min P01 与十万 Lease 性能门槛 | `tools/loadtest/main.go`、`tools/benchmark/main.go` | 显式工具 run；TestMetricsParsing、TestPercentiles 仅工具单元测试 | 长稳/性能 | PARTIAL | NOT_RUN | 按范围禁止执行；历史 P01 FAIL 不改判，单元 PASS 不等于门槛通过 |
| G02 | 公共 Module 外部安装 | `go.mod`、公共 imports | 尚无已确认远端安装用例 | 发布 | MISSING | BLOCKED | 等待目标 GitHub 路径；本地 replace/Quick Start 不是远端安装验收 |
| G03 | 生产业务幂等、网络/容量、告警、灰度回退 | `docs/runbook.md`、业务集成 | 尚无目标环境业务验收 | 生产 | MISSING | NOT_RUN | 用户操作生产与后续授权；本地基础设施用例不能替代 |

## 测试治理与执行成本

未删除测试函数或测试文件。复用现有 fixture，没有另建分阶段测试目录、跨包共享框架或为覆盖率扩写断言。

| 被合并/删除的重复内容 | 独立价值 | 覆盖未退化的证据 |
|---|---|---|
| TestRoundRobinFIFO 原 10/2/2 数据和手列 14 次顺序，替换为两个需求样本 | RR、FIFO、空组退出均有价值，原规模没有独立价值 | 新两子测试逐次验证 12300 次领取和最终索引；普通/race 均 PASS |
| TestRoundRobinFIFO 末尾单任务重新激活断言 | 与 TestDynamicGroupsAndUnknownCorruption 已有 B/A 再入队重激活重复 | 后者完整保留且本次 PASS，同时验证动态队尾顺序 |
| 两处推测 closing 的 Sleep | 时间经过不能证明调度已发生 | 改为受锁保护的状态等待；续租仍跨越原 Lease 到期，不缩成一次瞬时读 |

保留 48 个故障子测试、10×80 进程领取、10 进程维护、整数/时长/慢 Writer/GC 历史回归。剩余 Sleep 主要是有截止的条件轮询、Handler 模拟耗时，或验证“等待期间不领取/关闭后不续租”的负向观察窗口；未机械删除。两项真实恢复用例各约 15–17s，含契约规定退避，不为缩短全量耗时改成假时钟。长稳和完整性能工具未混入 go test。

## 本次实际执行

环境：macOS arm64，Go 1.27.1，Redis 8.0.2（本机原生 TLS 构建）。本轮新建专用 loopback `127.0.0.1:60792`，768MiB、noeviction、无持久化；基础设施测试另建临时实例，AOF 使用 appendfsync=always。未复用既有 Redis；验收后核对 PID/端口并关闭本轮实例。耗时为命令墙钟时间，包并行耗时不能相加。

复现时设置 `FAIRQ_TEST_REDIS` 为自己的可丢弃实例，`FAIRQ_REDIS_SERVER` 为支持 TLS 的 Redis 二进制；环境配置见 test-environment。命令均从仓库根目录执行。

| 命令 | 本次结果 | 耗时 |
|---|---|---|
| `go test -race ./internal/rdb ./server ./tests/acceptance -run 'Test(RoundRobinFIFO\|DynamicGroupsAndUnknownCorruption\|Shutdown.*\|RunCancellationPreservesGracefulDrain\|NodeCleanupAfterStartFailure\|AOFRestartAndBackupRestore)$' -count=1 -v -timeout 90s` | PASS，第一批定向 | 5.447s |
| `go test ./... -count=1 -v -timeout 120s` | PASS | 29.879s |
| `go test -race ./... -count=1 -v -timeout 120s` | PASS，无 race 报告 | 31.670s |
| `go vet ./...` | PASS | 0.201s |
| `gofmt -l` 检查全部跟踪 Go 文件 | PASS，输出为空 | 0.035s |
| `go mod verify` | PASS，all modules verified | 0.118s |
| `go mod tidy -diff` | PASS，无差异 | 0.043s |
| `go mod download` | PASS | 0.007s |
| `go run ./examples/basic -redis 127.0.0.1:60792` 后 SIGINT | PASS，Completed、退出 0、Active/Lease 空 | 1.929s |
| Markdown 本地文件/锚点、矩阵函数与 Diff 检查 | PASS，35 个本地目标（含 5 个锚点），27 行矩阵函数逐一与两轮日志核对 | 0.241s |

普通/race 各记录 60 个顶层 PASS、127 个子测试 PASS、0 FAIL、0 SKIP；其中 3 个顶层函数是子进程 helper，其父用例才提供有效验收。没有把测试数量当质量指标。race 的实际基础设施结果：AOF 95 Hash 保留、RTO 172.34ms；RDB 95 Hash 恢复、109.57ms；Sentinel 100 个已确认且验证复制水位的任务保留、1.812s；运行中 Worker 随 Redis 重启后第二次执行、16.067s（含真实退避）。这些都是本机演练观察值，不是 SLA。

## 开源检查与参考

- `module fairq` 与 imports 一致，但不具备已确认的 GitHub Module 路径；无 remote。没有擅改路径或宣称 `go get` 可用。
- 保留已有 MIT LICENSE，署名 2026 icew；不重新选择许可。源码未 vendor 依赖，本轮未移植第三方代码。依赖缓存中实际 LICENSE 核对：google/uuid v1.6.0 为 BSD-3-Clause，go-redis/v9 v9.19.0 为 BSD-2-Clause，xxhash/v2 v2.3.0 和 atomic v1.11.0 为 MIT。以后分发含依赖的二进制仍需附相应第三方许可。
- 当前文件与截至测试提交的全部可达历史 206 个不同 blob（另扫当前 69 个文件） 经私钥头、GitHub/常见服务 token、AWS Key、带凭据 URL、开发者绝对路径模式检查，无命中。测试固定 ACL 口令仅用于一次性隔离 fixture，TLS 私钥运行时生成。未回显凭据、未改写历史。定向模式检查不等于全面秘密检测，不覆盖不可达对象、外部日志或未识别的低熵口令。
- .gitignore 已排除 env、日志、coverage、二进制测试及 profile；当前无跟踪临时构建产物。工具 JSON 报告路径由用户指定，本次产物只在临时目录，不加入仓库。
- 未运行漏洞数据库扫描、独立权属调查、Go 1.24 最低版本、Windows 或其他 Redis 版本；不把依赖校验当作漏洞扫描，不扩大兼容性声明。

本轮实际读源码作对照：[Asynq v0.25.1 processor](https://github.com/hibiken/asynq/blob/v0.25.1/processor.go) 的缓冲结果与关闭、[rdb](https://github.com/hibiken/asynq/blob/v0.25.1/internal/rdb/rdb.go) 的 Dequeue/ExtendLease，以及 recoverer；FairQ 需保留自己的 Redis TIME、attempt_token 和超时保留 Lease 契约，不能直接照搬 ZAddXX 或 Shutdown 重入队。[GQM](https://github.com/benedict-erwin/gqm)（本地固定版本 `b090d225caa3a3d90a4e4bd5d17bb440f9067b7e`） 本地固定版本的 worker/reap/retry/dequeue 源码已读，其本进程 rrCounter 不是跨进程 Group RR；本轮固定 SHA 的目录/raw 网页读取失败，引用保留可访问的项目入口，未声称验证其最新分支或运行其测试。

[Redis Lua 官方文档](https://redis.io/docs/latest/develop/programmability/eval-intro/)与[无回滚说明](https://redis.io/blog/you-dont-need-transaction-rollbacks-in-redis/)支持执行隔离、短脚本和错误预检查的边界；[复制文档](https://redis.io/docs/latest/operate/oss_and_stack/management/replication/)说明异步复制的持久性限制。这些参考不作为 FairQ 测试 PASS 的证据。

## 四级判定与下一步

| 层级 | 判定 | 尚缺什么 |
|---|---|---|
| 公开源码 | **GO**，以 MIT 实验项目公开供评审和本地试用 | 本轮无已确认阻断源码公开的缺陷；维护者执行公开操作，本轮未 push |
| 可安装 v0.x 实验版 | **NO-GO（待发布配置）** | 确认仓库/Module 路径，统一修改 go.mod/imports，独立目录执行外部安装与 Quick Start；明确不承诺稳定 API/生产就绪 |
| 稳定版 | **NO-GO** | 上述发布验证、正式 P01 复验、目标版本与性能验收未完成 |
| 生产就绪 | **NO-GO** | 历史 P01 FAIL、正式复验 NOT_RUN；目标部署负载、真实业务幂等、告警值班、灰度及回退尚无验收 |

维护者需亲自决定：目标 GitHub owner/repo 与 Module 大小写；要发布实验版还是继续走稳定/生产门禁；之后在哪个隔离目标环境安排长稳与性能复验。LICENSE 已确定，不重复要求选择。

下一步先确认公开仓库地址并完成 Module/外部安装验证，即可准备实验版；稳定或生产使用另行授权正式 60 分钟 P01、完整性能测试和业务灰度。普通 P2/P3 风格优化与缺少 CI 均不阻塞源码公开。当前没有需要通过扩大架构来解决的已确认缺陷。
