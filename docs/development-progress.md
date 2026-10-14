# FairQ 开发进度

更新时间：2026-10-14（Asia/Shanghai）。开始时必须与git历史、代码和测试核对。

- 当前阶段：7 生产验收未全部通过；用户已要求暂时搁置goroutine峰值问题，继续其他功能，不再以该问题阻止独立工作。
- 已完成：完整复核v1.4；七项契约决议；Lua协议、数据不变量、状态/错误表、全量验收映射；原文归档。
- 已通过：阶段0文档链接/原文SHA256检查；阶段1 `FAIRQ_TEST_REDIS=127.0.0.1:16379 go test ./...` 与 `go test -race ./...`，真实Redis 8.0.2，100并发入队/去重、查询、校验无写入、错误返回ID、Cluster配置拒绝。
- 最近成功Commit：`ed5ad18 fix(inspector): detect invalid numeric state and index scores`；本次提交以git历史为准。
- 未完成：P01 goroutine峰值定位及生产目标环境/业务灰度验收；本机Sentinel真实切换已通过。
- 已知边界：Hash存续期去重；业务At-least-once幂等；Sentinel非零RPO；运行时Lua错误不回滚。
- 阻塞：无已确认P0架构阻塞。codebase-memory索引服务启动失败，已直接检查源文件；后续图覆盖不可用时使用源文件核验。隔离Docker容器 fairq-dev-01a13941，127.0.0.1:16379；测试显式选用DB1..15，禁止指向生产。
- 下一步最小任务：按v1.4与实际代码/测试核对剩余必需功能或交付缺口，优先推进不依赖P01的工作；暂不继续诊断goroutine峰值，不重跑一小时长测。完整生产门禁保持未通过。

## 阶段记录

提交粒度补充（2026-10-14）：按完整、可独立验收的功能/修复单元提交，实现、必要测试和验收记录一起提交；不积攒大 commit，也不把零碎修改分别提交。

### 独立缺口修复：任务时长溢出（2026-10-14）

继续核对 v1.4 的必要字段写前校验及 Timeout/Retention 契约，真实 Redis 复现 timeout_ms=9223372036855 被成功领取并转换为负 time.Duration；更大的损坏值还可能在领取已写入后才解析失败。retention_ms 也存在 Lua 接受、GetTask 拒绝的范围不一致。修复公共 Lua 任务时长校验，并用于 Enqueue/Dequeue/Done；超 Go 时长上限返回 INTERNAL 且 DUMP 完全不变。Inspector 同步按 Go 时长上限报告 FIELD；其它数值仍按 Lua 精确整数范围检查。最大合法整数毫秒的入队→领取→完成→查询回归通过。正常公共 API 可表达的合法值范围未缩小，未改变任务状态机或增加 Key。

验证使用本次新建的一次性 Redis 8.0.2，127.0.0.1:16402，DB1..4；client、inspector、internal/rdb、server 全包 race 回归及 go vet ./... 通过，包含多进程领取/维护器、48例故障注入与 Worker 生命周期；见 [验证记录](reports/task-duration-20261014.txt)。codebase-memory 仍无法索引，覆盖不可用，采用源码和调用方检索核验。未重跑独立基础设施、容量或长稳演练，P01与生产灰度状态不变。

### 独立缺口修复：Inspector 数值对账（2026-10-14）

对照 v1.4 §6.3、contracts §2 与源码，发现 X06 的数值损坏检查有遗漏：Lease/Retry/Archived/Completed 的非法 score、Hash 超出 Lua 精确整数范围的字段，以及负 pending_since，可能得到 Complete=true 且 Issues 为空的报告。真实 Redis 新增用例先复现失败，再修复只读 Inspector：非法时间索引返回 SCORE，非法 Hash 数值返回 FIELD；合法边界 0 与 2^53−1 保持可检查。DUMP 前后比较确认没有改写现场。没有修改队列状态转换、Worker 或验收阈值。

本次重新启动隔离 Redis 8.0.2 于 127.0.0.1:16402，仅测试 DB2/DB4；没有清空历史实例。Inspector 全包及依赖它的48例脚本故障矩阵 race 定向回归通过，相关包 go vet 通过，见 [验证记录](reports/inspector-numeric-20261014.txt)。codebase-memory 索引仍失败，已对本次依赖路径请求覆盖检查并直接读取源文件、检索调用方核验。

本次核对未发现需要另建组件的依据；后续按新证据推进独立缺口或交付工作。P01 峰值仍暂缓且未通过，目标生产环境与业务灰度仍未执行。本次不重跑未受影响的完整基础设施/性能验收，不运行长测，不推送。

最新用户指示（2026-10-14，交接优先读取）：用户明确说“暂时忽略这个问题，继续后续功能”，随后要求总结以便新会话接手。这里的“忽略”表示暂缓goroutine峰值诊断，不表示P01已通过或允许删除失败证据。阶段0–6的既定功能已经实现，阶段7多数本地验收已通过；新会话先核对真正的剩余需求，不重新设计架构、不虚构后续功能、不重新执行已通过且未受变更影响的完整测试。保持原授权：独立单元验收后本地提交，不自动推送，不接触生产；每轮仅做必要的短时验证。下文“自主推进阻塞核对”是此次用户指示之前的历史记录。

| 阶段 | 状态 | 证据 |
|---|---|---|
| 0 | 已提交 fe901d5 | contracts.md、acceptance.md、implementation-plan.md |
| 1 | 已提交 00511a2 | 上述真实Redis及race结果；A03的Sentinel演练延至G02 |
| 2 | 已提交 f34e85d | 3×100和10000/1000/1000的FIFO/RR；动态组；10进程×80槽领取1000任务；坏引用/证据失败/阈值；go test -race ./...通过 |
| 3 | 已提交 200929e | Redis TIME范围、批量逐项续租、Done删除、过期拒绝、旧token/缺索引保护；go test -race ./...通过；L03的Worker取消在阶段5 |
| 4 | 已提交 254893e | 25/26计数、退避RNG/溢出、旧候选、恢复时刻、10进程恢复/转发、坏队首推进；原生Redis上go test -race ./... -count=2通过 |
| 5 | 已提交 fd0ba7e | 并发上限、error/timeout/panic/decode、失权/断连取消、关闭续租与超时保留、受控Clock验证退避停领/重置、迟到续租不可复活；kill -9后真实15s+抖动退避恢复；go test -race ./...通过（server约24s） |
| 6 | 已提交 47d4f94 | Inspector只读DUMP对比及损坏检测、Completed公共API闭环、旧候选保护、10000归档阈值、终态清理轮流优先、Prometheus无敏感标签；真实Redis go test -race ./...及go vet ./...通过；README/Runbook已交付 |
| 7 | 未通过 | 8个脚本×6类故障（48例）通过；AOF/RDB/Sentinel、ACL/TLS、OOM/Cluster、全量race通过；perf报告已通过；首轮长稳失败记录已保留，不计整体完成 |

每次提交前记录本次验证；本次提交的哈希用git历史核对，下次更新填写，避免自引用提交哈希。禁止声称进程退出后会自动继续；续跑从本文件及实际git状态恢复。

## 环境问题记录

宿主机访问OrbStack Redis在800槽测试中发生TCP超时；相同测试在容器内和本机原生Redis各重复5次通过。当前使用127.0.0.1:16380原生Redis，详见test-environment.md。不将失败网络路径标为通过。测试fixture增加本地数据库锁防止重叠go test交叉清库。生产代码未为该故障放宽超时或加入重试。

## 阶段7执行记录

- 2026-10-14 本机真实Redis：48例故障涵盖执行前断连、执行后丢响应、超时、Lua首写前/首写后错误、主索引类型错误。部分写入保留Hash且Inspector发现；类型错误DUMP前后完全相同。
- 全量 `FAIRQ_TEST_REDIS=127.0.0.1:16380 FAIRQ_REDIS_SERVER=/private/tmp/fairq-native-redis/redis-8.0.2/src/redis-server go test -race ./... -count=1` 通过；新增工具race与go vet通过。基础设施实测原始记录见 reports/infrastructure-20261014.txt。
- reports/perf-20261014.json：Dequeue Lua p99上界0.066ms、API p99 0.160292ms；10万Lease恢复19.154s、p99 0.360875ms、100000扫描/转换、单tick≤500；159347712 bytes峰值，预算805306368 bytes。Inspector完整检查10万任务，无异常。
- 首轮长稳由当前会话启动，地址16400/DB5，日志 /private/tmp/fairq-soak.log；现已结束，原始报告为 docs/reports/soak-20261014-failed.json。后续遵循用户最新要求，不重跑一小时测试。

- 多进程测试增加仅测试用文件起跑屏障：10进程/800槽全就绪后同时领取；维护器同样同步启动。race目标回归通过，见 reports/multiprocess-20261014.txt。补充默认Done删除后的TaskID复用与旧token不能结束新任务的测试。

- O01/W03补充：真实Redis验证89天Archived保留、91天到期删除；Run取消仍等待在途完成，3个并发Shutdown均正常退出。目标race回归通过。完整回归原始输出见 reports/regression-20261014.txt；覆盖率按Go包独立计数，外部集成和独立进程工具执行不完整计入该百分比，不以该数字替代验收矩阵。

- 存活Worker跨Redis崩溃重启真实演练通过：本地租约到期取消旧Handler；重连后Recover→Retry→真实15s+抖动→Forward→新attempt完成，旧Handler迟到返回不能改新状态。实测完整恢复16.180s，race通过，见 reports/worker-restart-20261014.txt。

- 历史中间检查：跨过30分钟时，1000 Group/10进程×80槽持续工作，目标积压保持约10万，Redis used_memory_peak=150837824 bytes；最终结果见下文，不能只依据中间检查判定通过。生产代码自47d4f94后未改变。

- 最终全量race（含存活Worker重启）、go vet、go mod verify、gofmt检查通过；原文与仓库基线SHA256一致。ACL越界测试改用已授权HSET，确保验证Key边界而非命令禁用，目标race验收通过。

## 长稳结果与用户最新约束

首轮60分钟长稳已结束，排空126.130s，2973607入队/2973607完成、重复0、最终Inspector无异常，内存18.73%预算。**验收失败**：一个Worker在2310s样本中goroutine为299，前后均167，尚待定位。原始报告保留为 reports/soak-20261014-failed.json，不改写为通过。

用户已明确要求后续不再执行1小时长程测试；不再重跑60分钟，改用短时定向诊断。不能把短测冒充原P01完整长稳通过，也不因此要求用户逐阶段确认。

压测工具默认持续负载时间已改为30秒，日常小规模命令见 test-environment.md；预置与排空另计时间。未改变P01规模、时长和goroutine断言，也未修改生产Worker代码。

本次验证：`go test -race ./cmd/fairq-load -count=1` 通过；省略duration参数的真实Redis短测持续30.033s，10 Group/2进程×4槽、积压100，3348入队/3348执行，重复0、错误0、排空1.001s、Inspector无异常（精确时间以 reports/smoke-20261014.json 为准）。`Passed=false` 明确表示没有通过完整P01；不覆盖goroutine峰值原因。该Redis沿用首轮实例，used_memory_peak为实例历史峰值，不能归因于本次小负载。

## 短时峰值诊断（2026-10-14）

在294b731的负载工具临时副本中增加goroutine采样：启动2秒后超过199即写pprof堆栈，保持10独立进程×80槽/1000 Group，积压改为1000。三次真实Redis运行：默认连接空闲期+Handler10ms持续15秒；连接空闲期50ms+Handler10ms持续8秒；连接空闲期50ms+Handler1s持续8秒（采样由5ms加密到100µs）。原始结果见 reports/goroutine-diagnostic-20261014.json。

三次分别28800/16200/7400任务全部完成，重复0、错误0、Inspector无异常，均未产生阈值触发的堆栈文件。采样可能遗漏短峰值，未复现不等于修复或证明没有问题。临时诊断源码/可执行文件已移除，生产代码未改，原P01失败保持。codebase-memory再次索引失败，相关路径覆盖不可用，直接读取源文件核验。

当前诊断限制：没有首轮峰值堆栈，短场景尚不能稳定复现；无法有依据地修改生产实现。下一步需可复现该峰值的短场景或峰值堆栈，不继续盲目重复短测，也不启动一小时复验。目标环境验收仍需要外部环境授权。

## 自主推进阻塞核对

连续三轮已保留首轮失败证据、调整默认短测、完成三次定向复现尝试，并再次核对原始报告及全量race输出。当前没有运行中的长测，也没有待提交的业务实现。原P01要求完整长稳通过，首轮结果明确失败；用户禁止再跑一小时测试，不能自主改验收标准来关闭它。峰值根因还缺少堆栈或可复现短场景，目标环境/真实业务灰度也尚无执行授权。整体目标不标记完成，停止无证据的重复测试；恢复依据应是新增诊断证据、用户明确调整验收要求，或提供获授权的目标验收环境。不会自行重启长测或触碰生产。
