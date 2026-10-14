# FairQ 全项目独立审计（2026-10-14）

FairQ 面向多租户任务领取：每个 Group 使用 FIFO，共享 Redis 轮转队列保证领取机会公平。本轮按多进程、真实 Redis、最多历史目标 10×80 执行槽的使用场景审查；没有把该目标规模当成已验证的生产能力。

本轮从干净的 `fd27ec8` 开始，重新检查整个仓库，不以最近 diff 为边界，也不把旧报告当成代码正确性的证明。发现并修复 **1 项 P1 稳定性缺陷**：慢速指标输出持有统计锁，会阻塞队列操作。没有确认新的 P0 缺陷；这不等于证明不存在缺陷。10 个 Lua 文件、Worker 状态机、公开 API、Redis Key、依赖和许可证均保持原样。

## 1. 范围、证据与方法

- 基线 84 个跟踪文件：逐文件读取所有 Go 源码、测试、10 个 Lua、示例和工具；核对 README、契约、设计、验收及历史文档。JSON 报告按结构读取全部样本，TXT 核对用例、失败与包结果；没有仅凭文件名判断覆盖。
- `git status --short --ignored` 无额外工作区产物；检查 `.gitignore`、`go.mod`、`go.sum`、MIT LICENSE、Git remote 和全部 25 条基线提交记录。对 `git rev-list --objects --all` 的 181 个文件版本做定向模式扫描，见 §9。没有读遍每个历史版本的全部业务逻辑。
- 使用 codebase-memory 尝试 full 索引；worker 因无法安全确认 daemon coordination 退出。对全部 84 个路径和源码目录请求覆盖，返回项目未索引。结构及调用关系改用直接源码与全仓检索核验，**不声称已完成图验证**。
- 先完成只读审查，再对具体问题增加失败回归、最小修复、定向 Race、一次全项目 Race、静态检查，最后分批本地提交。无 CI、无长稳、无十万任务性能重跑、无推送或生产操作。
- 原设计 SHA256 仍为 `7b9fdd0f4ac4017ab320c652472cab8b5d5f0f660fd5102d3db70cc03c2473ca`。此前审计版本可由 `git show fd27ec8:docs/release-audit.md` 读取；历史原始报告全部保留。本文件收敛为当前独立审计，避免维护两份当前结论。

## 2. 架构、模块与依赖

| 模块 | 实际职责与调用关系 | 评价 |
|---|---|---|
| 根包 `fairq` | Task/Options/TaskInfo、Handler、Error/IsCode、ErrTaskNotFound | 公共契约集中，不包含连接或后台服务 |
| `client` → `base` / `rdb` | 生成 ID、校验 Group/Payload/配置、编码、原子入队、查询 | 输入校验在写入前；未知结果仍保留可查询 ID |
| `server` → `rdb` / `base` / `clock` | 固定 worker 循环、Handler、Heartbeat、Recoverer、Forwarder、Cleaner | 本地生命周期与 Redis 所有权分离；没有独立预投递调度层 |
| `internal/rdb` → go-redis / Lua | 连接与脚本缓存、状态转换、回复解析、指标 | 共享原子边界集中；禁用未知结果的透明网络重试 |
| `inspector` → `rdb` | 单任务、Group、索引查询和全量只读一致性检查 | 不承担修复；不是事务快照，完整扫描占 O(任务+引用) 客户端内存 |
| `internal/testutil` / `tests/acceptance` | 专用 DB、进程屏障、Redis/Sentinel/故障节点 | 测试辅助与库隔离；本地文件锁不是生产分布式锁 |
| `examples/basic` / `tools` | 业务接入示例；模拟负载/性能验收程序 | 使用目的明确，普通导入库不启动工具或监听器 |

成功 Enqueue 的 `LPUSH` 与 Dequeue 的 `RPOP` 构成组内 FIFO。领取 Lua 同时完成 `LPOP ready`、取该组一条任务、写 Active/Lease/token、非空组 `RPUSH ready`；因此在固定且持续非空的 G 个组上，每 G 次成功领取各组一次。入队已有组不改变位置，新组和重新激活的组排尾。并发下以 Redis 执行顺序判断，不能按网络回复或 Handler 完成顺序判断。Retry 转回 Pending 后排在已有任务之后。

包层次没有明显需要合并的重复职责。`Clock` 有受控等待测试这一实际调用方；不因实现少就删除。Store 是具体类型，没有额外 repository/service 包装接口。Retry 退避在 Go 计算，Lua 重验允许区间，两个位置承担不同信任边界。

## 3. Redis 数据结构与工程收敛判断

| 疑似冗余部分 | 解决的问题 | 删除后失去什么/可靠性影响 | 更简单替代及本轮决定 |
|---|---|---|---|
| Ready + Members | List 保存顺序，Set 保证 Group 激活去重 | 只留 List 需要查找整条列表或另改激活契约 | 没有实测性能收益足以抵消改动风险，保留 |
| Active + Lease | 执行集合与到期时间；可交叉核对损坏 | 可设想只用 ZSet，但会改变 I4、故障判断及 Inspector | 属核心契约变更，没有现实收益证据，保留 |
| Hash.state + 主索引 | 查询状态并发现部分写入/错误引用 | 缺少损坏对账依据；仅索引不足以判断合法归属 | 保留，不追求最少 Key/字段 |
| Retry / Archived / Completed | 延迟重入、失败保留、可选成功保留 | 合并会混淆到期语义和安全删除条件 | 现有共享 maintenance 已复用流程，无需合并存储 |
| BadRefs | 先保存证据，再隔离可证明错误引用 | 删除将失去故障追溯；自动裁剪会丢证据 | 保留阈值告警和人工处置，不建 Incident 平台 |
| 四个后台循环 | 心跳、到期恢复、转发、终态清理各有节奏 | 强行合并会让慢清理延误续租；删循环丢功能 | 固定 goroutine 数，保留 |
| common.lua | 统一类型/数值/所有权/失败规则 | 复制会让关键校验分叉 | 包初始化拼接一次；没有请求级动态脚本生成，保留 |
| 两处 Group 激活短逻辑 | Enqueue、Forward 各自在原子转换中激活 | 提取 Go 会打破原子性，Lua 包装收益小 | 保留短重复，不加框架 |

多实例维护器确有重复扫描，但转换脚本重验使其最多一次成功；历史十进程测试已经记录较高扫描/转换比。本轮无新性能数据，不加入选主、分布式锁或共享游标 Key。扫描按 rank 推进并回绕，删除会让某些候选延后到下一轮；这不是严格的恢复延迟上界，坏记录仍须人工处理。

## 4. Lua 专项审查

全部脚本共 261 行；每个 Go 入口、KEYS/ARGV 排列和回复解析重新核对。本轮 **逐字节不变**。统一 KEYS 为 Ready、Members、Active、Lease、Retry、Archived、Completed、BadRefs；Task/Pending Key 由固定前缀及当前 ID/Group 派生。N 表示索引规模，B≤100；下表复杂度不包括消息/证据字节长度和释放 Hash 的成本。

| 文件（行数） | Go 入口 | 核对重点与结论 |
|---|---|---|
| common.lua（83） | 9 个脚本公共前缀 | number/task_duration 边界、索引互斥、owner、证据 pcall、共享失败转换；退避最多8次倍增，无无界循环 |
| enqueue.lua（12） | Store.Enqueue | 写前冲突/类型/索引/Group检查；Hash、LPUSH、SADD/RPUSH一起完成；固定命令次数 |
| dequeue.lua（48） | Store.Dequeue | 先 LINDEX 再校验；一次最多隔离一条；合法领取和全局轮转同脚本，O(log N)；不能拆为多个调用 |
| heartbeat.lua（14） | Store.Heartbeat | ≤100项；逐项 state/token/未过期 Lease，独立返回结果；O(B log N)，不是无条件 ZADD XX |
| done.lua（17） | Store.Done | 当前未过期 owner、Retention范围；删除或写 Completed；不允许旧执行结束新尝试 |
| retry.lua（7） | Store.Fail | owner和快照count必须匹配；error/timeout/panic计次，decode直接归档保留msg |
| recover.lua（27） | Store.Recover | 再验token/count/score、Active及到期；从当前 Redis TIME 起算退避；孤儿先留证据，未知现场保留 |
| forward.lua（13） | Store.Forward | 再验候选及到期，ZREM Retry与回原组LPUSH/激活原子完成；不恢复旧FIFO名次 |
| clean.lua（15） | Store.Clean | 终态、token、score、互斥索引及到期一起校验，再删Hash/目标索引；不凭孤立过期引用删任务 |
| scan.lua（25） | Store.Scan | rank页≤100，TIME筛选，HMGET候选；O(log N+B)，只返回提示，转换仍重验 |

每个维护器每tick最多5批；Cleaner两类终态共享500候选预算并轮换优先级；没有 Lua `KEYS`、全队列 `LREM` 或无界消费循环。Pending 中无法证明归属的损坏会保持现场并阻塞该队首，符合当前故障保全契约，需告警和受控修复，不擅自跳过。

Lua 隔离不等于出错回滚；预校验覆盖可预测错误，注入首写后错误仍会留下部分现场，测试验证 Hash 留存与 Inspector 检出。未知网络结果不自动重发变更脚本；NOSCRIPT 的缓存恢复是另一种情况。

Redis 官方建议所有实际访问的 Key 显式列入 KEYS；当前动态领取在脚本中派生 Task/Pending，与该建议存在差异。单实例/Sentinel 8.0.2 的现有真实演练通过，只能支撑这一范围，不能推出 Cluster 或其他执行平台兼容。本轮没有借此改动领取原子边界，扩展兼容性须单独设计和验证。[官方脚本说明](https://redis.io/docs/latest/develop/programmability/eval-intro/)

## 5. Go 质量与 Worker 生命周期

配置校验、UUID、JSON/base64、退避饱和、整数毫秒范围、错误包装及 Context 传播有明确边界。公开错误支持 `errors.As`、`IsCode`；缺失任务可 `errors.Is(err, ErrTaskNotFound)`，底层错误通过 Unwrap 暴露。Error 文本不直接打印底层内容有脱敏价值，不为风格改成拼接全部错误。

Start 在锁内确定生命周期，登记 WaitGroup 后创建固定 worker 和四个后台循环；poll/background Context 分开，Shutdown 停领取时继续续租。重复 Shutdown 等待同一 closed 通道。执行登记以 token 为键，旧尝试退出不会删除同ID新尝试。Dequeue/Heartbeat 的本地有效界从请求前单调时间加 duration 得到；迟到续租不能复活已失权的执行。

Handler 结果通道容量为1，调度端退出后迟到结果不会卡在发送；timer Stop、handler cancel、连接 Close、维护循环取消路径均核对。**Concurrency 不是无条件的业务 goroutine 上限**：运行期间Handler忽略取消时，调度端超时返回并继续领取，旧Handler仍可能积累。Go不能强杀该协程；这是已声明的合作取消边界，改变为永远占槽会改变吞吐/关闭语义，本轮不改。README补充明确说明。用户提供的阻塞Logger/Dialer、禁用网络超时等配置，也不能靠ShutdownTimeout获得绝对进程退出上界。

与历史 P01 单次299峰值不能混淆：历史缺栈，无法追溯逐个归因；工具GC计数修复有既有实验支持，本轮对应回归通过，但未跑长期复验。

## 6. 新问题清单与实际修复

| 优先级/位置 | 具体证据与触发条件 | 实际影响 | 最小方案、收益与决定 |
|---|---|---|---|
| **P1** `internal/rdb/metrics.go:86` metrics.write | m.mu保护统计Map，同时包住对外Writer的所有输出；当一次指标响应Write阻塞，observe/operation/RecordMaintenance等待同一锁。新增阻塞Writer回归在旧代码稳定失败 | 同一Client/Server的Redis统计hook无法返回；领取/Done/Heartbeat和维护循环可被指标读取者拖住，Shutdown也可能等待；HTTP抓取Context不会自动解开Mutex或打断Writer | 锁内写标准库strings.Builder，解锁后一次性向外输出；保留一致的进程统计快照和文本格式。不增加依赖/后台协程，修改值得做，已提交ea0ab38 |
| **P2 文档** README的Concurrency边界 | 原文仅说忽略取消会继续运行，未点明超时后槽位复用导致残留Handler可超过Concurrency | 使用者可能把配置误当作不合作Handler的硬资源上限 | 补充一段准确边界说明；不改生命周期或引入强制等待 |

指标回归通过通道精确控制Writer已经进入阻塞，再验证Redis命令、队列操作、维护器统计都能完成，并让第二次抓取看到新值。超时只用于失败截止；不依赖网络缓冲恰好填满。旧代码FAIL，新代码三轮Race PASS；真实Redis现有指标格式/敏感标签测试和全项目Race也通过。

修复代价是每次抓取暂存一份指标文本；标签来自有限内部操作和命令名称，没有TaskID/Group/Payload高基数。网络慢只占用当前HTTP请求，不再持有队列统计锁。未声称吞吐提升或做新性能基准。

## 7. 测试体系评价

核心闭环不仅检查返回码，还核对 Hash、主索引、DUMP不变、ID守恒；包含默认25/26次失败、大整数和duration边界、旧token/过期保护、终态清理、Group动态入退与公平窗口。10独立进程×80槽测试验证无重复领取，十进程维护测试验证单次转换；600条坏队首验证扫描推进。

48例故障矩阵覆盖8个变更脚本×6种故障，使用真实RESP代理和Redis；没有用mock替代原子状态机。AOF、RDB、Sentinel、水位核对、TLS/ACL、OOM、kill Worker、存活Worker跨Redis重启分别验证不同故障边界。Sentinel切换前确认副本水位，所以该结果不能证明异步复制永不丢写。

fixture按DB分离并用文件锁串行化同DB顶层测试；子进程只接入，不FlushDB。启动的节点、证书、日志使用测试临时目录，Cleanup负责退出。基础设施的随机端口先释放再绑定存在极小竞争窗口；当前没有复现失败，不重写测试框架。部分测试使用短轮询和真实时间，尤其15s重试验证不能删掉或换成无关Sleep。子进程辅助Test在非子模式直接返回，不代表独立执行了验收；必须看父用例结果。

剩余测试边界：未跑Go1.24最低版本、其它Redis版本/部署网络；测试fixture使用Unix Flock，不能声称Windows全套可用。负载工具正常短测退出0只代表正确性子项，`Passed`仍需正式规模/时长等条件；报告应连同启动参数看，JSON未完整保存所有配置，不能单凭它推断Handler成本。无新的性能结果，不改变已有验收判定或补无关抽象。

## 8. API 与使用体验

Client负责入队/查询，Server接收单个Handler并管理生命周期；不再增加路由、调度插件或自动Repair API。配置虽涵盖多种间隔，但大部分有默认值，只要求显式Concurrency/ShutdownTimeout。MaxRetry=0选择默认25，暂不支持显式零次重试，这是已有约定；不擅自改变零值含义。Timeout=0无Handler时限，Retention=0成功删除，TaskID去重只持续到Hash删除。

README的Quick Start与`examples/basic`一致；本轮真实Redis例程测试验证入队、执行、Completed和关闭，`go test -race ./...`同时编译全部工具。公开模块路径仍是`fairq`；已有本地replace试用说明，GitHub仓库确定前不猜用户名、不改imports、不发布版本。无未实现的公开功能占位API。

Inspector.Index 对Set/ZSet使用SCAN系列，count是提示而非返回条数硬上限；只有List/rank页有明确上限。全量Check和指标最老Pending查询会分批扫描数据库，频繁、多进程重复抓取会增加负载。现有文档提示专用Redis和合理抓取间隔；没有实际瓶颈数据，不加缓存、额外Redis计数Key或后端服务。

## 9. 开源仓库、安全与文档

MIT LICENSE已由维护者确定，版权署名2026 icew，保持不变。两项直接依赖为google/uuid、go-redis；两项间接依赖为xxhash/v2、atomic。本机模块许可证分别为BSD-3-Clause、BSD-2-Clause、MIT、MIT；未vendor、未复制外部源码。`go mod verify`与`tidy -diff`通过；go.sum中依赖测试所需记录不是无用生产依赖，不手工删除。

基线无跟踪二进制、pprof、trace、coverage或临时构建产物；唯一隐藏跟踪文件为.gitignore。原始JSON/TXT是测试证据，保留。历史实施计划/进度已在history目录并标注历史上下文，不能把里面的指令当成本轮要求。原设计保持原文，与当前API差异由README/contracts说明；本轮只合并更新当前审计，不新增重复设计文档。

对181个历史文件版本检查私钥头、GitHub令牌、AWS访问键、开发者绝对路径及TODO/FIXME/HACK/shortcut模式，均无命中；源码中的固定ACL口令仅用于一次性本地fixture，TLS私钥运行时生成。这是**定向模式检查**，不包含所有可能的低熵口令、不可达对象、外部日志或独立权属核查，也不等同于供应链漏洞数据库扫描。没有发现需报告撤销/轮换的真实凭据。

## 10. 成熟实现对照

本轮重新读取本地固定版本源码及相关测试，不仅引用旧报告。Asynq使用v0.25.1；GQM使用此前已与固定SHA归档比对的b090d225副本，本轮网页目录读取失败，不据此声称核验最新分支。没有移植源码或新增第三方组件。

| 证据等级/来源 | 本轮实际核对 | 对FairQ的判断 |
|---|---|---|
| 已核对源码：[Asynq processor](https://github.com/hibiken/asynq/blob/v0.25.1/processor.go)、heartbeat.go、recoverer.go、forwarder.go | semaphore限制执行、缓冲结果、取消/关闭、在途登记、Lease续期、Recover重试 | 合作取消模式可比较；不能把它的Shutdown直接重入队套用到FairQ的保留Lease契约 |
| 已核对源码：[Asynq rdb](https://github.com/hibiken/asynq/blob/v0.25.1/internal/rdb/rdb.go)与rdb_test.go、processor_test.go相关用例 | 内嵌Lua、Queue领取、Go时间、ZAddXX续租、Forward单批100但Go循环至空；expired lease/ExtendLease测试 | FairQ的Redis TIME、token校验、全局Group RR、有界维护是实际不同需求，不能为了短代码替换 |
| 已核对源码：[GQM固定版本](https://github.com/benedict-erwin/gqm/tree/b090d225caa3a3d90a4e4bd5d17bb440f9067b7e)的worker/pool/reaper/scripts及dequeue/retry/reap Lua、conservation_test | 固定worker、本进程rrCounter、配置队列、独立Handler、pool/job心跳、deadline重验及逐ID守恒测试 | 本地轮转不能替代跨进程Group轮转；其先出队再查状态与FairQ先保全证据要求不同；守恒测试方法有参考价值 |
| 仅核对官方文档：[Redis Lua](https://redis.io/docs/latest/develop/programmability/eval-intro/) | 执行隔离、脚本缓存、KEYS约束 | 解释原子边界与兼容性范围，不当作FairQ故障实测证据 |
| 尚未验证 | 外部项目当前最新版的性能、整个外部测试套件及跨版本兼容性 | 不做谁更快/更可靠的结论；未扩展到无实际新增价值的更多队列 |

## 11. 本轮验证结果

完整输出与复现命令见 [independent-regression-20261014.txt](reports/independent-regression-20261014.txt)。Go1.27.1、macOS arm64；专用Redis8.0.2（TLS构建），loopback16404，768MiB/noeviction，常规fixture无持久化，基础设施另建临时实例。

| 验证 | 结果 |
|---|---|
| 阻塞Writer回归，旧代码 | 预期FAIL，1秒截止，证明队列统计受阻；包2.310s |
| 修复后定向Race，3轮 | PASS，包1.535s |
| 首次沙箱全量尝试 | 环境失败：loopback监听和构建缓存读取被禁止；不计产品测试通过 |
| 获准隔离环境 `go test -race ./... -count=1 -v -timeout 90s` | PASS；client1.702s、示例2.163s、inspector2.909s、base2.962s、rdb9.761s、server26.047s、acceptance28.816s、benchmark4.739s、loadtest5.626s；包并行，不能相加 |
| AOF/RDB/Sentinel/存活Worker恢复 | PASS；95个Hash保留、95个快照Hash恢复、100个确认任务切换后保留；本机RTO约112.866ms/109.804ms/1.712s/16.800s（最后包含真实退避） |
| `go vet ./...` / `go mod verify` / `go mod tidy -diff` | PASS，无依赖变化 |
| gofmt / `git diff --check` / Markdown本地目标 | PASS |
| 原始设计/历史JSON-TXT/核心Lua与fd27ec8比对 | 不变（本轮仅新增一份回归记录） |
| 60分钟P01、十万任务性能、生产目标环境/灰度 | NOT_RUN，按范围限制执行；历史P01仍FAIL，完整复验NOT_RUN |

测试后核对PID并关闭本轮专用Redis，未操作既有实例。26份基线Lua/设计/许可/模块/历史原始报告逐字节一致；53个本地Markdown目标检查通过。本轮没有另外重复普通全量测试或已通过的完整Race；后续文档编辑只做对应静态验证。

## 12. 文件与提交

- `ea0ab38 fix(metrics): release accounting lock before writing scrape response`：只改metrics.write，新增`internal/rdb/metrics_test.go`和本轮回归证据。
- 随后的文档提交：更新本报告，README明确不合作Handler的并发边界；精确哈希由`git log`查看，避免在提交内自引用尚未产生的哈希。
- 本轮无文件删除/搬迁，无依赖、CI、模块路径、LICENSE、Lua、Worker或数据格式变更，无Git历史重写。
- 先前`c2c756e`数值回复修复、`48eee86`内部简化、`813278e`文档/示例、`fd27ec8`MIT许可属于既有历史，不算本轮新发现；相关代码和证据已重新审查。

## 13. 未修改事项与发布判断

| 事项 | 本轮决定与原因 |
|---|---|
| 核心Lua、Key/索引、Worker并发模型 | 没有新的真实缺陷或足够收益证据，保持原样；不为风格统一做重构 |
| 指标扫描成本、维护器重复扫描 | 记录规模边界，无当前性能证据，不加入缓存、额外Key或选主 |
| 不合作Handler、外部阻塞回调 | 明确使用责任；Go无法强杀，改变调度契约需单独评估 |
| 完整P01及生产环境验收 | 历史失败保留，用户不允许本轮长测；不能用短回归关闭 |
| 公共模块路径与远程发布 | 按用户要求等待GitHub仓库确定；本地replace可试用，未执行发布 |
| Go最低版本、其它平台/Redis、漏洞库/全面权属审计 | 本轮未验证，不从本机结果外推 |

| 发布层级 | 明确结论 |
|---|---|
| 公开源码 | **适合**以MIT公开供评审、贡献和实验，并保留未生产验证说明；本轮未推送 |
| 实验性版本 | **源码可本地试用；公开可安装的预发布版本尚待模块路径确定及外部安装验证**。无需伪造生产成熟度即可做实验性发布准备 |
| 稳定版本 | **暂不适合**。公开模块发布验证、完整稳定性门禁及目标环境验收未完成 |
| 生产使用证据 | **不足**。本机并发/故障回归有证据；正式长稳复验、生产网络/硬件/配置、真实业务幂等与灰度尚缺，不能宣称生产就绪 |
