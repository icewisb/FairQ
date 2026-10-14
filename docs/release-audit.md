# 发布前审计（2026-10-14）

审计起点 `d77d399`，工作区干净。先只读检查全部 78 个 Git 跟踪文件（含所有 Go、Lua、测试、配置、文档和报告），再按收益实施最小修改。没有修改冻结的公平轮转、FIFO、At-least-once、Key 或主状态契约。按本次要求不运行 60 分钟 P01，不推送、不操作生产；后续明确要求不写 CI。

## 证据与范围

codebase-memory `index_repository` 因 daemon coordination 校验失败，`check_index_coverage` 返回项目未索引；没有可用图覆盖。改为直接读取源码、检索全部调用点、核对实际测试。此报告不声称完成图验证。v1.4 仓库文件与提供的原文 SHA256 一致：`7b9fdd0f4ac4017ab320c652472cab8b5d5f0f660fd5102d3db70cc03c2473ca`。

实际阅读的外部源码：

- [Asynq v0.25.1 rdb.go](https://github.com/hibiken/asynq/blob/v0.25.1/internal/rdb/rdb.go)：Enqueue、Dequeue、Done、Retry、Archive、Forward、ListLeaseExpired、ExtendLease；以及同版本 processor.go、heartbeat.go、recoverer.go、rdb_test.go 的相关实现与测试 fixture、LICENSE。
- [GQM b090d225](https://github.com/benedict-erwin/gqm/tree/b090d225caa3a3d90a4e4bd5d17bb440f9067b7e)：scripts.go、lua/dequeue.lua、retry.lua、reap.lua，worker.go 的轮转、重试及 heartbeat，reaper.go 和相关测试、LICENSE。下载时 main 对应此 SHA；未运行外部项目测试。
- [Redis Lua 文档](https://redis.io/docs/latest/develop/programmability/eval-intro/)：脚本执行隔离、缓存及协议转换。Lua 隔离不等于运行时出错回滚；FairQ 的部分写入故障矩阵专门检查这一点。

## 问题与处理顺序

| 优先级 | 发现 | 决定 |
|---|---|---|
| P1 | 合法的大 MaxRetry / retry_count 经 Lua tostring 变为科学计数法，任务已变为 Active 后 Go ParseInt 才失败；恢复/失败响应也有同类问题 | 已修复：数字直接返回 RESP integer，保持精确整数范围；新增真实 Redis 边界闭环测试 |
| P2 | no_indexes 与 state_indexes 的 pending 分支重复；Scan 读取/返回未使用的 State；metrics DialHook 仅透传 | 后续同批最小简化，不改变原子边界或校验 |
| P2 | README 无完整可运行程序、安装状态和 License 说明；历史交接指令混入当前文档 | 重写使用入口，归档历史计划/进度，保留原设计及失败证据 |
| 发布阻塞 | 无 LICENSE，无 Git remote，go.mod 为本地 module fairq | 不能猜测版权人、许可证或发布路径；正式版本前由维护者确定 |
| 生产阻塞 | 历史 P01 失败，完整复验和目标环境/业务灰度未完成 | 保持 FAIL / NOT_RUN；本地回归不能关闭该门禁 |

未发现需要新增锁、选主、任务状态或 Key 的依据。发现的数值问题有可验证的局部修复，无需重写队列。

## Lua 逐文件审计

基线为 10 个文件、267 行（`wc -l internal/rdb/lua/*.lua`）：9 个可执行脚本，加 1 个共享前缀。表中 N 为索引大小，B≤100，复杂度忽略消息字节数及 Redis 编码细节；Hash/List/Set 常用操作通常为 O(1)，ZADD/ZREM 为 O(log N)。大消息/证据编码与释放 Hash 的成本随数据大小增加。

所有脚本的 KEYS 固定为 Ready、Members、Active、Lease、Retry、Archived、Completed、BadRefs；实际只读取/修改所需 Key，Task/Pending 从 ID/Group 派生。下表以缩写列明逻辑范围；主索引校验读取 Active/Lease/Retry/Archived/Completed，不能移至 Go，否则产生 TOCTOU。

| 文件 | 基线行数 | 入口与频率 | 读写 Key、命令与复杂度 | 原子边界及结论 |
|---|---:|---|---|---|
| enqueue.lua | 12 | Store.Enqueue，每次入队 | Task EXISTS/HSET；主索引 SISMEMBER/ZSCORE；Pending LLEN/LPUSH；Ready/Members TYPE/SADD/RPUSH，固定次数 | 冲突检查、创建 Hash、Pending 写入与激活必须一起；复用 pending 索引检查即可 |
| dequeue.lua | 48 | Store.Dequeue，每槽领取或空轮询 | Ready/Pending LINDEX/LPOP/RPOP/LLEN/RPUSH；Members S*；Task HGET/HSET/HDEL；Active SADD、Lease ZADD；坏引用写 BadRefs，O(log N) | 选择 Group、出队、所有权写入、轮转不可拆；一次只隔离一条，保留当前组位置；修复数字返回 |
| heartbeat.lua | 14 | Store.Heartbeat，每 HeartbeatInterval，最多100项/批 | 主索引 TYPE；逐 Task HGET、Active SISMEMBER、各 ZSCORE、Lease ZADD，O(B log N) | 每项重验 token/state/未过期 Lease；批内部分结果必须保留；数字直接返回 |
| done.lua | 17 | Store.Done，每次成功 Handler | Task HGET/DEL 或 HSET；主索引检查；Active SREM、Lease ZREM、可选 Completed ZADD，O(log N)+Hash释放 | 所有权、Retention 验证和终态变更一起，保留 |
| retry.lua | 7 | Store.Fail，每次 Handler error/timeout/panic/decode | owner 与 fail_transition 读写 Task/主索引，O(log N) | Worker 不允许过期提交；使用共享失败转换，保留独立入口 |
| recover.lua | 27 | Store.Recover，RecoverInterval 内每到期候选 | Lease ZSCORE/ZREM、Active SISMEMBER、Task HGET；合法任务进入 Retry/Archived，孤儿先写 BadRefs，O(log N) | 原子重验候选 token/count/score 与 Redis TIME；保留无法判定的现场，不直接重入 Pending |
| forward.lua | 13 | Store.Forward，ForwarderInterval 内每到期候选 | Task/主索引校验；Retry ZREM；Task HSET；Pending LPUSH；Members SADD、必要时 Ready RPUSH，O(log N) | 到期重验、回原组与激活一起；两处短 Group 激活逻辑保留，不造框架 |
| clean.lua | 15 | Store.Clean，每清理周期的终态候选 | Task HGET/DEL、主索引检查、Archived/Completed ZSCORE/ZREM，O(log N)+Hash释放 | token/state/score/到期重验后删除；90天保留、数量阈值只告警不变 |
| scan.lua | 27 | Store.Scan，每维护器批次，最多5批/tick | Lease/Retry/Archived/Completed ZCARD/ZRANGE WITHSCORES；候选 Task TYPE/HGET，O(log N+B) | 一次 TIME 过滤有界页并返回快照，转换脚本仍需重验；State 未被 Go 使用，可删除并合并字段读取 |
| common.lua | 87 | 包初始化加载，作为上述9个脚本的前缀 | TYPE、TIME、HGET、SISMEMBER/ZSCORE；证据 cjson+pcall RPUSH；失败 SREM/ZREM/HSET/ZADD | 所有权和失败规则值得共享；指数退避区间检查最多8次倍增，无无界循环；去除重复 no_indexes |

### 保留及不做的简化

`script(name)` 在包变量初始化时读取 embed.FS、拼接 common/body 并生成固定 SHA；请求只调用 Script.Run，不动态生成用户脚本。保留这一简单实现，不引入注册表、DSL 或代码生成。共享前缀会让 Lua 报错行号相对合成文本，需要结合 common.lua 行数定位，测试故障注入已采用相同拼接。

Lua 仍负责状态、租约、索引和原子写入；Go 负责 JSON 编解码、UUID、配置、RNG/退避、Worker、日志与指标。Lua 中有限的退避区间复核用于拒绝无效转换，不能仅为减少行数删除。Heartbeat 的逐项校验和批次预算保留；不增加跨请求缓存。Scan 的 rank 游标可能因并发删除跳过条目，但会回绕，且转换重验避免重复；不能换成永远扫坏队首的实现。

Ready+Members 是轮转及去重，Active+Lease 是执行集合及时间索引；Retry/Archived 各有生命周期职责，Completed 是已有可选保留，BadRefs 是异常证据。没有发现可安全删除的存储索引；不新增 stats 或 Group GC。保留 started_at/failed_at/finished_at 等查询/故障证据字段，不以低频为由删除。

### 外部实现对比

Asynq 在 Go 中内嵌 redis.NewScript，按 Queue 使用 RPOPLPUSH，时间由 Go 传入，ExtendLease 使用 ZADD XX；Forward Lua 每批100条，但 Go forwardAll 循环到空，过期 Lease 查询没有同样的批次上限。归档有按数量和时间裁剪。FairQ 的全局动态 Group RR、Redis TIME、attempt token、写前索引检查、有界维护器和不裁剪异常证据均是不同契约，不能照搬 Asynq 以换短代码。两者都把编码、Handler 和重试策略主要放在 Go，并以 Lua 完成相关 Redis 转换。

GQM 用 embed 和脚本注册表；RR 起点来自单进程 pool 的原子计数器，并遍历配置队列，不能替代跨进程共享的 Group 轮转。其领取先 RPOP 再检查任务状态，reap.lua 重验处理截止时间；Heartbeat 写进程/任务时间字段。FairQ 必须保留先留证据后移除坏引用以及逐 attempt 租约保护。仅借鉴职责拆分和守恒测试，不移植这两家的实现，也未新增外部代码。

## Go、测试和仓库核对

公开包 client/server/inspector 与根 Task 类型清楚；rdb/base/clock/testutil 留在 internal。Clock 接口已有确定性等待测试，Store 没有无意义接口层，不做目录搬家。Server 的 poll 与后台 context 分离，使 Shutdown 等待时仍能续租；本地单调时间窗口防迟到响应复活；timer、ticker、连接及 goroutine 的退出路径已检查。忽略 Context 的业务 Handler 无法被 Go 强杀，可能残留并产生重复副作用，需在使用说明明确。

测试不仅验证返回值，也验证 Hash/主索引、DUMP 不变、多进程守恒、真实网络故障和 kill/restart。保留真实时间语义测试中的有限 Sleep；不为缩短运行时间去掉真实15s重试等待、降低800槽或10进程竞争。测试使用显式隔离 Redis、按DB文件锁；并发测试与实际底层连接故障有不同验证目的，不删除重复外观的用例。

依赖仅 UUID、go-redis 及其间接依赖；未发现需要新增或移除的依赖。未发现 Git 跟踪的二进制、coverage、pprof 或 trace；原始 JSON/TXT 是验收证据，保留。文本模式扫描未发现实际凭据或开发者绝对路径；ACL 测试口令仅一次性 fixture，私钥现场生成。历史文档中有临时路径，将清理不可复用的指引并保留事实。这不是完整 Git 历史的秘密扫描或法律权属鉴定。

## 验证与提交记录

第一批：真实 Redis 8.0.2 复现 `tostring(9007199254740990)` 得到 `9.007199254741e+15`，直接整数响应保持 `9007199254740990`。新增 `TestLargeRetryIntegersRoundTrip` 在旧代码两个子用例均 FAIL；修复后 `FAIRQ_TEST_REDIS=127.0.0.1:16403 go test -race ./internal/rdb -count=1 -v` PASS，包耗时5.836s，包含48例故障矩阵与10进程×80槽领取。测试前缀不变，无数据迁移。

后续简化、文档整理、最终回归及发布准备度将在本记录中更新。Git 提交采用独立可验收批次，以实际 `git log` 为准。
