# 开发测试指南

所有测试要求 `FAIRQ_TEST_REDIS` 指向隔离真实 Redis；测试会清空显式选定的 DB1..15，禁止使用生产或共享业务数据库。未设置地址直接失败，不跳过。

准备 Go 1.24+ 和隔离的真实 Redis。当前验证版本为 Redis 8.0.2；完整基础设施测试需要支持 TLS 的 `redis-server`。将下列地址和可执行文件路径替换为自己的测试环境，测试不依赖开发者机器上的临时目录。

常用命令：

```sh
FAIRQ_TEST_REDIS=127.0.0.1:16380 FAIRQ_REDIS_SERVER=/path/to/redis-server go test ./...
FAIRQ_TEST_REDIS=127.0.0.1:16380 FAIRQ_REDIS_SERVER=/path/to/redis-server go test -race ./...
```

包使用不同DB：client=1、rdb=2、server=3、inspector=4。同一数据库的顶层fixture使用本地文件锁，防止两次go test交叉FlushDB；子进程只接入，不清库。该锁仅属于测试环境，不是FairQ生产分布式锁。

## 多进程测试网络故障对照（2026-10-14）

以下为历史实测记录：macOS arm64，Go 1.27.1，Apple M4 Pro、48GiB RAM，Redis 8.0.2（TLS支持），本机 `127.0.0.1:16380`，AOF开启、noeviction、maxmemory=768MiB。

原800槽测试 `TestMultiProcessClaims` 在 macOS→OrbStack Redis 容器的端口映射及容器IP直连路径上出现间歇TCP `i/o timeout`，不能记为通过。Redis无慢脚本记录、无拒绝连接，Lua平均耗时约22µs。限制同时建连未解决，已撤回该生产代码尝试。

保持10独立进程×80领取槽、1000任务和全部断言不变的对照：

| 路径 | 命令/方法 | 结果 |
|---|---|---|
| macOS→OrbStack映射/容器IP | 原测试重复5次 | 出现TCP超时，失败 |
| Linux测试程序→同容器Redis | 同源码交叉编译GOOS=linux/GOARCH=arm64，重复5次 | 通过 |
| macOS→本机原生Redis | 原测试重复5次 | 通过，约1.46s |

证据将问题范围缩小到宿主机/虚拟机网络路径，不声称已定位虚拟化软件内部根因。正式验收改用可稳定复现的本机真实Redis，未放宽超时、并发或正确性断言；实际部署网络仍须重新验收。另修复重叠go test共享DB的fixture隔离问题，避免交叉清库。

## 基础设施和性能工具

阶段7的 `tests/acceptance` 自动启动一次性 Redis 子进程与三 Sentinel，使用随机 loopback 端口和 Go 测试临时目录；`FAIRQ_REDIS_SERVER` 指定支持 TLS 的 Redis 8.0.2 可执行文件，未指定时查 PATH，不可用直接失败。证书/测试口令仅在临时隔离环境生成/使用，进程退出由测试清理。标准 race 全量回归包含这些演练。

工具用途、参数和结果说明见 [tools/README.md](../tools/README.md)。`loadtest` 默认持续负载时间为30秒，另计预置和排空耗时；日常验证使用小积压，避免排空本身耗时过长：

```sh
go run ./tools/loadtest -redis 127.0.0.1:16400 -groups 10 -backlog 100 -processes 2 -slots 4 -handler-time 10ms -report /path/to/smoke.json
```

工具拒绝非空DB5，绝不自动FlushDB。命令成功退出说明该次负载的正确性检查通过；JSON中的 `Passed` 仍表示原P01完整长稳验收，短跑保持false，不能据此宣称生产长稳通过。外部执行记录按稳定ID守恒，不增加Redis账本Key。30秒冒烟也不覆盖60秒后才开始的goroutine增长判定。

历史首轮曾执行1000 Group、10进程×80槽、约10万Pending、Handler1s的60分钟长稳，结果失败，见 [原始报告](reports/soak-20261014-failed.json)。保留原验收阈值和失败证据；当前 goroutine 峰值问题暂缓，不自动重跑长测或继续诊断。

`go run ./tools/benchmark -redis 127.0.0.1:16401 -report /path/to/perf.json` 同样只接受空DB5；预置1000 Group/10万任务，独立测量99999次Dequeue（另1次预热）、10万Lease集中到期恢复。每批≤100/每tick≤500，批间10ms；为了测极端压力，tick之间不等10s。该结果不能当作默认RecoverInterval下的清空时间。工具保留10万Retry供核查，运行结束后只可清理自己建立的隔离实例。

性能Redis必须独立：benchmark 会 CONFIG RESETSTAT；运行前须配置显式maxmemory/noeviction，推荐AOF everysec。历史长稳与后续短测为16400/DB5，性能为16401/DB5；768MiB预算。采样使用Apple M4 Pro、48GiB RAM、macOS arm64、Go1.27.1、Redis8.0.2，loopback网络；正式数据中心硬件、网络与生产配置仍须复测。

历史报告及开发进度中的 `cmd/fairq-load`、`cmd/fairq-perf` 是当时的源码路径，保留原样供对应提交复现；当前入口分别为 `tools/loadtest`、`tools/benchmark`。
