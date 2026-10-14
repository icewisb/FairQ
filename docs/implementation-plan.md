# FairQ 分阶段实施计划

唯一基线：[v1.4](fairq-final-technical-design-v1.4.md)。契约见 [contracts](contracts.md)，验收ID见 [acceptance](acceptance.md)，当前事实见 [development-progress](development-progress.md)。所有目录为拟实现范围，不预建空抽象。

| 阶段 | 依赖 | 目标/关键实现 | 主要文件 | 测试/通过门禁 |
|---|---|---|---|---|
| 0 契约 | 无 | 七项决议、Lua输入输出/失败、状态和验收矩阵 | docs/contracts.md、acceptance.md、implementation-plan.md | 文档自洽、无新增Key/主状态/组件、P0无未决架构项 |
| 1 基础与入队 | 0 | 任务/错误编码、Redis连接、Enqueue/GetTask、UUID、Group激活 | task.go、client/、internal/base/、internal/rdb/、internal/testutil/、go.mod | A01–A03；真实Redis入队与并发去重 |
| 2 公平领取 | 1 | 单Lua Dequeue含Active/Lease/token写入与最小bad_refs | internal/rdb/dequeue.go、lua/dequeue.lua及测试 | Q01–Q04、X01–X03；FIFO/RR/800槽/合法Active不受损 |
| 3 执行所有权 | 2 | Heartbeat/Done、Redis TIME、逐任务token保护 | internal/rdb/{heartbeat,done}.go及Lua/测试 | L01–L03；旧/过期尝试不写状态 |
| 4 失败恢复 | 3 | Retry/Archive、Forward/Recover、上限批次、Clock/RNG | internal/base/retry.go、internal/clock/、internal/rdb/、server维护器 | R01–R04、X04；10进程竞争、25/26计数 |
| 5 Worker | 4 | 并发槽、Handler timeout/panic、取消、Shutdown与退避 | server/{server,config,processor,heartbeat}.go及测试 | W01–W03、X05、F03；race，关闭续租和恢复闭环 |
| 6 运维 | 5 | 只读Inspector、Archived清理、指标、Runbook | inspector/、internal/rdb/clean、server指标、docs/runbook.md | X06、O01–O03；无查询写入、无终态误删 |
| 7 生产验收 | 6 | 全量故障/多进程/长稳/恢复演练，容量与上线文档 | tests/、scripts/、docs/acceptance-report.md、使用说明 | F01–F05、P01–P04、G01–G03全部有真实结果 |

每个独立功能单元：实现→相关测试→回归→对照验收→Git commit→更新进度→自动进入下一单元。失败自主修复，不降低断言、不删除失败用例。未通过阶段门禁不进入下一阶段；环境阻塞与代码失败分开记录，可完成不依赖阻塞项的安全工作。

Completed/Retention已在阶段6实现，默认关闭；正Retention启用保留与到期清理。Redis持久stats延后，使用最小Prometheus指标。无Group GC、优先级、Cron、DAG、Incident/自动Repair平台、选主服务。

Asynq v0.25.1仅作实现参考：processor并发/Context/panic、heartbeat在途管理、server关闭顺序、rdb短Lua和测试fixture。FairQ重写RR、服务端时间、token/到期检查、恢复基准、失败计数、归档保留；不复制ZADD XX、关闭直接重入队、按数量裁剪或无界维护循环。移植源码必须保留MIT声明。参考链接：https://github.com/hibiken/asynq/tree/v0.25.1 。
