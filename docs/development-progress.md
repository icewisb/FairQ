# FairQ 开发进度

更新时间：2026-10-14（Asia/Shanghai）。开始时必须与git历史、代码和测试核对。

- 当前阶段：4 失败恢复已验收，准备阶段5。
- 已完成：完整复核v1.4；七项契约决议；Lua协议、数据不变量、状态/错误表、全量验收映射；原文归档。
- 已通过：阶段0文档链接/原文SHA256检查；阶段1 `FAIRQ_TEST_REDIS=127.0.0.1:16379 go test ./...` 与 `go test -race ./...`，真实Redis 8.0.2，100并发入队/去重、查询、校验无写入、错误返回ID、Cluster配置拒绝。
- 最近成功Commit：`200929e feat(rdb): fence heartbeat and done with live attempt ownership`；阶段4提交哈希见git log。
- 未完成：阶段5–7；Worker本地取消/迟到续租响应在阶段5验证；Sentinel真实切换/连接验收在阶段7，不把配置支持当作故障演练通过。
- 已知边界：Hash存续期去重；业务At-least-once幂等；Sentinel非零RPO；运行时Lua错误不回滚。
- 阻塞：无已确认P0架构阻塞。codebase-memory索引服务启动失败，当前无代码，已直接检查文件；后续图覆盖不可用时使用源文件核验。隔离Docker容器 fairq-dev-01a13941，127.0.0.1:16379；测试显式选用DB1..15，禁止指向生产。
- 下一步最小任务：Worker池、Handler/context/panic/timeout、续租与Shutdown生命周期。

## 阶段记录

| 阶段 | 状态 | 证据 |
|---|---|---|
| 0 | 已提交 fe901d5 | contracts.md、acceptance.md、implementation-plan.md |
| 1 | 已提交 00511a2 | 上述真实Redis及race结果；A03的Sentinel演练延至G02 |
| 2 | 已提交 f34e85d | 3×100和10000/1000/1000的FIFO/RR；动态组；10进程×80槽领取1000任务；坏引用/证据失败/阈值；go test -race ./...通过 |
| 3 | 已提交 200929e | Redis TIME范围、批量逐项续租、Done删除、过期拒绝、旧token/缺索引保护；go test -race ./...通过；L03的Worker取消在阶段5 |
| 4 | 已验收，准备提交 | 25/26计数、退避RNG/溢出、旧候选、恢复时刻、10进程恢复/转发、坏队首推进；原生Redis上go test -race ./... -count=2通过 |
| 5–7 | 未开始 | 不计为通过 |

每次提交前记录本次验证；本次提交的哈希用git历史核对，下次更新填写，避免自引用提交哈希。禁止声称进程退出后会自动继续；续跑从本文件及实际git状态恢复。

## 环境问题记录

宿主机访问OrbStack Redis在800槽测试中发生TCP超时；相同测试在容器内和本机原生Redis各重复5次通过。当前使用127.0.0.1:16380原生Redis，详见test-environment.md。不将失败网络路径标为通过。测试fixture增加本地数据库锁防止重叠go test交叉清库。生产代码未为该故障放宽超时或加入重试。
