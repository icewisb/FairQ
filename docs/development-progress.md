# FairQ 开发进度

更新时间：2026-10-14（Asia/Shanghai）。开始时必须与git历史、代码和测试核对。

- 当前阶段：6 运维能力已验收，准备阶段7。
- 已完成：完整复核v1.4；七项契约决议；Lua协议、数据不变量、状态/错误表、全量验收映射；原文归档。
- 已通过：阶段0文档链接/原文SHA256检查；阶段1 `FAIRQ_TEST_REDIS=127.0.0.1:16379 go test ./...` 与 `go test -race ./...`，真实Redis 8.0.2，100并发入队/去重、查询、校验无写入、错误返回ID、Cluster配置拒绝。
- 最近成功Commit：`fd0ba7e feat(server): run bounded workers with lease-aware graceful shutdown`；阶段6提交哈希见git log。
- 未完成：阶段7；Sentinel真实切换/连接验收在阶段7，不把配置支持当作故障演练通过。
- 已知边界：Hash存续期去重；业务At-least-once幂等；Sentinel非零RPO；运行时Lua错误不回滚。
- 阻塞：无已确认P0架构阻塞。codebase-memory索引服务启动失败，已直接检查源文件；后续图覆盖不可用时使用源文件核验。隔离Docker容器 fairq-dev-01a13941，127.0.0.1:16379；测试显式选用DB1..15，禁止指向生产。
- 下一步最小任务：故障注入矩阵、隔离Redis恢复/Sentinel演练和性能长稳工具。

## 阶段记录

| 阶段 | 状态 | 证据 |
|---|---|---|
| 0 | 已提交 fe901d5 | contracts.md、acceptance.md、implementation-plan.md |
| 1 | 已提交 00511a2 | 上述真实Redis及race结果；A03的Sentinel演练延至G02 |
| 2 | 已提交 f34e85d | 3×100和10000/1000/1000的FIFO/RR；动态组；10进程×80槽领取1000任务；坏引用/证据失败/阈值；go test -race ./...通过 |
| 3 | 已提交 200929e | Redis TIME范围、批量逐项续租、Done删除、过期拒绝、旧token/缺索引保护；go test -race ./...通过；L03的Worker取消在阶段5 |
| 4 | 已提交 254893e | 25/26计数、退避RNG/溢出、旧候选、恢复时刻、10进程恢复/转发、坏队首推进；原生Redis上go test -race ./... -count=2通过 |
| 5 | 已提交 fd0ba7e | 并发上限、error/timeout/panic/decode、失权/断连取消、关闭续租与超时保留、受控Clock验证退避停领/重置、迟到续租不可复活；kill -9后真实15s+抖动退避恢复；go test -race ./...通过（server约24s） |
| 6 | 已验收，准备提交 | Inspector只读DUMP对比及损坏检测、Completed公共API闭环、旧候选保护、10000归档阈值、终态清理轮流优先、Prometheus无敏感标签；真实Redis go test -race ./...及go vet ./...通过；README/Runbook已交付 |
| 7 | 未开始 | 故障/性能/长稳未执行项不计为通过 |

每次提交前记录本次验证；本次提交的哈希用git历史核对，下次更新填写，避免自引用提交哈希。禁止声称进程退出后会自动继续；续跑从本文件及实际git状态恢复。

## 环境问题记录

宿主机访问OrbStack Redis在800槽测试中发生TCP超时；相同测试在容器内和本机原生Redis各重复5次通过。当前使用127.0.0.1:16380原生Redis，详见test-environment.md。不将失败网络路径标为通过。测试fixture增加本地数据库锁防止重叠go test交叉清库。生产代码未为该故障放宽超时或加入重试。
