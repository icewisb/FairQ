# FairQ 运维手册

适用于 Redis 单实例或 Sentinel 部署。任务语义见 [架构说明](architecture.md)。维护前确认目标实例和备份，所有队列数据位于 `fairq:v1:` 命名空间；其存储格式不兼容其他队列。

## 部署与安全

- 专用 Redis 单实例或 Sentinel，禁用 Cluster。使用 `noeviction`，设置明确内存预算；容量目标为峰值低于可用预算 75%。AOF `everysec` 是性能/持久性取舍，要求更低 RPO 时评估 `always`。副本异步复制仍可能丢最近写入，Sentinel 不等于零 RPO。
- 使用 Redis ACL 限制到 `~fairq:v1:*`。应用需要 INFO（初始化拒绝 Cluster）、TIME、SCAN/SSCAN/ZSCAN、各 List/Set/ZSet/Hash 读写、DEL/EXISTS/TYPE、EVAL/EVALSHA/SCRIPT LOAD，以及客户端握手的 HELLO/AUTH/SELECT/PING/CLIENT SETINFO 等命令。实际最小权限由部署版本的 ACL LOG 演练收敛，不能直接给生产 `+@all`；备份/CONFIG/停服权限仅运维账号持有。SCAN 只读权限也可能看到库中其它 Key 名，使用专用实例。
- 启用 TLS，客户端 TLSConfig 设置可信 CA 和正确 ServerName，禁止 InsecureSkipVerify；私钥和口令从部署密钥系统注入，不入仓库。Sentinel 与 Redis 分别设置认证。为 Dial/Read/Write/Pool 配置超时并验证断连取消。
- Redis 主从与宿主同步时钟；绝对 Lease/Retry/终态时间以 Redis TIME 为准。监控跳时及主从时间差。为 Lua 执行和网络抖动留出 Heartbeat/Lease 间隔余量。

## 观测与告警

应用自行挂载受保护的 MetricsHandler，不对公网开放。进程重启清零计数器；Redis 深度是即时近似值。每次抓取分批读取 Pending 队尾计算最老等待，设置合理抓取间隔（建议 15–30s）并监控抓取负载。

| 信号 | 处置 |
|---|---|
| `fairq_bad_refs >= 10000` 或持续增长 | 高优先级调查，导出证据；继续保留，不 LTRIM |
| `fairq_archived >= 10000` | 容量告警，按失败原因调查；不拒绝归档，不按数量裁剪 |
| operations 的 INTERNAL/LEASE_EXPIRED 增长 | 查 Redis、时钟、连接和 Handler；错误不代表脚本未执行 |
| `fairq_lease_overdue_seconds` / `fairq_retry_overdue_seconds` 持续增长 | 核对维护器存活、扫描/转换比和坏引用；对照配置与退避解释恢复延迟 |
| `fairq_oldest_pending_wait_seconds` 增长 | 查吞吐、活跃组、Pending 可达性；按业务 SLA 告警 |
| Redis 内存接近预算 75%、evicted_keys>0、OOM | 立即暂停新生产流量，保留在途续租能力，扩容或恢复；不得删除证据换吞吐 |

端到端命令 p99 使用 `histogram_quantile(0.99, sum by (le,command) (rate(fairq_redis_command_duration_seconds_bucket[5m])))`；按业务 SLA 设置阈值。桶给出估计，精确测量用原始延迟采样。Dequeue Lua p99 在隔离基准中用 Redis LATENCY HISTOGRAM，不以平均值代替。指标不要增加 TaskID/Group/Payload 标签。结构化日志仅包含 TaskID、编码后的 Group、尝试 token 摘要和错误码，禁止打印 Payload 或完整 token。

## 异常与受控修复

1. 记录时间、环境、报警和操作者；暂停相关生产者/Worker，继续保护健康在途状态，必要时等待其完成。导出相关 Hash、Pending、ready/members、Active/Lease、Retry/Archived/Completed 和 bad_refs；保存在受控存储，记录校验值。Payload 可能敏感。
2. 用只读 Inspector 分批检查。在线报告只是候选异常，停写窗口重新确认。`INTERNAL` 可能留下 Lua 部分写入；Redis Lua 的原子性不等于错误回滚。任务仍有 Hash 或证据时不能自行假定已完成/未执行。
3. 只移除已确认错误的引用；合法 Active/Lease/新 token 必须保留。缺 Hash 的 Pending 或已正确索引的其它状态引用由现有 Lua 保存证据后隔离。矛盾/未知状态禁止自动执行，交业务确认副作用与幂等记录。
4. 人工修复逐条进行，脚本必须带预期 state/token/索引/引用快照并在同一 Lua 中重验；现态变化立即拒绝。禁止无条件 DEL Hash、无条件重入队、批量猜测修复。修复前保存脚本、输入与证据，执行后再次 Inspector，审计操作者、时间、结果、回滚或补偿决定。当前不提供通用自动 Repair API。
5. bad_refs 完成处理后先可靠导出核对。只人工删除已确认处理的条目，未处理证据绝不裁剪；Archived 90天到期清理前若需长期审计先转储并提高保留期。Completed 默认不启用。

存储故障时禁止不停换 TaskID 重投。Enqueue 返回的 ID 用于核查；Hash 已删除的成功任务无法仅凭队列证明是否曾执行，须查业务幂等记录。Done 响应丢失后不得直接重复副作用。

## 关闭、备份恢复与故障切换

正常关闭先停止新领取，Handler 继续执行和续租；ShutdownTimeout 到期取消 Context、停止续租，保留 Active/Lease 等后续恢复。kill -9 的恢复时间包含 Lease 到期、Recover 检测、15s 起步退避与 Forward 检测。不可承诺只有 RecoverInterval。

备份定期保存 RDB 或完整 AOF 目录（含 manifest/base/incr 文件），记录 Redis 版本、校验、时间和对应业务水位。恢复必须先在隔离实例验证加载、Inspector 和任务水位；禁止覆盖正在运行的生产目录。恢复后所有可能重复执行的任务仍需业务幂等；记录备份水位到故障时刻的实际 RPO、恢复到可处理任务的 RTO。

Sentinel 演练在隔离副本组执行：记录已确认写入水位，停止主节点，等待多数 Sentinel 选主，应用自动发现新主，核对每个 ID 和不变量；记录丢失/重复、RPO/RTO。生产故障时先确认唯一可写主、复制拓扑和时钟，避免旧主重新接受写入。不得把一次零丢失演练解释为系统保证零 RPO。

## 灰度与回退

先少 Group、低并发，观察等待时长、失败率、重复副作用、p99、内存及不变量；每次扩容后保留观察窗口。回退时通过业务路由将**新任务**改投原有处理系统，旧 FairQ 任务继续由 FairQ 排空；需要迁移时按稳定 ID、业务幂等记录和审计逐条处理。不要直接复制 FairQ Key 给其他队列或清空旧队列。上线前在目标环境验证持续负载、故障恢复、业务幂等和容量；本地测试通过不替代这些验证。
