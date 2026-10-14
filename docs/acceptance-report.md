# FairQ v1.4 验收报告（进行中）

日期：2026-10-14。唯一行为基线为 v1.4；矩阵见 [acceptance.md](acceptance.md)。阶段0–6已验收，阶段7的故障、安全、隔离性能已通过，**首轮长稳因goroutine峰值检查失败，整体生产门禁尚未关闭**。没有已确认未修复的P0架构问题。用户已要求不再运行一小时长测，后续只做短时定向验证；不放宽原验收标准。

## 实测环境

Apple M4 Pro，48GiB内存，macOS arm64，Go1.27.1；Redis8.0.2官方源码、TLS支持。单机loopback网络；Redis独立进程，maxmemory=768MiB、noeviction，长稳/性能AOF everysec。基础设施测试为一次性loopback Redis/Sentinel、AOF always；明确区分各配置的持久性边界。代码提交与功能对应见 [development-progress](development-progress.md)。

本机结果证明实现和该环境下的性能；目标机房、生产TLS/ACL配置、网络故障模型、时钟同步和业务幂等须由上线负责人在实际环境再次验收。这些外部操作本次未执行，不视作通过。

## 已完成证据

| 验收 | 结果与证据 |
|---|---|
| A01–A02、Q01–Q03 | 编码/校验、100并发入队与去重、3×100公平窗口、10000/1000/1000非空窗口、动态Group入退测试通过 |
| Q04、R02–R04 | [同步多进程记录](reports/multiprocess-20261014.txt)：10独立进程×80槽全就绪统一起跑；1000引用无重复；10维护进程恢复/转发各1000次，扫描9894/9939次，无重复转换；600条矛盾Lease队首不饿死合法后继 |
| L01–L03、R01、W01–W03 | 服务端TIME、过期/旧token保护、迟到续租不复活、25/26次失败、退避溢出、Handler error/timeout/panic、失权/断连取消、关闭续租/超时保留通过 |
| X01–X06、O01–O03、C01 | 最小证据先写后删、证据写失败保留引用、容量阈值只告警、解码失败归档、Inspector只读DUMP比较、终态重验与Retention闭环、指标无敏感标签通过 |
| F01–F02 | 8个变更脚本×6类故障，共48例：执行前断连、执行后丢响应、执行后超时、Lua首写前/首写后报错、主索引类型错误。真实TCP代理和真实Redis执行；预校验失败无写入，部分写入保留Hash且Inspector检测到，不声称Lua回滚 |
| F03 | kill -9真实Worker，Lease过期后进入Retry，等待真实15s+抖动退避，再Forward并重新执行；20s完成窗口包含检测和退避，不把RecoverInterval误当总恢复时间 |
| F04、A03 | [基础设施记录](reports/infrastructure-20261014.txt)：AOF崩溃重启95个Hash均恢复，RTO 119.3ms；RDB快照恢复95个Hash，RTO 109.8ms；三Sentinel切换前核实两个副本已应用水位99，100个确认任务均保留，新主可入队，RTO 1.607s。实测丢失0；异步复制不保证生产零RPO |
| F04存活Worker | [真实重启记录](reports/worker-restart-20261014.txt)：Redis停机超过Lease后旧Handler被取消；原Worker重连并完成Recover→真实退避→Forward→新attempt，完整恢复16.180s；迟到旧Handler不能改新状态，race通过 |
| F05、G02技术演练 | 真实noeviction OOM返回INTERNAL并保留已有任务；恢复容量后Inspector健康；TLS可信CA/主机名、ACL命名空间权限通过，错误主机名/越权写拒绝；实际Cluster节点被初始化拒绝 |
| P02、P04 | [原始性能报告](reports/perf-20261014.json)：99999次Dequeue单独采样，Lua p99桶上界0.066ms，API p99 0.160292ms；10万Lease集中到期，100000扫描/100000转换，203轮、每轮最多500条，耗时19.154s，调用p99 0.360875ms；Inspector核对10万任务无异常 |
| P03容量与排空 | 性能实例Redis峰值159347712bytes，占预算19.79%；长稳峰值150837824bytes，占预算18.73%，无eviction，停止生产后126.130s排空，最终Inspector无异常 |
| G03 | README/Runbook提供少Group低并发灰度、新任务路由回Asynq、旧FairQ排空/审计迁移方案；未触碰生产路由 |

Dequeue Lua指标来自隔离Redis `LATENCY HISTOGRAM evalsha`，只包含预热后的Dequeue；桶上界与原始累计计数均保留，不拿平均耗时代替p99。[Redis官方命令说明](https://redis.io/docs/latest/commands/latency-histogram/)定义了该累计直方图。API采样含脚本调用、连接等待和客户端开销；10万Lease恢复是无tick间10s等待的极端压力测试，不能用19.154s推算默认配置的故障恢复SLA。

## 首轮长稳结果

[失败原始报告](reports/soak-20261014-failed.json) 保留了完整样本和错误。持续3600.179s，入队/执行记录均为2973607，重复0；排空后Inspector完整检查，任务和索引均为空。含排空吞吐约798次/s；11766623条Redis命令中1273条超过30ms，比例约0.01082%。任务守恒、内存和延迟子项通过。

P01整体失败：Worker 5在约2310s样本中goroutine由167升至299，前后样本均167；触发工具原有的基线+32断言。尚无当时的goroutine堆栈，不能据此断言为泄漏或正常波动，也不能凭随后恢复就关闭问题。后续最小诊断应在短时压力下采集峰值堆栈；不重复一小时测试，不用短跑替代P01。

补充[短时诊断记录](reports/goroutine-diagnostic-20261014.json)：15秒/8秒/8秒的10进程×80槽测试，包含加速连接空闲淘汰和加密采样；28800/16200/7400任务分别全部排空，无重复或索引异常，未采到超过199的堆栈。此结果没有复现首轮峰值，不能关闭P01；生产代码及验收断言未修改。

## 待完成

X06 后续补充：Inspector 现在能报告 Lease/Retry/Archived/Completed 的非有限、负、小数及超出 2^53−1 的 score，以及 Hash 超范围数值和负 pending_since。新增真实 Redis 用例在修复前复现漏报，修复后 Inspector 与48例故障矩阵 race 回归通过，DUMP 对比确认只读；见 [验证记录](reports/inspector-numeric-20261014.txt)。此修复不改变下面的生产门禁状态。

- P01：goroutine瞬时峰值原因未确认，原长稳验收失败；用户限制后续测试时长，完整长稳复验不执行。
- 最终交付门禁签收；G01完整真实Redis/race回归已通过，见 [原始输出](reports/regression-20261014.txt)。
- 生产目标环境验收与真实业务灰度：未执行，不属于本次本地演练通过范围。

## 运行与边界

复现命令见 [test-environment.md](test-environment.md)。测试不以mock/miniredis代替Redis，不跳过缺失环境；没有删除失败用例或降低阈值。Docker宿主网络路径曾超时，保留失败记录，本机原生Redis对照通过，未放宽生产超时。

Hash存续期去重；成功删除后的同ID可再次入队。默认Done删除，队列不能替业务证明外部副作用是否完成。未知写结果不能盲目换ID重投。At-least-once、Sentinel非零RPO和Lua错误不回滚是既定边界，见 contracts.md 与 runbook.md。
