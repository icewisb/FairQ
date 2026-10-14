# 开发测试指南

准备 Go 1.24+ 和专用、可丢弃的真实 Redis。当前验证过 Redis 8.0.2；完整测试还需要支持 TLS 的 `redis-server`，不声明未验证版本的兼容性。

## 本地环境

在单独终端启动测试实例，不要使用生产或共享业务实例：

```sh
redis-server --bind 127.0.0.1 --port 16380 --save '' --appendonly no --maxmemory 768mb --maxmemory-policy noeviction
```

该实例不启用持久化，仅用于本地测试。在仓库根目录运行：

```sh
export FAIRQ_TEST_REDIS=127.0.0.1:16380
export FAIRQ_REDIS_SERVER=/path/to/tls-enabled/redis-server
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
```

`FAIRQ_REDIS_SERVER` 替换为实际可执行文件路径；未设置时基础设施测试查找 PATH 中的 `redis-server`。真实 Redis 测试缺少环境时失败，不静默跳过。

## 数据隔离

测试会执行 FlushDB，包使用以下数据库：

| 包 | DB |
|---|---|
| `client` | 1 |
| `internal/rdb` | 2 |
| `server` | 3 |
| `inspector` | 4 |
| `examples/basic` | 6 |

同一 DB 的顶层 fixture 使用本地文件锁，避免本机重叠测试交叉清库；子进程只接入，不清库。文件锁不保护来自其他机器的测试，不应让多台机器共享测试实例。

`tests/acceptance` 额外启动一次性 Redis 和 Sentinel 进程，使用随机 loopback 端口和临时目录，覆盖重启、备份恢复、故障切换、ACL/TLS 和 OOM。测试生成临时证书并负责清理进程；这些用例属于普通全量测试，不是持续负载测试。

## 修改与验证

先运行受影响的包，再按改动范围运行全量测试；涉及并发、租约或共享状态时使用 `-race`。测试应断言可观察行为和数据不变量，保留已修复缺陷的回归用例，避免只重复实现步骤。格式检查使用 `gofmt`，静态检查使用 `go vet`。

普通 `go test` 不启动长稳或十万任务性能测试。需要测量负载时，使用独立实例并按 [工具说明](../tools/README.md) 显式运行；短测不能替代持续负载和目标部署环境验证。项目验证边界见 [README](../README.md)。
