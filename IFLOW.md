# Redust 项目上下文

## 项目概述

Redust 是一个使用 Rust 编写的、兼容 Redis 协议的轻量级内存数据库服务。项目目标是提供一个国产、自主可控的 Redis 替代方案，目前已支持 120+ 个 Redis 命令，并保持与 Redis 协议（RESP2）的高度兼容性。

### 核心功能
- **Redis 协议兼容**：完整实现 RESP2 协议，支持 String、List、Set、Hash、Sorted Set 等数据结构
- **异步高并发**：基于 Tokio 运行时，支持多客户端并发访问
- **内存存储**：提供内存键值存储，支持 TTL/过期时间与懒删除 + 定期删除策略
- **持久化**：支持 AOF 和 RDB 两种持久化方式
- **Pub/Sub**：支持 channel/pattern/shard 订阅模式
- **事务与脚本**：支持 MULTI/EXEC/DISCARD/WATCH 事务和 Lua 脚本执行
- **内存管理**：可配置内存上限与 LRU 淘汰策略
- **认证安全**：支持基于密码的简单认证

### 技术栈
- **语言**：Rust (edition 2021)
- **异步运行时**：Tokio (full features)
- **并发存储**：DashMap
- **Lua 脚本**：mlua (lua54, vendored)
- **日志**：env_logger + log
- **测试**：内置集成测试 + redis 客户端测试

## 构建和运行

### 环境要求
- Rust 稳定版工具链（推荐使用 [rustup](https://rustup.rs/)）

### 基本命令
```bash
# 构建项目
cargo build --release

# 运行服务（默认监听 127.0.0.1:6379）
cargo run

# 自定义监听地址
REDUST_ADDR="0.0.0.0:6380" cargo run

# 使用命令行参数（优先级高于环境变量）
cargo run -- --bind 0.0.0.0:6380 --maxmemory-bytes 1GB

# 运行测试
cargo test

# 运行特定测试
cargo test server_basic
```

### 环境变量配置
- `REDUST_ADDR`：TCP 监听地址，默认 `127.0.0.1:6379`
- `REDUST_RDB_PATH`：RDB 快照路径，默认 `./redust.rdb`
- `REDUST_RDB_AUTO_SAVE_SECS`：自动 RDB 保存间隔秒数
- `REDUST_METRICS_ADDR`：Prometheus 指标导出地址
- `REDUST_MAXMEMORY_BYTES`：最大内存预算（支持单位：KB/MB/GB）
- `REDUST_MAXVALUE_BYTES`：单个 value 最大字节数
- `REDUST_AUTH_PASSWORD`：全局认证密码
- `REDUST_DISABLE_PERSISTENCE`：禁用持久化（测试用）

### 命令行参数
- `--bind <addr>`：覆盖 `REDUST_ADDR`
- `--maxmemory-bytes <value>`：覆盖 `REDUST_MAXMEMORY_BYTES`

## 项目结构

```
Redust/
├── src/
│   ├── main.rs          # 服务主入口，处理命令行参数和环境变量
│   ├── lib.rs           # 库 crate 入口，导出主要模块
│   ├── resp.rs          # RESP 协议解析与编码实现
│   ├── command.rs       # 命令枚举与解析逻辑
│   ├── server.rs        # TCP 监听、连接处理和命令执行调度
│   ├── storage.rs       # 内存存储引擎，支持各种数据结构和持久化
│   ├── scripting.rs     # Lua 脚本执行引擎
│   ├── hyperloglog.rs   # HyperLogLog 算法实现
│   └── bin/
│       └── resp_set_get_bench.rs  # 性能基准测试
├── tests/               # 集成测试
├── doc/                 # 文档
├── .github/workflows/   # CI/CD 工作流
└── Cargo.toml           # 项目配置和依赖
```

## 开发约定

### 代码风格
- 使用 Rust 标准格式化工具 `rustfmt`
- 使用 Rust 标准检查工具 `clippy`
- 遵循 Rust 2021 Edition 规范

### 测试规范
- 单元测试：各模块内部测试，主要测试协议解析和命令处理
- 集成测试：`tests/` 目录下的端到端测试，使用真实 TCP 连接
- 性能测试：确保基本性能要求（如 200 次 PING 往返需在 2 秒内完成）

### 提交规范
- 参考 `roadmap.md` 中的分阶段规划
- Phase A（核心功能）已完成，Phase B（数据结构扩展）进行中
- 详见 `command.md` 了解具体命令实现进度

### 持久化实现
- RDB v1 格式：自定义二进制格式，不与官方 Redis RDB 兼容
- AOF：支持 everysec 语义的异步写入
- 启动时自动加载 RDB/AOF 数据，失败时以空库启动

## 当前状态

### 已完成功能（Phase A）
- ✅ 核心数据结构：Strings/Lists/Sets/Hashes/Sorted Sets
- ✅ 事务与脚本：MULTI/EXEC/DISCARD/WATCH/UNWATCH，EVAL/EVALSHA/SCRIPT
- ✅ Pub/Sub：channel/pattern/shard 订阅
- ✅ 持久化：AOF + RDB
- ✅ 扫描命令：SCAN/SSCAN/HSCAN/ZSCAN
- ✅ 运维命令：CONFIG GET/SET，CLIENT 管理命令
- ✅ HyperLogLog：PFADD/PFCOUNT/PFMERGE

### 进行中功能（Phase B）
- 🔄 Streams、Geo、Bitmaps 数据结构
- 🔄 主从复制（基础版）
- 🔄 内存管理策略优化

### 规划中功能（Phase C）
- 📋 Sentinel/Cluster 高级特性
- 📋 ACL 权限控制
- 📋 TLS 传输安全

## 快速验证

### 使用 redis-cli 测试
```bash
# 基本连接测试
redis-cli -h 127.0.0.1 -p 6379 PING

# 字符串操作
redis-cli -h 127.0.0.1 -p 6379 SET foo bar
redis-cli -h 127.0.0.1 -p 6379 GET foo

# HyperLogLog 测试
redis-cli -h 127.0.0.1 -p 6379 PFADD visitors user1 user2 user3
redis-cli -h 127.0.0.1 -p 6379 PFCOUNT visitors
```

### 使用 nc 直接测试
```bash
printf "*1\r\n$4\r\nPING\r\n" | nc 127.0.0.1 6379
```

## 开发注意事项

1. **内存管理**：注意 `maxmemory` 配置和 LRU 淘汰策略的影响
2. **持久化**：RDB 格式为自定义格式，不与 Redis 兼容
3. **协议兼容性**：严格遵循 RESP2 协议规范
4. **测试覆盖**：新增功能需要添加对应的单元测试和集成测试
5. **性能要求**：确保基本性能指标满足要求

## 相关文档

- `command.md`：详细的命令实现进度
- `roadmap.md`：项目发展规划
- `doc/rdb.md`：RDB 格式详细说明
- `AGENTS.md`：AI 助手使用说明
- `GEMINI.md`：Gemini 集成说明