# Redust

Redust 是一个使用 Rust 编写的、兼容 Redis 协议的轻量级服务。项目目标是提供一个国产、自主可控的 Redis 替代，目前已支持一组基础的 Redis 命令，并保持与 Redis 协议（RESP2）尽量一致的行为。

## 功能特点

- **兼容 Redis 协议**：
  - 已支持的核心命令示例（不完全列表）：
    - 通用：`PING`、`ECHO`、`QUIT`、`TYPE`、`KEYS`、`DBSIZE`、`INFO`、`EXPIRE`/`PEXPIRE`、`TTL`/`PTTL`、`PERSIST` 等。
    - Strings：`SET`（含 NX/XX/KEEPTTL/GET 等扩展选项）、`GET`、`DEL`、`EXISTS`、`INCR`/`DECR`、`INCRBY`/`DECRBY`、`INCRBYFLOAT`、`APPEND`、`STRLEN`、`GETSET`、`GETRANGE`/`SETRANGE`、`MGET`、`MSET`、`MSETNX`、`SETNX`、`SETEX`/`PSETEX`、`GETDEL`、`GETEX` 等。
    - Lists：`LPUSH`、`RPUSH`、`LPOP`、`RPOP`、`LRANGE` 等。
    - Sets：`SADD`、`SREM`、`SMEMBERS`、`SCARD`、`SISMEMBER`、`SUNION`、`SINTER`、`SDIFF` 及部分存储型变体。
    - Hashes：`HSET`、`HGET`、`HGETALL`、`HDEL`、`HEXISTS`、`HINCRBY` 等常用命令。
  - 更完整、实时的命令支持情况请参考仓库根目录的 `command.md`。
  - 协议层基于 RESP2，实现了数组解析与 Bulk String 编解码。
- **异步高并发**：基于 Tokio 运行时，每个 TCP 连接在独立任务中处理，支持多客户端并发访问同一存储。
- **内存键值存储 + 过期**：提供内置内存存储引擎，支持字符串、列表、集合和哈希类型，支持 TTL/过期时间与懒删除 + 定期删除策略。
- **可配置内存上限与 LRU 淘汰（MVP）**：支持通过 `maxmemory`（字节或 MB/GB 后缀）限制内存使用，当逼近上限时采用 `allkeys-lru` 采样淘汰最近最少使用的键（近似实现）。
- **可配置监听地址**：通过 `REDUST_ADDR` 环境变量或 `--bind` 启动参数调整监听地址与端口。

## 快速开始

1. 安装 Rust 稳定版工具链（推荐使用 [rustup](https://rustup.rs/)）。
2. 克隆仓库并进入项目目录：

   ```bash
   git clone <repo-url>
   cd Redust
   ```

3. 启动服务（默认监听 `127.0.0.1:6379`）：

   ```bash
   cargo run
   ```

   如果需要自定义地址或端口：

   ```bash
   REDUST_ADDR="0.0.0.0:6380" cargo run

   # 或使用命令行参数（优先级高于环境变量）
   cargo run -- --bind 0.0.0.0:6380
   ```

## 配置与运行参数

Redust 通过环境变量和少量 CLI 参数进行配置：

- `REDUST_ADDR`：TCP 监听地址，默认 `127.0.0.1:6379`。
- `REDUST_RDB_PATH`：RDB 快照路径，默认 `./redust.rdb`。
- `REDUST_RDB_AUTO_SAVE_SECS`：开启自动 RDB 保存的间隔秒数（可选）。
- `REDUST_METRICS_ADDR`：Prometheus 指标导出地址，例如 `127.0.0.1:9898`。
- `REDUST_MAXMEMORY_BYTES`：最大内存预算：
  - 纯数字：按字节解析，例如 `104857600`。
  - 或带单位：`64KB` / `100MB` / `1GB`（大小写不敏感）。
  - `0` 或未设置：表示不限制内存使用，不触发 LRU 淘汰。
- `REDUST_MAXVALUE_BYTES`：单个 value 最大字节数（可选）：
  - 纯数字或带单位，解析规则与 `REDUST_MAXMEMORY_BYTES` 一致。
  - 目前会限制字符串/列表/集合/哈希写入中的 value 长度（如 `SET`/`MSET`/`LPUSH`/`SADD`/`HSET` 等），超限时返回 `ERR value exceeds REDUST_MAXVALUE_BYTES` 并拒绝写入。
- `REDUST_AUTH_PASSWORD`：全局认证密码（可选）：
  - 未设置或为空：不启用认证，所有命令无需 AUTH 即可执行。
  - 设置非空值：启用基于密码的简单认证，未认证连接仅允许执行 `PING`/`ECHO`/`QUIT`/`AUTH`。

CLI 参数（在 `cargo run -- ...` 之后传入）：

- `--bind <addr>`：覆盖 `REDUST_ADDR`。
- `--maxmemory-bytes <value>`：覆盖 `REDUST_MAXMEMORY_BYTES`，支持与环境变量相同的写法。

## 协议示例

使用 `redis-cli` 或 `nc` 都可以进行测试：

```bash
# 使用 redis-cli
redis-cli -h 127.0.0.1 -p 6379 PING
# => PONG

redis-cli -h 127.0.0.1 -p 6379 ECHO hello
# => "hello"

redis-cli -h 127.0.0.1 -p 6379 SET foo bar
redis-cli -h 127.0.0.1 -p 6379 GET foo
# => "bar"

redis-cli -h 127.0.0.1 -p 6379 INCR cnt
redis-cli -h 127.0.0.1 -p 6379 TYPE cnt
redis-cli -h 127.0.0.1 -p 6379 KEYS "*"

# 查看当前 key 数与内存使用估算
redis-cli -h 127.0.0.1 -p 6379 DBSIZE
redis-cli -h 127.0.0.1 -p 6379 INFO | grep memory
# => maxmemory / maxmemory_human / used_memory / used_memory_human

# 或使用 nc 直接发送 RESP：
printf "*1\r\n$4\r\nPING\r\n" | nc 127.0.0.1 6379

# 启用 AUTH 后的简单示例
# 假设以 REDUST_AUTH_PASSWORD=secret 启动服务：
REDUST_AUTH_PASSWORD="secret" cargo run

# 未认证时只能执行 PING / ECHO / QUIT / AUTH：
redis-cli -h 127.0.0.1 -p 6379 SET k v
# => (error) NOAUTH Authentication required

# 先 AUTH 再写入：
redis-cli -h 127.0.0.1 -p 6379 AUTH secret
redis-cli -h 127.0.0.1 -p 6379 SET k v
redis-cli -h 127.0.0.1 -p 6379 GET k
# => "v"
```

## 运行测试

项目包含协议/命令层单元测试，以及针对服务器行为的集成测试和简单性能回归测试：

```bash
cargo test
```

- 单元测试：
  - `src/resp.rs`：RESP 协议解析与编码测试。
  - `src/command.rs`：命令解析（包括未知命令处理）。
- 集成测试：
  - `tests/server_basic.rs`：使用真实 TCP 连接验证 PING/ECHO/QUIT/SET/GET/DEL/EXISTS/INCR/DECR/TYPE/KEYS 等命令行为，以及多客户端共享存储与基本性能（默认 200 次 PING 往返需在 2 秒内完成）。

## 持久化与 RDB 启动加载

Redust 当前提供了一版**实验性的 RDB v1 快照格式**，用于在重启时恢复内存数据：

- **启动加载**：
  - 服务器启动时会尝试从 RDB 文件中加载现有数据。
  - 默认路径为当前工作目录下的 `redust.rdb`。
  - 可通过环境变量 `REDUST_RDB_PATH` 覆盖，例如：

    ```bash
    REDUST_RDB_PATH="/var/lib/redust/data.rdb" cargo run
    ```

- **加载失败行为**：
  - 当文件不存在、magic/版本不匹配或内容损坏时，当前实现会记录一条日志并**以空库启动**，不会阻止服务监听 TCP 端口。

- **格式说明**：
  - RDB v1 的二进制格式仅用于 Redust 内部，不与官方 Redis RDB 兼容。
  - 详细字段与语义说明见 `doc/rdb.md`。

后续会根据 `roadmap.md` 中的规划，逐步完善持久化能力（包括触发保存的命令/策略、后台保存等）。

## 目录结构

- `src/main.rs`：服务主入口，只负责读取配置并调用库的 `run_server`。
- `src/lib.rs`：库 crate 入口，导出主要模块和 `run_server` API。
- `src/resp.rs`：RESP 协议解析与编码实现。
- `src/command.rs`：命令枚举与从 RESP 到命令的解析逻辑。
- `src/server.rs`：TCP 监听、连接处理和命令执行调度，以及 Prometheus 指标导出。
- `src/storage.rs`：内存存储引擎（基于 `DashMap`），支持 String/List/Set/Hash、TTL/过期、RDB 持久化、可选 `maxmemory` 与 `allkeys-lru` 淘汰策略（近似实现）。
- `tests/server_basic.rs`：端到端集成测试。
- `Cargo.toml`：依赖与构建配置。

## 后续规划

更详细的规划见 `roadmap.md`，当前重点集中在：

- 完善键空间与字符串命令的行为和错误处理，使之更贴近 Redis。
- 规划并实现列表、集合等常见数据结构，以及最小可用的持久化雏形。
- 逐步补充性能与压力测试，以及基础可观测性支持。
