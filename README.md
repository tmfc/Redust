# Redust

Redust 是一个使用 Rust 编写的、兼容 Redis 协议的轻量级服务。项目目标是提供一个国产、自主可控的 Redis 替代，目前已支持一组基础的 Redis 命令，并保持与 Redis 协议（RESP2）尽量一致的行为。

## 功能特点

- **兼容 Redis 协议**：
  - 当前支持命令：`PING`、`ECHO`、`QUIT`、`SET`、`GET`、`DEL`、`EXISTS`、`INCR`、`DECR`、`TYPE`、`KEYS`、`LPUSH`、`RPUSH`、`LPOP`、`RPOP`、`LRANGE`、`SADD`、`SREM`、`SMEMBERS`、`SCARD`、`SISMEMBER`、`SUNION`、`SINTER`、`SDIFF`。
  - 协议层基于 RESP2，实现了数组解析与 Bulk String 编解码。
- **异步高并发**：基于 Tokio 运行时，每个 TCP 连接在独立任务中处理，支持多客户端并发访问同一存储。
- **内存键值存储**：提供内置内存存储引擎，支持字符串、列表和集合类型，支持整数自增/自减以及简单的键空间查询。通过 `DashMap` 实现高并发。
- **可配置监听地址**：通过 `REDUST_ADDR` 环境变量即可调整监听地址与端口。

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
   ```

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

# 或使用 nc 直接发送 RESP：
printf "*1\r\n$4\r\nPING\r\n" | nc 127.0.0.1 6379
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
- `src/server.rs`：TCP 监听、连接处理和命令执行调度。
- `src/storage.rs`：简单的内存存储引擎（基于 `HashMap<String, String>`）。
- `tests/server_basic.rs`：端到端集成测试。
- `Cargo.toml`：依赖与构建配置。

## 后续规划

更详细的规划见 `roadmap.md`，当前重点集中在：

- 完善键空间与字符串命令的行为和错误处理，使之更贴近 Redis。
- 规划并实现列表、集合等常见数据结构，以及最小可用的持久化雏形。
- 逐步补充性能与压力测试，以及基础可观测性支持。
