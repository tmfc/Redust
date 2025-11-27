# Redust

Redust 是一个使用 Rust 编写的、兼容 Redis 协议的轻量级服务。项目目标是提供一个国产、自主可控的 Redis 替代，支持基础的 PING/ECHO/QUIT 指令。

## 功能特点

- **兼容 Redis 协议**：当前实现了 `PING`、`ECHO`、`QUIT` 三个常用命令。
- **异步高并发**：基于 Tokio 运行时，处理多连接时每个连接都会在独立任务中执行。
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
# PING
printf "*1\r\n$4\r\nPING\r\n" | nc 127.0.0.1 6379
# 返回
# +PONG

# ECHO
printf "*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n" | nc 127.0.0.1 6379
# 返回
# $5
# hello
```

## 运行测试

项目包含功能测试与简单的性能回归测试：

```bash
cargo test
```

- 功能测试覆盖 `PING`、`ECHO`、`QUIT` 指令。
- 性能测试会在本地循环发送多次 `PING` 来检测基础延迟，默认要求 200 次往返在 2 秒内完成。

## 目录结构

- `src/main.rs`：服务主入口及协议处理逻辑。
- `Cargo.toml`：依赖与构建配置。

## 后续规划

- 支持更多 Redis 指令（如 SET/GET）。
- 持久化及高可用能力。
- 更完善的性能与压力测试工具。
