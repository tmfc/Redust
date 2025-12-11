# Repository Guidelines

## Project Overview
Redust 是一个使用 Rust 编写的、兼容 Redis 协议的轻量级服务。项目目标是提供一个国产、自主可控的 Redis 替代，目前已支持 120+ 个 Redis 命令，覆盖 5 种核心数据结构、事务、Lua 脚本、持久化、Pub/Sub 等功能。

## Project Structure & Modules
- `src/main.rs`: 服务主入口，负责读取配置（环境变量和命令行参数）并调用库的 `run_server`。
- `src/lib.rs`: 库 crate 入口，导出主要模块和 `run_server` API。
- `src/server.rs`: TCP 监听、连接处理和命令执行调度，以及 Prometheus 指标导出。
- `src/resp.rs`: RESP 协议解析与编码实现。
- `src/command.rs`: 命令枚举与从 RESP 到命令的解析逻辑，包含 120+ 个 Redis 命令实现。
- `src/storage.rs`: 内存存储引擎（基于 `DashMap`），支持 String/List/Set/Hash/ZSet/HyperLogLog、TTL/过期、RDB/AOF 持久化、可选 `maxmemory` 与 `allkeys-lru` 淘汰策略。
- `src/scripting.rs`: Lua 脚本引擎，支持 `EVAL`/`EVALSHA`/`SCRIPT` 命令和 `redis.call`/`pcall` 回调。
- `src/hyperloglog.rs`: HyperLogLog 数据结构实现。
- `src/bin/`: 性能测试工具。
- `tests/`: 集成测试，包含 99 个测试用例覆盖各种 Redis 功能。
- `Cargo.toml`: Crate metadata 和依赖配置，使用 Tokio full features。
- `README.md`: 快速开始、协议示例和功能特性文档。
- `command.md`: 详细的命令实现进度和兼容性说明。
- `roadmap.md`: 项目发展路线图。

## Build, Test, and Development
- `cargo run`: 启动 Redis 兼容的 TCP 服务器（默认绑定 `127.0.0.1:6379`）。
- `REDUST_ADDR="0.0.0.0:6380" cargo run`: 覆盖监听地址/端口。
- `cargo run -- --bind 0.0.0.0:6380 --maxmemory-bytes 1GB`: 使用命令行参数覆盖配置。
- `cargo test`: 运行单元/集成/性能测试，包含 99 个测试用例。
- `cargo fmt && cargo clippy -- -D warnings`: 格式化和代码检查，推送前必须执行。

## Configuration
项目通过环境变量和命令行参数进行配置：
- `REDUST_ADDR`: TCP 监听地址（默认 `127.0.0.1:6379`）
- `REDUST_RDB_PATH`: RDB 快照路径（默认 `./redust.rdb`）
- `REDUST_RDB_AUTO_SAVE_SECS`: 自动 RDB 保存间隔秒数
- `REDUST_METRICS_ADDR`: Prometheus 指标导出地址
- `REDUST_MAXMEMORY_BYTES`: 最大内存限制（支持 KB/MB/GB 后缀）
- `REDUST_MAXVALUE_BYTES`: 单个 value 最大字节数限制
- `REDUST_AUTH_PASSWORD`: 全局认证密码

## Coding Style & Naming
- Rust 2021 edition; 优先使用小而专注的函数和显式 match 语句。
- 函数/模块使用 snake_case，类型/枚举使用 PascalCase，常量/环境变量使用 SCREAMING_SNAKE_CASE。
- 日志保持简洁结构化（`[conn]`, `[resp]`, `[cmd]` 等前缀），避免在热路径上使用 println!。
- 使用 `DashMap` 进行并发存储，`Tokio` 进行异步处理，`OrderedFloat` 进行有序集合分数比较。

## Testing Guidelines
- 单元测试位于各模块文件末尾的 `#[cfg(test)]` 块中。
- 集成测试位于 `tests/` 目录下，按功能分类（如 `server_basic.rs`, `auth.rs`, `pubsub.rs` 等）。
- 测试命名关注行为（`responds_to_basic_commands`, `performance_ping_round_trips`），性能敏感测试包含时间断言。
- 优先使用 `#[tokio::test]` 进行异步测试，延迟检查时使用 `Duration` 限制运行时间。
- 使用 `serial_test` 确保某些测试的串行执行。

## Commit & Pull Requests
- 提交信息使用短祈使句主语（`Add RESP parser`, `Tighten ping perf guard`），相关变更分组提交，避免重构与行为变更混合。
- Pull Request 包含变更内容、原因和验证方式（运行的命令、预期响应），关联相关 Issue，注明协议变更（新增命令或破坏性响应）。
- 为 bug 修复添加最小复现片段（如触发 bug 的 RESP 数组），用户行为变更时更新 README 片段。

## Security & Config Notes
- 网络绑定由 `REDUST_ADDR` 控制，默认为 loopback，避免提交未提及的 `0.0.0.0` 测试。
- 支持认证功能（`AUTH` 命令），未认证连接仅允许执行 `PING`/`ECHO`/`QUIT`/`AUTH`。
- 持久化层包含 AOF 和 RDB 两种格式，添加状态特性时需在 README/PR 说明中记录存储路径和故障模式。
- 支持内存限制和 LRU 淘汰策略，通过 `REDUST_MAXMEMORY_BYTES` 配置。

## Development Workflow
1. 新功能开发前先查看 `command.md` 了解命令实现状态
2. 参考 `roadmap.md` 了解项目发展方向
3. 添加新命令时在 `src/command.rs` 中实现，并在 `tests/` 中添加对应测试
4. 确保所有测试通过：`cargo test`
5. 代码质量检查：`cargo fmt && cargo clippy -- -D warnings`
6. 更新相关文档（README.md、command.md 等）
