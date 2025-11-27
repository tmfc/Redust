# Redust Redis 兼容路线图

本路线图用于规划 Redust 从当前的轻量实现，逐步演进为**协议与行为尽量兼容 Redis 的服务端实现**。路线图会随着实现推进不断调整，但整体分为以下几个阶段：

- **阶段 0：当前状态与基础设施**
- **阶段 1：核心协议与基础命令集**（当前主要关注点）
- **阶段 2：键值数据结构与持久化雏形**
- **阶段 3：高可用、性能与运维特性**
- **阶段 4：高级特性与生态兼容**

---

## 阶段 0：当前状态与基础设施

目标：确保当前原型可用、有基本测试与开发规范，为后续扩展打好基础。

- **状态**：
  - 支持 RESP 基础解析与少量命令（如 PING/ECHO/QUIT）。
  - 单文件 `src/main.rs`，测试也集中在其中。
  - 无持久化，无多实例/集群能力。

- **方向**：
  - 保持简单可运行的 demo，同时逐步抽象代码结构，为支持更多命令做准备。

---

## 阶段 1：核心协议与基础命令集（已完成，持续打磨中）

目标：
- 将 Redust 从“演示性质”推进到“可以被普通 Redis 客户端（如 redis-cli）稳定使用”的程度。
- 代码结构从单文件演进为清晰的多模块架构，便于扩展新命令。
  
当前状态：
- 已有较完整的 RESP2 解析与编码工具，支持基础错误处理。
- 代码已拆分为 `lib.rs` + `resp.rs` + `command.rs` + `server.rs` + `storage.rs` 等模块。
- 命令层覆盖：
  - 连接与元信息：`PING`、`ECHO`、`QUIT`。
  - 字符串与键空间：`SET`、`GET`、`DEL`、`EXISTS`、`INCR`/`DECR`、`TYPE`、`KEYS`。
- 集成测试：`tests/server_basic.rs`、`tests/standard_set_get.rs` 覆盖常见命令与性能基线。
- Benchmark：`src/bin/resp_set_get_bench.rs` 提供以 RESP 客户端驱动的简单 QPS 测试（包含 SET/GET、INCR/DECR、EXISTS/DEL、列表操作分组）。

### 1.1 协议与架构目标

- **协议层**：
  - 完整、健壮的 RESP2 解析与编码（包含错误处理、边界情况）。
- **架构层**：
  - 将“入口 / 启动逻辑”与“业务逻辑”分离：
    - `main.rs` 只负责：配置解析 + 监听端口 + 启动服务器。
    - `lib.rs` 及子模块负责：RESP、命令解析/执行、连接处理。

- **命令层（建议优先级）**：
  - 连接与元信息：`PING`、`ECHO`、`QUIT`、`CLIENT` 部分子命令（可选）。
  - 字符串基础：`SET`、`GET`、`DEL`、`EXISTS`、`INCR`/`DECR`。
  - 键空间操作：`KEYS`（注意性能提示）、`TYPE`、`EXPIRE`、`TTL`（可简化）。

### 1.2 代码结构规划

按职责拆分为若干模块（初稿）：

- `src/lib.rs`
  - 对外暴露主要 API，例如 `run_server`。
  - `pub mod resp;`
  - `pub mod command;`
  - `pub mod server;`
  - 之后根据需要增加 `mod storage; mod error;` 等。

- `src/resp.rs`
  - 定义 RESP 数据结构，例如：`enum RespValue { ... }`。
  - 解析：字节流 -> `RespValue`。
  - 编码：`RespValue` -> 字节流。
  - 针对不合法输入的错误处理与测试。

- `src/command.rs`
  - 命令枚举：`enum Command { Ping, Echo(String), Set { ... }, Get { ... }, ... }`。
  - 从 `RespValue` 解析到 `Command` 的逻辑（含错误响应）。
  - 针对单条命令的“纯逻辑执行”接口（不直接操作 socket）。

- `src/server.rs`
  - `run_server(addr: &str)`：监听、accept、spawn 连接任务。
  - 连接处理：读取 RESP、调用 `command` 模块、写回结果。
  - 简单的日志 / 错误处理。

- （后续）`src/storage.rs`
  - 简单的内存存储引擎（比如基于 `HashMap<String, Value>`）。
  - 提供线程安全访问接口（如基于 `Arc<RwLock<...>>`）。

### 1.3 阶段 1 详细 TODO（已基本完成）

> 下列任务按推荐顺序排列；当前绝大部分已落地，后续只需在行为兼容性和性能上持续微调即可。

1. **抽离库 crate 结构**
   - [x] 新建 `src/lib.rs`，将 `main.rs` 中“与 Tokio 启动无关”的逻辑移动到 `lib.rs`。
   - [x] 在 `lib.rs` 中定义对外暴露的入口（当前为 `run_server`）。
   - [x] 修改 `main.rs`：
     - [x] 仅保留 `#[tokio::main] async fn main()` 和配置解析。
     - [x] 调用 `redust::run_server(addr, shutdown).await` 启动服务。

2. **按职责拆分模块**
   - [x] 从 `lib.rs` 中抽取 RESP 相关代码到 `src/resp.rs`，并在 `lib.rs` 中 `pub mod resp;`。
   - [x] 从 `lib.rs` 中抽取命令解析/执行代码到 `src/command.rs`，并在 `lib.rs` 中 `pub mod command;`。
   - [x] 从 `lib.rs` 中抽取网络与连接处理逻辑到 `src/server.rs`，并在 `lib.rs` 中 `pub mod server;`，同时通过 `pub use server::run_server;` 对外统一导出。
   - [x] 新增 `src/storage.rs`，提供线程安全内存存储引擎。

3. **整理测试布局**
   - [x] 将协议/命令相关的测试迁移到对应模块的 `#[cfg(test)] mod tests` 中（例如 `resp.rs` / `command.rs`）。
   - [x] 在 `tests/` 目录中新增集成测试 `tests/server_basic.rs`，用真实 TCP 连接进行端到端验证（包含性能基线）。

4. **扩展基础命令集（字符串 + 键空间）**
   - [x] 设计并实现内存存储引擎 `storage` 模块（目前支持字符串类型）。
   - [x] 在 `command` / `server` 模块中实现：`SET`、`GET`、`DEL`、`EXISTS` 基本行为，与 Redis 尽量保持兼容（包含返回值与错误）。
   - [x] 增加 `INCR`/`DECR`，并在解析失败或溢出时返回 `-ERR value is not an integer or out of range`。
   - [x] 增加 `TYPE`、`KEYS`（当前 `KEYS` 支持 `*` 与精确匹配，后续可逐步强化性能与匹配规则）。

5. **文档与约定同步**
   - [x] 更新 `README.md`：
     - [x] 描述当前支持的命令列表与使用示例。
     - [x] 简要描述项目当前的模块划分（入口、协议、命令、存储、集成测试位置）。
   - [ ] 更新 `AGENTS.md` 或新增开发文档：
     - [ ] 说明测试的推荐位置（模块内单元测试 + `tests/` 集成测试）。
     - [ ] 说明新增命令的基本流程（修改哪些模块、如何补测试）。
   - [ ] 补充与 Benchmark 相关的开发建议（如何运行 `resp_set_get_bench`、各 group 的含义等）。

---

## 阶段 2：数据结构与持久化雏形（进行中）

目标：在基础命令集之上，逐步支持 Redis 常见数据结构和最简单形式的持久化。

当前状态：
- 已实现的数据结构子集：
  - **列表（List）**：
    - 存储层：`storage.rs` 中维护 `lists: HashMap<String, VecDeque<String>>`，并提供 `lpush` / `rpush` / `lrange` 等操作。
    - 命令层：`LPUSH`、`RPUSH`、`LRANGE` 已在 `command.rs` 和 `server.rs` 中打通。
    - 行为验证：
      - `tests/server_basic.rs::lists_basic_behaviour` 覆盖基础列表操作语义（含下标和负索引）。
      - `resp_set_get_bench.rs` 中的 `list_ops` group 对列表操作做简单性能/功能回归。
  - **集合（Set）**：
    - 存储层：`storage.rs` 中维护 `sets: HashMap<String, HashSet<String>>`，并提供 `sadd` / `srem` / `smembers` / `scard` / `sismember` 等操作。
    - 命令层：`SADD`、`SREM`、`SMEMBERS`、`SCARD`、`SISMEMBER` 已在 `command.rs` 和 `server.rs` 中实现。
    - 行为验证：`tests/server_basic.rs::sets_basic_behaviour` 覆盖集合的基础语义与去重行为。
- 尚未实现：
  - 列表的弹出类操作（如 `LPOP` / `RPOP`）。
  - 哈希、有序集合等更复杂数据结构。
  - 任何形式的持久化（当前仍为纯内存）。

阶段 2 计划拆分（草案）：
- **2.1 列表能力补全**
  - [ ] 为 `Storage` 增加 `lpop` / `rpop` 等接口，并在 `command` / `server` 中接线。
  - [ ] 补充列表边界条件测试（空列表弹出、多客户端并发等）。

- **2.2 集合能力增强（可选）**
  - [ ] 视需要增加 `SUNION` / `SINTER` 等读操作（可只做子集）。
  - [ ] 为集合操作补充更多交叉测试用例。

- **2.3 其他数据结构预研**
  - [ ] 设计哈希（`HSET`/`HGET` 等）在现有 `Storage` 里的表示方式。
  - [ ] 预估有序集合（Sorted Set）的最小实现成本，暂不急于落地。

- **2.4 持久化雏形**
  - [ ] 选定最小可行的持久化方式（例如：周期性内存快照到本地文件）。
  - [ ] 实现一个简化版的“保存/加载”接口，用于开发环境下的状态保留（不追求 Redis 完整语义）。
  - [ ] 在 README 中明确记录持久化语义与 Redis 的差异和限制。

---

## 阶段 3：高可用、性能与运维特性（规划中）

- 更完善的连接管理、超时、慢查询观测。
- 性能优化（批量处理、pipeline、内存布局优化等）。
- 基本监控指标和简单的可观测性支持。

---

## 阶段 4：高级特性与生态兼容（规划中）

- 更丰富的数据结构和模块能力。
- 与 Redis 生态工具的兼容性增强（如哨兵/集群行为的部分模拟等）。

---

> 本文件会随着实现推进持续更新。当前优先级已经逐步转向 **阶段 2：数据结构与持久化雏形**，在保持阶段 1 命令稳定性的前提下，优先补齐列表/集合等基础数据结构，再循序渐进探索最小持久化方案。
