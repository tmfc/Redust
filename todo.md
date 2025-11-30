# Redust 项目待办事项

本文件用于跟踪当前“可以立刻开干”的小类工作，同时保留长期问题列表。中长期路线请参考 `roadmap.md`。

---

## 当前聚焦的小类工作

- [x] **字符串多 key / 原子操作补齐（v1）**
  - 实现并测试：`MGET` / `MSET` / `SETNX` / `SETEX` / `PSETEX` / `INCRBY` / `DECRBY` 等子集。
  - 确认与 Redis 语义对齐（返回值、错误信息、过期语义交互）。
  - 补充 `tests/` 下端到端用例，并在 `README.md` 示例中体现。（测试已补，README 示例待后续完善）

- [x] **Hash 数据结构雏形（HSET/HGET 核心子集）**
  - 设计 `Storage` 中 Hash 的内部存储结构（如 `HashMap<String, String>` 封装）。
  - 实现并测试：`HSET` / `HGET` / `HDEL` / `HEXISTS` / `HGETALL` 的最小集合。
  - 使用 `redis-cli` 做基本兼容性验证，补充文档示例。

- [x] **基础性能基准测试（单机内网场景）**
  - 使用 `redis-benchmark` / 内置 bench，对 PING/SET/GET 做 QPS 与 P99 延迟测量。
  - 记录一版基线数据（本地开发机），在 `roadmap.md` 或 `future.md` 中留存。（当前基线已记录在 `roadmap.md` 中）
  - 如发现明显瓶颈，再在 `future.md` 中登记相应优化条目。

- [x] **列表命令补全（LLEN/LINDEX/LREM/LTRIM 子集）**
  - 在现有 `LPUSH` / `RPUSH` / `LPOP` / `RPOP` / `LRANGE` 基础上，设计并实现 `LLEN` / `LINDEX` / `LREM` / `LTRIM` 的最小子集。
  - 补充端到端测试，并使用 `redis-cli` 做基本兼容性验证。（已通过 `tests/server_basic.rs` 中的列表扩展用例覆盖）

- [x] **集合命令核心子集（SADD/SMEMBERS/SISMEMBER/SCARD）**
  - 实现 `SADD` / `SMEMBERS` / `SISMEMBER` / `SCARD` 的第一批集合命令。
  - 保持语义与 Redis 对齐（例如重复元素时的行为和返回值）。（当前已在 `storage.rs` / `server.rs` 中实现，并由多条集合相关测试覆盖）

- [x] **命令参数校验与统一错误信息**
  - 梳理现有命令的参数校验逻辑，对齐 Redis 的错误风格（错误类型与错误消息）。
  - 为缺参、参数类型错误等场景补充系统化测试用例。

- [x] **RESP 解析边界与 DoS 防护雏形**
  - 为 RESP 解析增加长度限制、嵌套深度等基本防护能力。（当前已在 `resp.rs` 中通过 `MAX_BULK_STRING_SIZE` / `MAX_ARRAY_SIZE` 等常量实现长度限制）
  - 针对超长 bulk string、巨型数组等构造专门测试用例，避免简单 DoS 攻击路径。（相关错误路径已由 `resp::tests::parses_error_cases` 覆盖）

- [x] **实验性 Prometheus 指标导出**
  - 在独立 HTTP 端口上暴露 Prometheus 文本格式指标（实验性实现）。当前通过环境变量 `REDUST_METRICS_ADDR` 控制是否启用，在指定地址上提供 `/metrics` HTTP 端点（Prometheus 文本格式）。
  - 在文档中说明该功能的使用方式及当前限制。（后续可在 `README.md`/`roadmap.md` 中补充使用说明）

- [x] **持久化雏形与演进（AOF/RDB v1）**
  - 选型：优先实现 AOF 或简化版 RDB（二选一起步），定义内部数据结构序列化格式。
  - 功能：支持启动加载历史数据，以及定期或触发式持久化（后台执行，避免阻塞主处理循环）。
  - 语义：在文档中清晰说明持久性级别（例如接近 Redis `appendfsync everysec` 的语义），并标明当前限制。

---