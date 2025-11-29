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

- [ ] **基础性能基准测试（单机内网场景）**
  - 使用 `redis-benchmark` / 内置 bench，对 PING/SET/GET 做 QPS 与 P99 延迟测量。
  - 记录一版基线数据（本地开发机），在 `roadmap.md` 或 `future.md` 中留存。
  - 如发现明显瓶颈，再在 `future.md` 中登记相应优化条目。

---