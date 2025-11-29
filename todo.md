# Redust 项目待办事项

本文件用于跟踪当前“可以立刻开干”的小类工作，同时保留长期问题列表。中长期路线请参考 `roadmap.md`。

---

## 当前聚焦的小类工作

- [x] **过期语义预研与设计草案（MVP 完成）**
  - 基于当前 `Storage` 结构，思考如何存储过期时间（如额外 map 或封装 value）。
  - 对比惰性删除 + 定期采样删除两种策略，给出适合 Redust 的方案草图。
  - （已初步实现）基础懒删除 + 定期采样删除 + EXPIRE/PEXPIRE/TTL/PTTL/PERSIST 命令与测试。

- [x] **Storage 锁粒度与并发方案设计**
  - 评估当前全局 `RwLock` 带来的瓶颈场景（例如大量并发写入/读写混合）。
  - 对比几种可选方案（分片锁、并发 map 等），输出一份设计建议。
  - 结论：当前采用单个 `DashMap<String, StorageValue>`，依赖其内部分片锁作为主要并发策略；不额外增加全局 `RwLock` 或手写多级分片，未来如遇瓶颈再在 `future.md` 中演进。

- [x] **INFO 子集与监控指标草稿**
  - 已实现 INFO v1：redust_version、tcp_port、uptime_in_seconds、connected_clients、total_commands_processed、db0:keys 等基础指标。
  - 进一步的指标与监控增强（per-command 统计、更精确内存、QPS 等）已记录在 `future.md` 的「INFO 指标增强（V2）」中。

---