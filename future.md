# Redust 中长期工作（future）

> 本文件用于记录 **不需要立刻开干，但希望未来实现或增强** 的事项。
> 与 `todo.md` 的区别：`todo.md` 更偏向“近期可执行的小任务”，而这里是“第二阶段/以后”的工作。

---

## 过期语义增强（V2）

目标：在现有过期语义 MVP 的基础上，逐步向更完整的 Redis 行为靠近。

- [ ] **高级 SET 选项支持**
  - `SET key value NX|XX [EX seconds|PX milliseconds|EXAT unix-time|PXAT ms-unix-time] [KEEPTTL] [GET]`
  - 需要明确：
    - 参数组合的合法/非法组合及返回错误信息。
    - 与现有 `EX` / `PX` 实现的兼容与迁移策略。
  - 补充测试：
    - NX/XX 在 key 存在/不存在两种情况下的行为。
    - EXAT/PXAT 与当前相对时间实现的边界行为（过期点就在“现在”附近时）。

- [ ] **主动过期采样策略调优**
  - 当前实现：
    - 固定间隔（例如 100ms）+ 固定样本数（例如 20 个 key）的简单采样。
  - 后续方向：
    - 优先采样有过期时间的 key，而不是所有 key。
    - 根据最近一次扫描的“过期命中率”粗略调整扫描频率/样本数。
    - 观察不同参数下对吞吐量和内存占用的影响（可以在 `INFO` 或日志中打印简单指标）。

- [ ] **过期语义边界与持久化交互**
  - 在将来引入持久化（AOF/RDB 子集）后，明确：
    - 加载时如何处理已经过期的 key（过滤/加载后立刻清理等）。
    - 过期时间在 AOF/RDB 中的编码方式（绝对时间 vs 相对时间）。
  - 预留：待持久化 PoC 成形后再细化具体方案与测试。

---

## 持久化雏形选型与 PoC

- [ ] **AOF 与简化版 RDB 权衡记录**
  - 对比写入放大、实现复杂度、崩溃恢复语义等维度，给出文字记录。
- [ ] **选出最小可行方案（MVP）**
  - 例如：仅实现 append-only 的 AOF 子集，或者仅实现周期性 snapshot 的 RDB 子集。
- [ ] **列出需要持久化的内部数据结构清单**
  - 包括：键空间结构（字符串/列表/集合等）、过期时间元数据、未来需要的统计信息（如 INFO 指标）。

---

## 持久化演进（基于 RDB v1 的后续方向）

> 现状：已经有 RDB v1 + 启动加载 + 基于 `REDUST_RDB_AUTO_SAVE_SECS` 的定时保存，仍有不少可以改进的空间。

- [ ] **RDB 保存的崩溃一致性与原子性**
  - 当前实现直接写入目标文件路径，未来可以考虑：
    - 先写入临时文件（如 `.rdb.tmp`），完成后再 `rename` 覆盖，避免生成半截快照；
    - 明确何时 `fsync`（例如：每次保存后，对文件和父目录各做一次 `fsync`）。
  - 在文档中补充：不同文件系统/平台下的一致性假设和风险提示。

- [ ] **RDB 保存的调度与限流**
  - 当前自动保存任务是简单的固定间隔循环：`sleep(interval) -> save_rdb`。
  - 后续可以：
    - 结合最近一轮 save 的耗时/失败次数，动态调整间隔（例如：过慢时拉长间隔）；
    - 在 save 过程中增加简单的分段写入/进度日志，便于诊断大实例下的抖动。

- [ ] **RDB 版本升级与兼容策略**
  - 在 `doc/rdb.md` 的基础上，设计 v2+ 的可能改动：
    - 增强 TTL 表示方式（例如改用绝对时间戳，以便跨进程更精确恢复）；
    - 支持更多数据类型（如未来的 ZSet、Stream 等）；
    - 预留多 DB 支持的字段（db 编号）。
  - 加载策略上明确：
    - 如何处理「版本号比当前实现新的」文件（拒绝/警告/尝试降级解析）；
    - 是否需要提供简单的“格式迁移”工具或命令。

- [ ] **AOF 方向的预研与 PoC**
  - 在 RDB v1 稳定后，可以启动一个最小 AOF PoC：
    - 仅记录关键写命令（SET/DEL/EXPIRE 等）的 append-only 日志；
    - 刷盘策略先对齐 Redis 常用的 `appendfsync everysec` 语义；
    - 通过简单 rewrite（基于当前内存快照重写 AOF）控制文件大小。
  - 目标是形成一份对比文档：RDB-only vs AOF-only vs RDB+AOF 的推荐部署策略。

- [ ] **可配置的持久化策略与关闭选项**
  - 通过环境变量或配置文件，允许用户：
    - 完全关闭定时保存（当前默认已是关闭，仅配置变量才启用）；
    - 为 RDB/AOF 分别配置保存间隔、文件路径等；
    - 在 `INFO` 或 metrics 中暴露当前持久化策略的摘要（例如 `persistence:rdb,interval=60s`）。

- [ ] **持久化与 INFO / metrics 的联动**
  - 在 `INFO` 中补充：
    - 上一次成功 RDB 保存的时间戳（类比 Redis `rdb_last_save_time`）；
    - 最近一次保存结果（成功/失败及错误消息简要统计）。
  - 在 Prometheus metrics 中暴露：
    - `redust_rdb_last_save_timestamp`、`redust_rdb_last_save_duration_seconds`；
    - `redust_rdb_save_failures_total` 等计数器。

---

## INFO 指标增强（V2）

在当前 INFO v1（redust_version/tcp_port/uptime_in_seconds/connected_clients/total_commands_processed/db0:keys）基础上，后续可以逐步增强：

- [ ] **更丰富的统计维度**
  - 每个命令的调用次数与平均耗时（类似 Redis 的 `cmdstat_*`）。
  - 键空间按类型统计：字符串/列表/集合等的数量分布。
  - 过期相关统计：总过期 key 数、当前带 TTL 的 key 数等。

- [ ] **更准确的内存信息**
  - `used_memory` / `used_memory_rss` 等指标：
    - 方案一：通过分配器（jemalloc/malloc）统计；
    - 方案二：粗略估算（按 value 长度累加等）。
  - 评估采集这些信息的性能影响，并在 INFO 输出中标注估算精度。

- [ ] **DBSIZE / used_memory 计数器增量维护**
  - 现状：
    - `dbsize` 通过遍历所有 key 并执行一次过期检查得到结果，复杂度为 O(N)。
    - `approximate_used_memory` 同样通过遍历并按字符串长度粗略估算内存占用，也是 O(N)，且在 `maybe_evict_for_write` 中可能被多次调用。
  - 未来方向：
    - 为 `dbsize` 和 `used_memory` 引入全局原子计数器（如 `AtomicUsize` / `AtomicU64`），在增/删/改 key 时按增量更新，让查询变成 O(1)。
    - 需要系统性梳理所有写路径（包括懒删除和定期过期任务、RDB 加载等），保证计数器与真实状态的一致性。

- [ ] **瞬时 QPS 与滑动窗口指标**
  - `instantaneous_ops_per_sec` 等：
    - 在 Metrics 中维护一个时间窗口，对最近 N 秒的命令数做近似统计。
    - 用于快速观察负载高峰，而不必依赖外部压测工具。

- [ ] **度量与日志集成**
  - 将部分指标暴露给日志或 metrics 系统（如 Prometheus exporter），而不仅仅通过 INFO 查看。
  - 在 `INFO` 中增加一个简单字段指示 metrics 导出状态（例如 `metrics_exporter:disabled|enabled`）。

> 注：V2 不必一次性完成，可以按“命令统计 → 内存统计 → QPS/窗口指标”的顺序逐步推进。

---

## 列表 / 集合命令增强（V2）

- [ ] **列表高级命令与阻塞语义预研**
  - 评估并规划 `LINSERT` / `LSET` / `BLPOP` / `BRPOP` 等命令的最小子集实现。
  - 思考阻塞列表操作在当前 Tokio 并发模型下的实现方式（例如：每 key 的等待队列 vs 全局调度）。

- [ ] **集合命令扩展与性能调优**
  - 在现有 `SADD` / `SMEMBERS` / `SISMEMBER` / `SCARD` / `SUNION` / `SINTER` / `SDIFF` 基础上，评估是否需要 `SPOP` / `SRANDMEMBER` / `SMOVE` / `*STORE` 等命令。
  - 对大集合场景下的 `SUNION` / `SINTER` / `SDIFF` 做性能 Profiling，记录潜在优化方向（如减少分配、选择更合适的数据结构）。

> 后续如果有新的“第二阶段”想法（例如 Hash/List/Stream 的高级特性），可以在本文件中按模块继续追加章节。例如：
>
> - `## Hash 模块增强（V2）`
> - `## 集群/复制相关设想`
> - 等等。
