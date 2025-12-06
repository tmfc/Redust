# Redust 中长期工作（future）

> 本文件用于记录 **不需要立刻开干，但希望未来实现或增强** 的事项。
> 与 `todo.md` 的区别：`todo.md` 更偏向"近期可执行的小任务"，而这里是"第二阶段/以后"的工作。

---

## 🎉 Phase B: 命令补全完成总结（2025-12）

### 已完成命令（25+ 个）

#### Hash 命令
- ✅ HSETNX - 字段不存在时设置
- ✅ HSTRLEN - 获取字段值长度
- ✅ HMSET - 批量设置字段

#### List 命令
- ✅ LTRIM - 裁剪列表
- ✅ LSET - 设置指定索引元素
- ✅ LINSERT - 在指定元素前/后插入
- ✅ RPOPLPUSH - 弹出并推入另一列表
- ✅ LPOS - 查找元素位置

#### Sorted Set 命令
- ✅ ZCOUNT - 统计分数范围内的成员数
- ✅ ZRANK / ZREVRANK - 获取成员排名
- ✅ ZPOPMIN / ZPOPMAX - 弹出最小/最大分数成员
- ✅ ZINTER / ZUNION / ZDIFF - 集合运算（支持 WEIGHTS/AGGREGATE/WITHSCORES）
- ✅ ZINTERSTORE / ZUNIONSTORE / ZDIFFSTORE - 集合运算并存储
- ✅ ZLEXCOUNT - 字典序范围计数

#### Generic 命令
- ✅ COPY - 复制键（支持 REPLACE 选项）
- ✅ UNLINK - 异步删除（简化实现）
- ✅ TOUCH - 更新访问时间
- ✅ OBJECT ENCODING - 获取对象编码类型

#### Expire 命令
- ✅ EXPIREAT - 设置绝对过期时间（Unix 秒）
- ✅ PEXPIREAT - 设置绝对过期时间（Unix 毫秒）
- ✅ EXPIRETIME - 获取绝对过期时间（Unix 秒）
- ✅ PEXPIRETIME - 获取绝对过期时间（Unix 毫秒）

### 测试覆盖
- 所有新增命令均有对应的集成测试
- 测试覆盖正常路径、边界条件、错误处理

---

## 🎉 Phase B: HyperLogLog 完成总结（2025-12）

### 已完成功能

- ✅ **HyperLogLog 核心算法**: 16384 个 6-bit 寄存器，标准误差约 0.81%
- ✅ **PFADD/PFCOUNT/PFMERGE**: 完整的 HyperLogLog 命令支持
- ✅ **RDB 持久化**: HyperLogLog 类型序列化/反序列化
- ✅ **WATCH 集成**: PFADD/PFMERGE 触发键版本更新
- ✅ **性能测试**: 大量元素添加、多键合并、内存占用验证

### 待优化方向

#### 稀疏表示优化（推荐优先实现）

Redis 的 HyperLogLog 使用两种表示方式来优化内存：

**当前问题**：每个 HLL 固定占用 16KB，即使只添加了 1 个元素。

**Redis 的稀疏表示方案**：
- 小基数时，直接存储 `(寄存器索引, 计数值)` 对
- 使用变长编码压缩存储：
  - `ZERO:len` - 连续 len 个零值寄存器
  - `XZERO:len` - 连续 len 个零值（扩展，支持更长）
  - `VAL:value,len` - 连续 len 个相同 value 的寄存器
- 当稀疏表示超过 ~3000 字节时，自动转换为密集表示

**简化实现方案**（推荐）：
```rust
enum HllRepr {
    // 稀疏：存储非零的 (index, value) 对，按 index 排序
    // 内存占用：entries.len() * 3 bytes (u16 index + u8 value)
    Sparse { entries: Vec<(u16, u8)> },
    
    // 密集：完整的 16384 个寄存器
    // 内存占用：16384 bytes
    Dense { registers: Vec<u8> },
}
```

**转换阈值**：当 `entries.len() * 3 > 3000` 时转为密集表示（约 1000 个非零寄存器）

**预期收益**：
- 1 个元素：~3 bytes vs 16KB（节省 99.98%）
- 100 个元素：~300 bytes vs 16KB（节省 98%）
- 1000+ 个元素：自动转为密集表示

#### 其他优化

- **AOF 支持**: PFADD/PFMERGE 命令记录到 AOF
- **紧凑存储**: 使用 6-bit 紧凑存储替代 u8，将内存从 16KB 降至 12KB（密集表示）

---

## 🎉 Phase A 完成总结（2025-12）

### 已完成功能

#### 核心数据结构（5 种）
- ✅ **String**: 完整的字符串操作（SET/GET/INCR/APPEND 等 20+ 命令）
- ✅ **List**: 双端队列操作（LPUSH/RPUSH/LPOP/RPOP/LRANGE 等）
- ✅ **Set**: 集合操作（SADD/SREM/SUNION/SINTER/SDIFF 等）
- ✅ **Hash**: 哈希表操作（HSET/HGET/HINCRBY/HGETALL 等）
- ✅ **Sorted Set**: 有序集合（ZADD/ZRANGE/ZSCORE/ZINCRBY/ZSCAN 等）

#### 高级特性
- ✅ **事务**: MULTI/EXEC/DISCARD/WATCH/UNWATCH，支持乐观锁
- ✅ **Lua 脚本**: EVAL/EVALSHA/SCRIPT 命令，redis.call/pcall 支持 46 个命令
  - 二进制安全参数处理
  - Nil 正确映射为 false
  - SHA1 脚本缓存
- ✅ **持久化**: AOF（everysec）+ RDB 快照，支持 SAVE/BGSAVE/LASTSAVE
- ✅ **Pub/Sub**: Channel/Pattern/Shard 三种订阅模式
- ✅ **扫描**: SCAN/SSCAN/HSCAN/ZSCAN 游标扫描
- ✅ **运维命令**: CONFIG GET/SET、CLIENT 管理、SLOWLOG 基础

#### 质量保证
- ✅ **测试覆盖**: 99 个测试全部通过
- ✅ **命令总数**: 120+ 个 Redis 命令
- ✅ **文档完善**: command.md、roadmap.md、future.md 全面更新

### 技术亮点
1. **完整的 WATCH 机制**: Key 版本追踪覆盖所有写操作和过期/淘汰
2. **二进制安全**: Lua 脚本参数和值保持原始字节，支持非 UTF-8 数据
3. **Redis 语义对齐**: Nil 映射为 false，错误处理与 Redis 一致
4. **异步持久化**: AOF 异步写入，RDB 后台保存，不阻塞主线程
5. **内存管理**: LRU 淘汰策略，maxmemory 限制，过期键自动清理

---

## 过期语义增强（V2）

目标：在现有过期语义 MVP 的基础上，逐步向更完整的 Redis 行为靠近。

- [x] **高级 SET 选项支持** ✅ 已完成
  - `SET key value NX|XX [EX seconds|PX milliseconds|EXAT unix-time|PXAT ms-unix-time] [KEEPTTL] [GET]`
  - 已实现所有选项组合和错误处理
  - 完整的测试覆盖

- [ ] **主动过期采样策略调优**
  - 当前实现：
    - 固定间隔（例如 100ms）+ 固定样本数（例如 20 个 key）的简单采样。
  - 后续方向：
    - 优先采样有过期时间的 key，而不是所有 key。
    - 根据最近一次扫描的“过期命中率”粗略调整扫描频率/样本数。
    - 观察不同参数下对吞吐量和内存占用的影响（可以在 `INFO` 或日志中打印简单指标）。

- [x] **过期语义边界与持久化交互** ✅ 基础已完成
  - 已实现 AOF/RDB 持久化
  - 启动时自动加载并处理过期键
  - 🔄 待完善：损坏文件校验与友好降级

---

## 内存与淘汰策略演进（V2）

- [ ] **策略可配置化**
  - 通过环境变量或配置项暴露：`maxmemory_policy=allkeys-lru|noeviction|allkeys-random`（先支持少量策略）。
  - 允许调整采样大小（如 `REDUST_LRU_SAMPLE_SIZE`），用于控制 LRU 近似度与开销。

- [ ] **采样与触发调优**
  - 当前淘汰在写路径上循环计算 `approximate_used_memory` + 采样淘汰 1 个键；未来可改为批量淘汰 n 个，减少循环次数。
  - 将 `approximate_used_memory` O(N) 开销替换为增量计数（参见 INFO 演进节），降低写放大。

- [ ] **指标与可观测性**
  - 在 INFO 与 Prometheus metrics 中暴露：`maxmemory`, `used_memory`, `evictions_total`, `lru_sample_size` 等。
  - 记录最近一次淘汰用时与触发原因（写入超限/显式触发）。

- [ ] **测试与稳定性**
  - 增加确定性测试：固定随机种子与数据规模，验证超限后 `DBSIZE < 总写入` 且不发生崩溃。
  - 在 CI 中增加不同 `REDUST_MAXMEMORY_BYTES` 与采样大小组合的冒烟测试。

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

## INFO 指标与多 DB 演进（V2）

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

- [ ] **多 DB / SELECT 雏形的演进**
  - 现状：
    - 当前多 DB 是通过在 key 前增加 `"<db>:"` 前缀实现的逻辑隔离（例如：DB0 的 `foo` 物理存为 `"0:foo"`）。
    - `DBSIZE` 与 `INFO` 的 `# Keyspace` 统计，依赖 `keys("*")` 结果 + 前缀解析来区分不同 DB。
  - 未来方向：
    - 将内部存储演进为真正的多 DB 结构，例如 `Vec<DashMap<String, StorageValue>>`，使得 DB 之间在类型层面隔离更清晰。
    - 与上面的 DBSIZE/used_memory 计数器一起重构：为每个 DB 维护独立的 key 数/内存占用统计，使 `INFO`/`DBSIZE` 查询在多 DB 场景下也保持 O(1)。
    - 为后续可能的多 DB 持久化（RDB/AOF 中记录 DB 编号）预留空间，避免前缀方案在格式层面造成额外兼容负担。

- [ ] **瞬时 QPS 与滑动窗口指标**
  - `instantaneous_ops_per_sec` 等：
    - 在 Metrics 中维护一个时间窗口，对最近 N 秒的命令数做近似统计。
    - 用于快速观察负载高峰，而不必依赖外部压测工具。

- [ ] **度量与日志集成**
  - 将部分指标暴露给日志或 metrics 系统（如 Prometheus exporter），而不仅仅通过 INFO 查看。
  - 在 `INFO` 中增加一个简单字段指示 metrics 导出状态（例如 `metrics_exporter:disabled|enabled`）。

> 注：V2 不必一次性完成，可以按“命令统计 → 内存统计 → QPS/窗口指标”的顺序逐步推进。

---

## 键扫描与模式匹配增强（V2）

> 现状：`SCAN`/`KEYS`/`SSCAN`/`HSCAN`/`ZSCAN` 已实现基础功能，`pattern_match` 支持 `*`、`?`、`[abc]`、`[a-z]`、`\` 转义等 glob 语法。

- [x] **`[^abc]` 取反字符集支持** ✅ 已完成（2025-12）
  - Redis 支持 `[^abc]` 表示"不匹配 a/b/c 中任一字符"。
  - 已在 `match_set` 中增加对 `^` 前缀的处理，支持取反字符集和取反范围。

- [x] **SCAN TYPE 选项** ✅ 已完成（2025-12）
  - Redis 6.0+ 支持 `SCAN cursor TYPE string|list|set|hash|zset` 按类型过滤。
  - 已在 `Command::Scan` 中增加 `type_filter` 字段，扫描时调用 `storage.type_of()` 过滤。

- [x] **SCAN NOVALUES 选项（HSCAN/ZSCAN）** ✅ 已完成（2025-12）
  - Redis 7.4+ 支持 `HSCAN key cursor NOVALUES` 仅返回 field 不返回 value，减少网络开销。
  - 已实现 HSCAN 和 ZSCAN 的 NOVALUES 选项。

- [x] **SCAN 游标稳定性优化** ✅ 已完成（2025-12）
  - 使用键名哈希值作为游标，替代简单的数组索引。
  - 游标基于 `DefaultHasher` 计算的 u64 哈希值，按哈希值排序后扫描。
  - 优点：并发写入/删除时不会产生重复键（已扫描的哈希值不会再次返回）。
  - 限制：新增键如果哈希值小于当前游标可能被跳过（与 Redis 行为一致）。
  - 支持完整的 u64 游标范围，解析时使用 `parse_u64_from_bulk`。

---

## 事务与脚本增强（V2）

> 现状：`MULTI`/`EXEC`/`DISCARD`/`WATCH`/`UNWATCH` 已实现基础语义，支持命令队列和乐观锁。
> Lua 脚本基础功能已实现（`EVAL`/`EVALSHA`/`SCRIPT LOAD|EXISTS|FLUSH`）。

- [x] **EVAL/EVALSHA Lua 脚本支持（基础版）**
  - 已引入 `mlua` crate（Lua 5.4），支持基础脚本执行。
  - 已实现 `EVAL script numkeys [key ...] [arg ...]` 和 `EVALSHA sha1 numkeys [key ...] [arg ...]`。
  - 已实现 `SCRIPT LOAD` / `SCRIPT EXISTS` / `SCRIPT FLUSH` 脚本管理命令。
  - 已支持 `KEYS` 和 `ARGV` 表访问。
  - 已支持返回值类型转换（integer, string, array, nil, boolean）。

- [x] **redis.call() / redis.pcall() 回调实现**
  - 已实现 `redis.call()` 和 `redis.pcall()` 在 Lua 脚本中调用 Redis 命令。
  - 支持 40+ 常用命令：GET/SET/DEL/EXISTS/INCR/DECR/INCRBY/DECRBY/APPEND/STRLEN/MGET/MSET、
    HGET/HSET/HDEL/HEXISTS/HGETALL/HKEYS/HVALS/HLEN/HMGET/HMSET/HINCRBY、
    LPUSH/RPUSH/LPOP/RPOP/LLEN/LRANGE/LINDEX、SADD/SREM/SMEMBERS/SISMEMBER/SCARD、
    ZADD/ZREM/ZSCORE/ZCARD/ZRANGE/ZREVRANGE、TYPE/TTL/PTTL/EXPIRE/PEXPIRE/PERSIST。
  - `redis.call()` 在错误时抛出 Lua 异常，`redis.pcall()` 返回 `{err = "..."}` 表。

- [x] **事务中 Lua 脚本支持** ✅ 已完成（2025-12）
  - 支持 `EVAL`/`EVALSHA`/`SCRIPT LOAD`/`SCRIPT EXISTS`/`SCRIPT FLUSH` 在 MULTI 中执行。
  - 脚本在事务中正常执行，结果作为事务响应数组的一部分返回。

- [x] **事务中更多命令支持** ✅ 已完成（2025-12）
  - 已支持 `TYPE`、`KEYS`、`SCAN`、`DBSIZE` 等元命令在事务中执行。

- [ ] **事务错误处理增强**
  - Redis 在 EXEC 时如果队列中有语法错误命令，会中止整个事务。
  - 当前实现在命令入队时已做语法检查，但可进一步对齐 Redis 行为。

- [x] **WATCH 版本追踪优化**
  - 已在所有写路径中调用 `bump_key_version`，包括 `LPUSH`、`SADD`、`HSET`、`ZADD` 等。
  - 已在 TTL 过期删除和 LRU 淘汰时更新 key 版本。

---

## 运维命令（V2）

- [x] **基础运维命令实现**
  - 已实现 `CONFIG GET pattern` - 获取匹配的配置参数（支持 * 通配符）。
  - 已实现 `CONFIG SET parameter value` - 设置配置参数（当前大多数参数不可动态修改，返回错误）。
  - 已实现 `CLIENT LIST` - 列出当前客户端连接信息（简化版）。
  - 已实现 `CLIENT ID` - 获取当前连接的唯一 ID。
  - 已实现 `CLIENT SETNAME name` - 设置连接名称。
  - 已实现 `CLIENT GETNAME` - 获取连接名称。
  - 已实现 `SLOWLOG GET [count]` - 获取慢日志（当前返回空数组）。
  - 已实现 `SLOWLOG RESET` - 重置慢日志。
  - 已实现 `SLOWLOG LEN` - 获取慢日志长度（当前返回 0）。

- [x] **CONFIG 动态配置支持** ✅（部分完成）
  - 已支持运行时修改：`maxmemory`、`slowlog-log-slower-than`、`slowlog-max-len`、`timeout`、`tcp-keepalive`（timeout/keepalive 当前仅存值，尚未作用于连接行为）。
  - 待办：
    - 使 `timeout`/`tcp-keepalive` 实际生效：在连接处理层应用超时和 keepalive 参数。
    - 考虑配置持久化（写入配置文件或环境变量）。
    - 评估是否开放更多动态参数（如 maxmemory-policy）。

- [x] **SLOWLOG 实际实现** ✅ 已完成（2025-12）
  - 已实现完整的慢日志功能：记录超过阈值的命令、维护固定大小队列。
  - 支持 SLOWLOG GET/RESET/LEN 完整语义。
  - 环境变量配置：REDUST_SLOWLOG_SLOWER_THAN（微秒）、REDUST_SLOWLOG_MAX_LEN。

- [x] **CLIENT 命令扩展**（部分完成）
  - 已支持：LIST/ID/SETNAME/GETNAME/PAUSE/UNPAUSE。
  - 待实现：
    - `CLIENT UNBLOCK client-id [TIMEOUT|ERROR]` - 解除阻塞的客户端（需要阻塞命令支持，如 BLPOP）。
    - `CLIENT KILL [ID client-id] [ADDR ip:port] [TYPE normal|master|slave|pubsub]` - 关闭指定客户端连接。
    - `CLIENT REPLY ON|OFF|SKIP` - 控制响应行为。
    - `CLIENT NO-EVICT ON|OFF` - 标记客户端不被驱逐。
    - `CLIENT CACHING YES|NO` - 客户端缓存控制。
    - `CLIENT TRACKINGINFO` - 获取客户端追踪信息。

- [ ] **INFO 命令实现**
  - 实现 `INFO [section]` 命令，返回服务器状态信息。
  - 支持的 section：server, clients, memory, persistence, stats, replication, cpu, commandstats, cluster, keyspace。
  - 与现有 Metrics 结构体集成。

- [ ] **Prometheus metrics 导出**
  - 提供 HTTP 端点导出 Prometheus 格式的指标。
  - 包括：连接数、命令数、内存使用、键空间统计等。

---

## 列表 / 集合命令增强（V2）

- [x] **列表高级命令** ✅ 已完成
  - 已实现 `LINSERT` / `LSET` / `LTRIM` / `RPOPLPUSH` / `LPOS`
  - 🔄 待实现：`BLPOP` / `BRPOP` 等阻塞语义命令
  - 思考阻塞列表操作在当前 Tokio 并发模型下的实现方式（例如：每 key 的等待队列 vs 全局调度）。

- [x] **集合命令扩展与性能调优** ✅ 已完成（2025-12）
  - 已实现 `SPOP` / `SRANDMEMBER` / `SMOVE` / `SUNIONSTORE` / `SINTERSTORE` / `SDIFFSTORE` 等命令。
  - 性能优化：
    - 移除 `SUNION`/`SINTER`/`SDIFF` 返回结果的不必要排序（Redis 不保证顺序）。
    - `SUNION`/`SUNIONSTORE` 使用 `extend` 替代循环插入。
    - `SDIFF`/`SDIFFSTORE` 使用 `retain` 替代遍历后续集合，并添加早期退出优化。
  - 基准测试提升：`set_union` +17%，`set_difference` +8%。

- [x] **Sorted Set 高级命令** ✅ 已完成
  - 已实现 `ZCOUNT` / `ZRANK` / `ZREVRANK` / `ZPOPMIN` / `ZPOPMAX`
  - 已实现 `ZINTER` / `ZUNION` / `ZDIFF` 及其 STORE 变体（支持 WEIGHTS/AGGREGATE）
  - 已实现 `ZLEXCOUNT` 字典序范围计数

> 后续如果有新的“第二阶段”想法（例如 Hash/List/Stream 的高级特性），可以在本文件中按模块继续追加章节。例如：
>
> - `## Hash 模块增强（V2）`
> - `## 集群/复制相关设想`
> - 等等。

---

## 安全与多租户（V2）

- [ ] **基于 AUTH 的多用户/多租户模型设计**
  - 在当前全局密码的基础上，调研 Redis ACL 语义及常见多租户隔离需求（如按 DB、按前缀做逻辑隔离）。
  - 评估是继续基于单密码扩展，还是引入简单用户表（用户名 + 密码 + 权限掩码）。

- [ ] **按命令/按 DB 的权限控制雏形**
  - 以命令白名单/黑名单的方式，为不同用户配置可执行的命令子集（至少覆盖读/写/管理类命令）。
  - 探索按 DB 维度做简单隔离：不同租户映射到不同逻辑 DB，或在 key 前缀中编码租户 ID，并在权限检查时强制匹配。

- [ ] **兼容与配置策略**
  - 保持与现有 `REDUST_AUTH_PASSWORD` 行为的向后兼容（例如：未配置高级 ACL 时仍按单密码模型工作）。
  - 通过环境变量或配置文件暴露最小可用的安全配置集，并在 README/INFO 中简要描述当前安全特性边界。

- [ ] **Pub/Sub 级 ACL / 权限粒度**
  - 现状：仅全局密码开关；未提供按 channel / pattern 的 Pub/Sub 权限控制。
  - 目标：设计/实现基础 ACL 钩子或 per-channel 权限检查（与 AUTH 兼容），用于限制特定客户端对敏感频道的订阅/发布能力。

---

## Pub/Sub 在集群/复制场景下的语义（V2）

- [ ] **Pub/Sub 与集群/复制场景交互语义**
  - 现状：仅面向单机实例的 Pub/Sub，未考虑 Cluster 或主从复制下的消息传播和路由策略。
  - 目标：规划在 cluster/复制场景中 Pub/Sub 的行为边界（仅本节点广播、按分片路由、还是全集群广播等），以设计文档/PoC 的形式记录，不纳入当前阶段实现。

## 客户端兼容性与生态集成（V2）

- [ ] **redis-cli / go-redis 端到端验证扩展**
  - 基于当前 redis-rs 测试经验，补充 `redis-cli` 脚本和 Go `go-redis` 小程序，覆盖 string/list/set/hash/expire 等常用命令。
  - 在 CI 或本地脚本中统一执行这些兼容性测试，并记录所有与官方 Redis 行为差异的案例。

- [ ] **不兼容行为登记与跟踪**
  - 为每一条不兼容行为记录：使用的客户端/版本、触发命令及参数、Redis 实际返回 vs Redust 返回。
  - 将这些差异条目集中登记在本文件或独立文档中，并在 PR/issue 中引用，作为后续修复/取舍决策的依据。

- [ ] **生态特性与 Redust 范围边界说明**
  - 明确目前不打算支持的 Redis 高级特性（如事务、Cluster、Lua 等），在 README 中用一小节列出“兼容性范围说明”。
  - 对已经验证兼容的客户端组合给出一个简短列表（例如：redis-cli x.y、redis-rs 0.25、go-redis vX 等）。
