# Redust 中长期工作（future）

> 本文件用于记录 **不需要立刻开干，但希望未来实现或增强** 的事项。
> 与 `todo.md` 的区别：`todo.md` 更偏向"近期可执行的小任务"，而这里是"第二阶段/以后"的工作。

---

## HyperLogLog 待优化方向

### 稀疏表示优化（推荐优先实现）

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

### 其他优化

- **AOF 支持**: PFADD/PFMERGE 命令记录到 AOF
- **紧凑存储**: 使用 6-bit 紧凑存储替代 u8，将内存从 16KB 降至 12KB（密集表示）

## 过期语义增强（V2）

目标：在现有过期语义 MVP 的基础上，逐步向更完整的 Redis 行为靠近。

- [ ] **主动过期采样策略调优**
  - 当前实现：
    - 固定间隔（例如 100ms）+ 固定样本数（例如 20 个 key）的简单采样。
  - 后续方向：
    - 优先采样有过期时间的 key，而不是所有 key。
    - 根据最近一次扫描的“过期命中率”粗略调整扫描频率/样本数。
    - 观察不同参数下对吞吐量和内存占用的影响（可以在 `INFO` 或日志中打印简单指标）。

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

---

## 事务与脚本增强（V2）

> 现状：`MULTI`/`EXEC`/`DISCARD`/`WATCH`/`UNWATCH` 已实现基础语义，支持命令队列和乐观锁。
> Lua 脚本基础功能已实现（`EVAL`/`EVALSHA`/`SCRIPT LOAD|EXISTS|FLUSH`）。

- [ ] **事务错误处理增强**
  - Redis 在 EXEC 时如果队列中有语法错误命令，会中止整个事务。
  - 当前实现在命令入队时已做语法检查，但可进一步对齐 Redis 行为。

---

## 运维命令（V2）

- [ ] **timeout / tcp-keepalive 真正生效**
  - 现状：配置值可改但未作用于连接行为。
  - 目标：在连接处理层应用超时与 keepalive。

- [ ] **SCRIPT KILL**
  - 允许终止正在运行的脚本。

- [ ] **RDB 校验与自动备份/部分恢复策略**
  - 添加校验和、损坏文件自动备份、尽量恢复可读数据。

- [ ] **错误信息审计与错误码**
  - 审查所有错误信息，避免泄露内部细节；考虑错误码体系。

- [ ] **Prometheus 指标增强**
  - 命令延迟 histogram、按命令分类计数器、过期删除计数器等。

- [ ] **CLIENT 命令扩展**
  - `CLIENT UNBLOCK/KILL/REPLY/NO-EVICT/CACHING/TRACKINGINFO`。

- [ ] **INFO 命令实现**
  - 实现 `INFO [section]` 命令，返回服务器状态信息。
  - 支持的 section：server, clients, memory, persistence, stats, replication, cpu, commandstats, cluster, keyspace。
  - 与现有 Metrics 结构体集成。

- [ ] **Prometheus metrics 导出**
  - 提供 HTTP 端点导出 Prometheus 格式的指标。
  - 包括：连接数、命令数、内存使用、键空间统计等。

---

## 列表 / 集合命令增强（V2）
> 后续如果有新的“第二阶段”想法（例如 Hash/List/Stream 的高级特性），可以在本文件中按模块继续追加章节。例如：
>
> - `## Hash 模块增强（V2）`
> - `## 集群/复制相关设想`
> - 等等。

---

## Streams 模块增强（V2）

现状：已实现 Streams 完整子集：`XADD`/`XRANGE`/`XREVRANGE`/`XLEN`/`XREAD`/`XREADGROUP`/`XGROUP`/`XINFO`/`XACK`/`XPENDING`/`XCLAIM`。

- [ ] **XREAD 改进**
  - 使用 `Notify`/条件变量替代轮询，降低空转 CPU 并提升唤醒精度。
  - 补齐/对齐更多错误消息细节。
  - Redis 7.4 的 `+` 特殊 ID 语义（请求最后一条，且忽略 COUNT）可作为可选增强。

- [x] **XGROUP / 消费者组**
  - 已实现：`XGROUP CREATE`/`SETID`/`DESTROY`/`CREATECONSUMER`/`DELCONSUMER`。
  - 已实现：`XREADGROUP`、`XACK`、`XPENDING`、`XCLAIM`。

- [x] **XINFO / introspection**
  - 已实现 `XINFO STREAM` 输出基础字段。

- [x] **XRANGE/XREVRANGE 完整语义**
  - 已实现 `XRANGE` 和 `XREVRANGE`，支持 `-`/`+` 与 `COUNT`。

- [ ] **XAUTOCLAIM**
  - 自动转移长时间未确认的消息。

- [ ] **XDEL / XTRIM**
  - `XDEL` 删除 entry。
  - `XTRIM` 支持 MAXLEN/MINID/approximate（~）。

- [ ] **RDB/AOF 持久化**
  - 当前实现中 Streams 暂未写入 RDB（save 时跳过）。
  - 后续需要定义序列化格式：
    - stream length
    - last_id
    - entries: [id(ms,seq), field/value pairs]
    - （有消费者组时还需 group/PEL 等元数据）
  - 同时需要补 load_rdb 的解析与回放。

- [ ] **内存与性能优化**
  - entry 索引：目前线性 Vec 扫描；后续可引入二分查找（按 ID 有序）或跳表。
  - 限制单个 stream 的 entry 数/单条 entry 字段数/字段 value bytes（防止大 key）。

---

## Geo 模块增强（V2）

现状：已实现 Geo 完整子集：`GEOADD`/`GEOPOS`/`GEODIST`/`GEOHASH`/`GEOSEARCH`（基于 ZSet + 52-bit geohash 编码）。

- [x] **GEOSEARCH**
  - 已实现基于半径或矩形范围的地理位置搜索。
  - 支持 `BYRADIUS`/`BYBOX`、`ASC`/`DESC` 排序、`COUNT` 限制。
  - 支持 `WITHCOORD`/`WITHDIST`/`WITHHASH` 输出选项。

- [ ] **GEOSEARCHSTORE**
  - 将搜索结果存储到新的 ZSet 中。

- [ ] **GEORADIUS / GEORADIUSBYMEMBER**
  - 旧版命令，已被 GEOSEARCH 取代，可选实现。

- [ ] **精度与兼容性**
  - 当前 geohash 编码与 Redis 略有差异，后续可对齐 Redis 的精确编码算法。
  - 距离计算使用 haversine 公式，与 Redis 一致。

---

## 主从复制（V2）

现状：尚未实现主从复制功能。

- [ ] **REPLICAOF/SLAVEOF 命令**
  - 实现从库连接主库的命令。
  - 支持 `REPLICAOF NO ONE` 断开复制。

- [ ] **全量同步（RDB 传输）**
  - 从库首次连接时，主库发送 RDB 快照。
  - 从库加载 RDB 并开始接收增量命令。

- [ ] **增量命令流同步**
  - 主库将写命令实时传播到从库。
  - 实现复制积压缓冲区（replication backlog）。

- [ ] **只读从库模式**
  - 从库默认拒绝写命令。
  - 支持 `replica-read-only` 配置。

- [ ] **INFO replication section**
  - 在 INFO 命令中添加复制状态信息。
  - 包括：role、connected_slaves、master_link_status 等。

---

## 安全与多租户（V2）

- [x] **ACL 基础实现**
  - 已实现 ACL 用户管理：`ACL SETUSER/DELUSER/LIST/USERS/WHOAMI/GETUSER/CAT`
  - 已实现命令权限控制：`+/-命令`、`+/-@类别`
  - 已实现 Key 权限控制：`~pattern`
  - 已实现频道权限控制：`&pattern`
  - 支持密码哈希（SHA256）和多密码

- [ ] **ACL 增强**
  - 全局共享 ACL 管理器（当前每个连接独立）
  - AUTH 命令与 ACL 集成（支持 `AUTH username password`）
  - 命令执行前的权限检查钩子
  - ACL SAVE/LOAD 持久化用户配置

- [ ] **按命令/按 DB 的权限控制增强**
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

- [ ] **redis-rs 兼容性测试扩展点**
  - 错误类型覆盖：`-ERR`/`-WRONGTYPE`/`-NOSCRIPT`/`-EXECABORT` 等在 redis-rs 侧的解码与错误信息一致性。
  - Null array 覆盖：`BLPOP` timeout、`XREAD` 空读、`EXEC` aborted（nil）等返回形态。
  - 二进制安全更多场景：key/value 含 `\0`、非 UTF-8 的 hash field、list/set 元素等 roundtrip。
  - 阻塞命令与取消：`BLPOP/BRPOP` 的超时与取消（配合 tokio timeout），以及连接关闭后的资源回收。
  - Pub/Sub 模式：订阅后普通命令的行为对齐（是否返回错误/是否允许），以及 `SUBSCRIBE/UNSUBSCRIBE` 返回帧结构断言。

- [ ] **go-redis 回归测试最小落地（建议）**
  - 现状：仓库当前没有 `go.mod` / `*.go`，因此无法直接在现有测试体系里运行 go-redis 客户端回归。
  - 建议落地方式（最小闭环）：
    - 在仓库新增独立目录（例如 `client-tests/go-redis/`），包含：
      - `go.mod`（module 名可用 `redust-client-tests`）
      - `main_test.go`（使用 `github.com/redis/go-redis/v9`）
      - 测试逻辑通过环境变量读取地址（例如 `REDUST_ADDR=127.0.0.1:6379`），避免测试内启动 Rust server。
    - 先覆盖最小用例：
      - 基础：`PING`/`SET`/`GET`/`INCR`
      - Pipeline：`Pipelined` 与 `TxPipeline`（只验证返回类型/顺序/错误不 panic）
      - 超时/取消：针对 `BLPOP` 等阻塞命令用 `context.WithTimeout`
    - CI 策略：
      - 分两步：先启动 `redust`（后台进程/独立 job），再运行 `go test ./...`。
      - 若 CI 不方便引入 Go，可先提供本地脚本（Makefile/justfile）用于开发者手动回归。

  - 运行注意事项（本地/CI）：
    - 需要在 `client-tests/go-redis/` 目录执行 `go test ./...`（该目录包含 `go.mod`）。
    - 需要先启动 Redust 并设置地址，例如：`cargo run --bin redust -- --bind 127.0.0.1:6380`。
    - 测试通过 `REDUST_ADDR=127.0.0.1:6380` 连接外部 Redust，不在 Go 测试内启动 Rust server。
    - 首次执行如遇到网络问题，可设置 `GOPROXY=direct` 或使用可用代理（或配置 `http_proxy/https_proxy`）后再跑 `go mod tidy` / `go test`。

- [ ] **不兼容行为登记与跟踪**
  - 为每一条不兼容行为记录：使用的客户端/版本、触发命令及参数、Redis 实际返回 vs Redust 返回。
  - 将这些差异条目集中登记在本文件或独立文档中，并在 PR/issue 中引用，作为后续修复/取舍决策的依据。

- [ ] **生态特性与 Redust 范围边界说明**
  - 明确目前不打算支持的 Redis 高级特性（如事务、Cluster、Lua 等），在 README 中用一小节列出“兼容性范围说明”。
  - 对已经验证兼容的客户端组合给出一个简短列表（例如：redis-cli x.y、redis-rs 0.25、go-redis vX 等）。
