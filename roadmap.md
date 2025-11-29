## 走向生产可用的路线图（9 大类能力 / 工作包总览）

> 这一节从“要让 Redust 成为**单机、生产可用的 Redis 替代**”的角度，梳理能力大类与工作包。可直接映射为中长期里程碑，具体实现节奏可按阶段 2/3/4 细化。

### 一、功能维度：命令 & 数据结构

- **1. 核心功能补齐（高优先级）**
  - Key 过期与淘汰：
    - 实现 `EXPIRE` / `PEXPIRE` / `TTL` / `PTTL` / `PERSIST` 等最小语义。
    - 设计过期机制（惰性删除 + 定期采样删除），避免全表扫描。
    - 预留内存上限与淘汰策略（如 `maxmemory` + 一种 LRU 策略）。
    - 当前已完成：基础过期语义 MVP（支持 EXPIRE/PEXPIRE/TTL/PTTL/PERSIST，采用懒删除 + 定期采样删除）。
  - 字符串增强：
    - 补齐常用多 key/原子操作：`MGET` / `MSET` / `SETNX` / `SETEX` / `PSETEX` / `INCRBY` / `DECRBY` 等。
    - 评估并明确二进制安全策略（当前以 `String` 为主）。
- **2. 数据结构扩展**
  - 哈希（Hash）：
    - 设计存储结构并实现 `HSET` / `HGET` / `HDEL` / `HEXISTS` / `HGETALL` / `HINCRBY` 等子集。
    - 为常见业务场景（配置、用户画像）提供足够功能覆盖。
  - 有序集合（Sorted Set）：
    - 设计最小实现（如跳表/平衡树），支持 `ZADD` / `ZREM` / `ZRANGE` / `ZREVRANGE` / `ZCARD` / `ZINCRBY` 等。
    - 主要覆盖排行榜、计分场景。
  - 列表 & 集合补完：
    - 列表：在现有 `LPUSH` / `RPUSH` / `LPOP` / `RPOP` / `LRANGE` 基础上补齐 `LLEN` / `LINDEX` / `LREM` / `LTRIM` 等。
    - 集合：围绕 `SADD` / `SREM` / `SMEMBERS` / `SCARD` / `SISMEMBER` / `SUNION` / `SINTER` / `SDIFF`，视需求增加 `SPOP` / `SRANDMEMBER` / `SMOVE` / `*STORE` 命令。

### 二、持久化与数据安全

- **3. 持久化雏形与演进（高优先级）**
  - 选型：优先实现 AOF 或简化版 RDB（二选一起步），定义内部数据结构序列化格式。
  - 功能：
    - 支持启动加载历史数据，保证基本崩溃恢复能力。
    - 支持定期或触发式持久化（后台执行，避免阻塞主处理循环）。
  - 语义：
    - 在文档中清晰说明持久性级别（接近 Redis `appendfsync everysec` 的语义，或其它约定）。

### 三、可靠性 & 高可用

- **4. 单机稳定性（高优先级）**
  - 健壮错误处理：
    - 命令参数严格校验，统一返回 Redis 风格错误信息。
    - RESP 解析的边界情况、防御性编程与 DoS 防护（长度限制等）。
  - 压力行为：
    - 支持 pipeline、大 key/大集合 场景的基准测试与回归。
    - 避免在 Tokio runtime 上进行长时间阻塞操作（如持久化、全量扫描）。
- **5. 复制与只读从库（中–高优先级）**
  - 主从复制的最小实现：
    - 全量同步：主节点生成快照并传输给从节点。
    - 命令流复制：写命令在主节点应用的同时发送到从节点。
  - 从库：
    - 支持只读从库，允许业务做读写分离（先不实现自动选主/故障转移）。

### 四、安全 & 多租户

- **6. 安全性（中优先级，视部署环境）**
  - 基础认证：
    - 实现简单版 `AUTH` + 密码校验，支持通过配置文件/环境变量设置密码。
    - 拒绝未认证连接执行绝大多数命令。
  - ACL 预留：
    - 暂不实现完整 Redis ACL 体系，仅规划后续可能的命令/键空间级控制。
  - 可选：
    - 视生产环境需要，预研 TLS 支持与部署方式。

### 五、可观测性 & 运维

- **7. 监控与可观测（高优先级）**
  - INFO 子集：
    - 暴露基本运行指标：内存占用、连接数、key 总数、命令调用次数、过期 key 数量等。
    - 当前已完成：INFO v1（redust_version、tcp_port、uptime_in_seconds、connected_clients、total_commands_processed、db0:keys 等基础指标）。
  - 指标导出：
    - 选用一个简单方案（如 HTTP 端口暴露 Prometheus 文本格式）进行实验性实现。
  - 日志：
    - 用统一日志框架替代零散的 `println!`，支持日志等级和模块划分。

### 六、性能 & 资源管理

- **8. 性能优化与资源控制（高优先级，贯穿所有阶段）**
  - 基准测试：
    - 使用 `redis-benchmark` 和内置 bench，对比不同命令族的 QPS 与 P99 延迟。
  - 并发模型：
    - 评估并优化 `Storage` 的锁粒度（如采用分片锁或并发 Map 实现）。
    - 当前阶段结论：采用单个 `DashMap<String, StorageValue>`，利用其内部分片锁作为主要并发策略，后续如遇瓶颈再演进。
  - 内存管理：
    - 规范化大 key/大集合的行为与限制，避免单 key 把进程拖垮。

### 七、协议兼容性

- **9. 协议与客户端兼容（中优先级）**
  - RESP 细节：
    - 对齐 Redis 在错误类型、nil、数组/多 bulk 等方面的边界行为。
  - 客户端兼容性测试：
    - 用常见语言的 Redis 客户端（如 redis-cli、redis-rs、go-redis、Jedis 等）做端到端测试。

> 建议：将上述 9 大类拆分为若干 Phase，例如：
> - **Phase A：单机、内网服务可用** —— 重点完成核心命令扩展（1/2）、持久化雏形（3）、基本稳定性（4）和监控 INFO 子集（7）。
> - **Phase B：对外生产环境部署** —— 加固安全（6）、完善监控与日志（7）、持续性能优化（8）、补充更多协议/客户端兼容性（9）。
> - **Phase C：读写分离与高可用探索** —— 逐步实现主从复制和只读从库（5），并在真实业务流量下做灰度验证。

---

## 本地基线性能（单连接，对比 Redis）

> 环境说明：
> - 机器：本地开发机（CPU/内存未特别限制，单实例，loopback 网络）。
> - 压测方式：`cargo run --release --bin resp_set_get_bench`，单连接、同步往返。
> - 迭代次数：各组 `--iterations=5000`，所有 group 依次串行执行。
> - Redust：监听 `127.0.0.1:6380`，无 AUTH。
> - Redis：监听 `127.0.0.1:6379`，`AUTH lab`。

### 1. Redust @ 127.0.0.1:6380（无 AUTH）

命令：

```bash
cargo run --release --bin resp_set_get_bench -- \
  --addr=127.0.0.1:6380 \
  --iterations=5000
```

结果概览（取 `ops/sec`，越大越好）：

| group            | 语义概述                     | total ops | ops/sec |
|------------------|------------------------------|----------:|--------:|
| `ping`           | 纯 PING 往返                 |     5,000 | 20,896.78 |
| `mset_mget`      | 固定 10 key 的 MSET + MGET   |    10,000 |  5,816.35 |
| `hash_ops`       | 同一 Hash 上 HSET+HGET       |    10,000 | 16,651.40 |
| `set_get`        | 单 key SET+GET               |    10,000 | 16,028.64 |
| `incr_decr`      | 单 key INCR+DECR             |    10,000 | 19,232.68 |
| `exists_del`     | SET+EXISTS+DEL 组合          |    15,000 | 18,043.76 |
| `list_ops`       | LPUSH/RPUSH/LRANGE 组合      |    15,000 | 16,839.69 |
| `list_pops`      | LPOP/RPOP 循环               |    10,000 | 15,145.64 |
| `set_union`      | 两个集合上的 SADD+SUNION     |    15,000 |  1,215.17 |
| `set_intersection` | 三集合 SADD+SINTER         |    20,000 |  7,298.96 |
| `set_difference` | 两集合 SADD+SDIFF            |    10,000 |  1,711.47 |

### 2. Redis @ 127.0.0.1:6379（AUTH lab）

命令：

```bash
cargo run --release --bin resp_set_get_bench -- \
  --addr=127.0.0.1:6379 \
  --iterations=5000 \
  --auth=lab
```

结果概览：

| group              | 语义概述                     | total ops | ops/sec |
|--------------------|------------------------------|----------:|--------:|
| `ping`             | 纯 PING 往返                 |     5,000 | 11,926.82 |
| `mset_mget`        | 固定 10 key 的 MSET + MGET   |    10,000 | 10,582.72 |
| `hash_ops`         | 同一 Hash 上 HSET+HGET       |    10,000 | 11,245.02 |
| `set_get`          | 单 key SET+GET               |    10,000 | 11,213.62 |
| `incr_decr`        | 单 key INCR+DECR             |    10,000 | 11,420.17 |
| `exists_del`       | SET+EXISTS+DEL 组合          |    15,000 | 11,461.14 |
| `list_ops`         | LPUSH/RPUSH/LRANGE 组合      |    15,000 | 11,000.05 |
| `list_pops`        | LPOP/RPOP 循环               |    10,000 | 11,123.26 |
| `set_union`        | 两个集合上的 SADD+SUNION     |    15,000 |    719.25 |
| `set_intersection` | 三集合 SADD+SINTER           |    20,000 |  8,649.56 |
| `set_difference`   | 两集合 SADD+SDIFF            |    10,000 |    641.30 |

### 3. 粗略观察与后续方向

- **注意：** 以上为单机、开发环境下的单连接往返吞吐，尚未考虑 pipeline、多连接、高并发、内存占用等因素，仅作为早期对比参考。
- Redust 在部分轻量命令（`PING`、`INCR/DECR` 等）上的单连接 QPS 已处于可用水平，但与 Redis 相比，不同命令族的相对关系并不完全一致，后续需要结合 flamegraph/Profiling 做更细粒度分析。
- 集合相关命令（尤其是 `SUNION` / `SDIFF`）在 Redust 与 Redis 上都表现为明显低于其他命令，符合其 O(N) 特性，可以在 `future.md` 中登记后续优化方向（例如：减少分配、优化遍历策略）。
