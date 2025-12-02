## 生产可用路线图（社区版：单机/轻量复制，兼容 Redis 客户端）

> 目标：在不包含 Sentinel/Cluster 等多服务器高可用特性的前提下，提供可替代 Redis 的生产可用单实例（可选轻量复制），兼容现有 Redis 客户端与协议习惯。

### 一、协议与命令覆盖
- 核心数据结构：Strings/Lists/Sets/Hashes 已基本齐全；补齐 Sorted Sets（ZADD/ZRANGE/ZREVRANGE/ZREM/ZCARD/ZINCRBY 等）、Streams、Geo、HyperLogLog、Bitmaps。
- 事务与脚本：实现 MULTI/EXEC/DISCARD/WATCH 及 EVAL/EVALSHA（简化 Lua 环境即可），保证与客户端库兼容。
- Pub/Sub：已支持 channel/pattern/shard 子集与 PUBSUB 查询；持续对齐行为细节（缓冲、订阅模式命令限制等）。
- 扩展命令：SCAN/SSCAN/HSCAN 等游标扫描，键备份/恢复（DUMP/RESTORE），慢日志/CLIENT INFO 等兼容性命令。

### 二、持久化与数据安全
- 持久化格式：提供 Redis 兼容的 AOF（everysec 语义）与 RDB 导出/导入；支持启动加载、触发/自动持久化。
- 崩溃恢复：启动时校验并恢复数据，损坏文件友好降级（保留空库启动能力）。
- 复制（社区版上限）：提供基础主从复制（全量 + 增量命令流）与只读从库以支撑读写分离；Sentinel/多副本自动故障转移/Cluster 路由留给企业版。

### 三、内存管理与淘汰
- 对齐 Redis 的 `maxmemory` 策略：allkeys-lru、volatile-lru、allkeys-random、volatile-ttl 等；支持 `maxmemory-policy` 配置。
- 大 key/大集合保护：写入与读取的尺寸限制、渐进式释放策略，避免单 key 拖垮实例。
- 精准过期与采样：优化采样策略与时间轮/分层定时结构，减少过期抖动。

### 四、安全与租户隔离
- 认证与 ACL：在现有 AUTH 基础上增加 ACL 子集（用户/规则/频道限制），保持与客户端交互一致。
- 多 DB 与隔离：完善 DB 配置与隔离策略，为未来租户/ACL 铺路。
- 传输安全：预研 TLS 接入模式（stunnel/内建 TLS）。

### 五、可观测性与运维
- 指标与日志：INFO 扩展（内存、过期、复制、Pub/Sub 计数等）、Prometheus 指标完善（含 pubsub 缓冲/投递/丢弃、存储指标）、统一结构化日志。
- 管理命令：CONFIG GET/SET 子集、SLOWLOG 基础支持、CLIENT LIST/PAUSE/UNBLOCK 子集，便于运维脚本直接复用。
- 工具链：提供 docker 镜像与 systemd 示例配置，便于部署。

### 六、性能与兼容性验证
- 基准测试：redis-benchmark/自研 bench 的对齐，覆盖 pipeline、多连接、混合读写与大 key 场景，输出对比报告。
- 兼容性回归：用 redis-cli、go-redis、jedis/lettuce、node-redis 等主流客户端跑回归用例，覆盖事务、订阅、pipeline、超时等行为。
- 资源回归：在内存/CPU 限制下的稳定性与降级策略验证。

### 七、企业版保留项（不在社区版范围）
- Sentinel/Cluster 自动故障转移、多节点一致性、跨机房复制/备份。
- 高级安全（细粒度 ACL、审计日志）、多租户隔离强化。
- 商业支持与运维套件。

> 阶段建议：
> - **Phase A（功能齐备 + 持久化 + 兼容回归）**：补齐命令族（含 Sorted Set/Streams/脚本/事务）、AOF/RDB 兼容与基础复制、maxmemory 策略对齐、INFO/指标扩展。
> - **Phase B（性能与安全强化）**: 优化并发与内存、完善 ACL/TLS、补充运维命令与工具链，完成多客户端兼容回归。
> - **Phase C（企业版探索）**: Sentinel/Cluster/多副本 HA、审计/更丰富 ACL、跨机房复制等收费特性。

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
