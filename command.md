# Redust 命令实现进度

> 目标：社区版对齐 Redis 行为，分三期推进。
> - ✅ **Phase A（核心功能）**：已完成 - 5 种数据结构、事务、Lua 脚本、持久化、Pub/Sub、运维命令
> - 🔄 **Phase B（数据结构扩展）**：进行中 - Streams、Geo、HyperLogLog、Bitmaps、主从复制
> - 📋 **Phase C（企业版探索）**：规划中 - Sentinel/Cluster/跨机房复制等高级特性

---

## Phase A：核心功能 ✅ 已完成

### 统计概览
- **已实现命令**: 120+ 个
- **数据结构**: String, List, Set, Hash, Sorted Set
- **高级特性**: 事务（MULTI/EXEC/WATCH）、Lua 脚本（redis.call/pcall）
- **持久化**: AOF + RDB
- **Pub/Sub**: Channel/Pattern/Shard 订阅
- **测试覆盖**: 99 个测试全部通过

### 连接与调试类

- [x] **PING**
  - 当前：`PING` → `+PONG`，参数过多时报错。
  - TODO：暂不需要扩展。
- [x] **ECHO**
  - 当前：`ECHO msg` → bulk string；参数个数校验完整。
- [x] **QUIT**
  - 当前：`QUIT` → `+OK` 并关闭连接。
  
### 信息与监控

- [x] **INFO**
  - 当前：返回 `# Server`、`# Clients`、`# Stats`、`# Keyspace` 等基础信息。
  - 额外包含内存相关字段：`maxmemory` / `maxmemory_human` / `used_memory` / `used_memory_human`，用于观测内存配置与当前估算使用量。

---

### 字符串 String

- [x] **SET key value**
  - 当前：基础 `SET key value` 行为 + 覆盖已有值，单元/集成测试完善。
- [x] **SET key value EX seconds / PX milliseconds**
  - 当前：
    - 支持 `EX`/`PX` 相对过期（秒/毫秒），`EXAT`/`PXAT` 绝对过期时间（Unix 秒/毫秒）。
    - 支持 `NX` / `XX` / `KEEPTTL` / `GET` 等高级选项组合：
      - `NX` 仅在 key 不存在/已过期时写入；`XX` 仅在 key 存在时写入；二者互斥。
      - `GET` 返回旧值（或 `$-1`），无论本次是否写入成功（与 Redis 一致）。
      - `KEEPTTL` 在未显式指定 EX/PX/EXAT/PXAT 时保留旧 TTL，否则由显式 TTL 覆盖。
    - 过期时间为非整数或为负数时，返回 `-ERR value is not an integer or out of range`；语法冲突（如同时出现 EX 与 PX/EXAT/PXAT）返回 `-ERR syntax error`。
    - 懒删除 + 定期删除策略生效。
- [x] **GET key**
  - 当前：
    - 未过期：返回 bulk string。
    - 过期/不存在：返回 `$-1`。

- [x] **GETDEL key** / **GETEX key [EX seconds|PX milliseconds|PERSIST]**
  - 当前：
    - `GETDEL`：对 String 类型返回旧值并删除 key；不存在/过期返回 `$-1`；类型不为 String 时返回 WRONGTYPE 错误。
    - `GETEX`：语义等价 `GET`，同时根据选项更新或清除 TTL：
      - `EX seconds` / `PX milliseconds`：在成功返回当前值后设置新的相对过期时间（`seconds <= 0`/`millis <= 0` 时按照 EXPIRE/PEXPIRE 语义立刻删除）。
      - `PERSIST`：清除已有过期时间，保留当前值。
    - 对不存在/已过期的 key：只返回 `$-1`，不改变 TTL 状态。

- [x] **INCR / DECR / INCRBY / DECRBY**
  - 当前：
    - `INCR` / `DECR`：基于字符串整数值 +1 / -1，非整数或溢出时报 `-ERR value is not an integer or out of range`。
    - `INCRBY key delta` / `DECRBY key delta`：在上述语义基础上支持带步长的自增/自减，delta 为 `i64`，错误语义同上。
    - 不存在的 key 视为 `0` 再进行运算。
  - TODO：后续可考虑 `INCRBYFLOAT` 等扩展命令。

- [x] **INCRBYFLOAT key increment**
  - 当前：
    - 基于字符串值按 `f64` 做浮点自增，结果覆盖写回为十进制字符串（去掉多余尾随 0，与 Redis 行为接近）。
    - 不存在或已过期的 key 视为 `0` 再进行运算。
    - 参与计算的当前值或 increment 不是合法浮点，或结果为 `NaN` / `Inf` 时，返回 `-ERR value is not a valid float`，并保持原值不变。

- [x] **MSET key value [key value ...] / MGET key [key ...]**
  - 当前：
    - `MSET`：要求参数个数为偶数且 >= 2，原子性简化为“要么全部 set，要么参数错误直接报错不写入”；成功返回 `+OK`。
    - `MGET`：对每个 key 独立调用当前 `GET` 语义，组成数组返回；不存在或类型不匹配的 key 返回 `nil`。

- [x] **MSETNX key value [key value ...]**
  - 当前：
    - 仅当所有目标 key 当前都不存在或已过期时，才执行批量写入并返回 `:1`。
    - 只要有任意一个目标 key 已存在且未过期，则整个命令不写入任何 key，返回 `:0`，保证 all-or-nothing 原子语义。

- [x] **SETNX / SETEX / PSETEX**
  - 当前：
    - `SETNX key value`：当 key 不存在或已过期时写入并返回 `:1`，否则不变并返回 `:0`。
    - `SETEX key seconds value`：等价 `SET key value` + `EXPIRE key seconds`，seconds 为非负整数，错误时返回整数错误。
    - `PSETEX key milliseconds value`：等价 `SET key value` + `PEXPIRE key milliseconds`，语义同上，单位为毫秒。
  - TODO：
    - 与带 EX/PX 的 `SET` 高级选项打通统一语义。

- [x] **DEL key [key ...]**
- [x] **EXISTS key [key ...]**
- [x] **TYPE key**
- [x] **KEYS pattern**
  - 当前：在当前 DB 的逻辑 key 上支持 glob 风格匹配（`*`、`user:*`、`foo?` 等），匹配语义与 Pub/Sub 中的模式订阅一致，已通过端到端测试覆盖。

---

### 列表 List

- [x] **LPUSH key value [value ...]**
- [x] **RPUSH key value [value ...]**
- [x] **LRANGE key start stop**
- [x] **LPOP key**
- [x] **RPOP key**

状态说明：
- 已有较完整的行为覆盖测试（边界下标、空列表、多个客户端可见性等）。
- 已实现 `LLEN`、`LREM`、`LINDEX` 等常用列表命令，并在端到端测试中覆盖参数错误、整数解析错误和 WRONGTYPE 等行为。

---

### 集合 Set

- [x] **SADD key member [member ...]**
- [x] **SREM key member [member ...]**
- [x] **SMEMBERS key**
- [x] **SCARD key**
- [x] **SISMEMBER key member**
- [x] **SUNION key [key ...]**
- [x] **SINTER key [key ...]**
- [x] **SDIFF key [key ...]**

状态说明：
- 多 key 运算在集合缺失/类型错误时的行为已经与 Redis 接近，并有针对性测试。
- 已补充 `SUNIONSTORE` / `SINTERSTORE` / `SDIFFSTORE` 等写入型命令，结果写回会覆盖目标 key 并清除旧 TTL。

---

### 哈希 Hash

- [x] **HSET key field value**
  - 当前：
    - key 不存在或已过期：创建一个 Hash 并插入该 field，返回 `1`。
    - key 已存在且为 Hash：如果是新 field 返回 `1`，覆盖已有 field 返回 `0`。
    - key 存在但类型不是 Hash：当前实现返回 `0`，不修改值。

- [x] **HGET key field**
  - 当前：
    - key 存在且为 Hash 且 field 存在：返回该 field 的 bulk string。
    - 其他情况（key 不存在、过期、类型不对、field 不存在）：`$-1`。

- [x] **HDEL key field [field ...]**
  - 当前：删除指定 field，返回成功删除的 field 数量；key 不存在或类型不对时返回 `0`。

- [x] **HEXISTS key field**
  - 当前：field 存在且 key 为 Hash 时返回 `:1`，否则返回 `:0`。

- [x] **HGETALL key**
  - 当前：
    - key 为 Hash：返回 `[field1, value1, field2, value2, ...]` 形式的数组（不保证顺序）。
    - 其余情况返回空数组。

状态说明：
- Hash 键的过期语义与 String/List/Set 一致，统一由 `EXPIRE`/`PEXPIRE`/`TTL`/`PTTL`/`PERSIST` 管理。

---

### 过期与 TTL

- [x] **EXPIRE key seconds**
  - 当前：
    - `seconds > 0`：设置相对过期时间，返回 `1` / `0`（key 是否存在）。
    - `seconds <= 0`：视为立刻删除。
- [x] **PEXPIRE key milliseconds**
- [x] **TTL key**
  - 当前：
    - key 不存在：`-2`。
    - 存在且无过期：`-1`。
    - 存在且有过期：剩余秒数，向上取整。
- [x] **PTTL key**
  - 当前：同 TTL，但单位为毫秒（返回 `-2` / `-1` / 剩余毫秒数）。
- [x] **PERSIST key**
  - 当前：
    - 原先有过期：清除过期并返回 `1`。
    - 无过期或 key 不存在：返回 `0`。

过期实现概要：
- 懒删除：所有读/改 key 的路径在操作前会检查并删除已过期键。
- 定期删除：后台任务定期抽样少量 key，执行过期检查与删除。

---

## Pub/Sub

- [x] **PUBLISH / SUBSCRIBE / PSUBSCRIBE / UNSUBSCRIBE / PUNSUBSCRIBE**
  - 当前：支持频道订阅与模式订阅，订阅模式下仅允许 (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT，推送 `message` / `pmessage` 事件；未认证连接禁止发布/订阅。
- [x] **PUBSUB CHANNELS / NUMSUB / NUMPAT**
  - 当前：`CHANNELS` 列出仍有订阅者的频道（可选简单 glob 过滤），`NUMSUB` 返回各频道的订阅数，`NUMPAT` 返回模式订阅总数；连接关闭后会自动退订并清理空频道/模式，慢订阅者会丢弃滞后消息但保持订阅。

---

### 命令错误风格与参数校验约定（Redis 对齐）

当前已实现的命令子集中，以下命令的**参数个数错误**与**整数解析错误**已经对齐 Redis 的错误风格，并通过端到端测试覆盖：

- 字符串与多 key：
  - `SET` / `GET` / `DEL` / `EXISTS` / `TYPE` / `KEYS`
  - `SCAN`
  - `INCR` / `DECR` / `INCRBY` / `DECRBY`
  - `MGET` / `MSET` / `SETNX` / `SETEX` / `PSETEX`
- 列表 List：
  - `LPUSH` / `RPUSH` / `LRANGE` / `LPOP` / `RPOP`
  - `LLEN` / `LINDEX` / `LREM` / `LTRIM`
- 集合 Set：
  - `SADD` / `SREM` / `SMEMBERS` / `SCARD` / `SISMEMBER`
  - `SUNION` / `SINTER` / `SDIFF`
- 哈希 Hash：
  - `HSET` / `HGET` / `HDEL` / `HEXISTS` / `HGETALL`
- 过期与 TTL：
  - `EXPIRE` / `PEXPIRE` / `TTL` / `PTTL` / `PERSIST`
- 连接与信息：
  - `PING` / `ECHO` / `QUIT` / `INFO`

统一的错误文案约定：

- 参数个数错误：
  - `-ERR wrong number of arguments for '<cmd>' command`（其中 `<cmd>` 为小写命令名）。
- 需要整数的位置传入非整数或越界值：
  - `-ERR value is not an integer or out of range`。
- 目前 `SET` 带 EX/PX 等选项解析中的语法错误：
  - `-ERR syntax error`。

未来新增命令/扩展子语义时，建议沿用以上三类错误文案，并在解析阶段尽早返回 `Command::Error`，保持所有调用路径的错误风格一致。

---

## Phase A 未完成的核心命令（优先补齐）

- [x] 高级 SET 选项：`EXAT` / `PXAT`、完整 NX/XX/KEEPTTL/GET 组合与冲突校验（已实现并通过端到端测试；复杂组合和边界行为的进一步打磨见 future.md）。
- [x] 键扫描与模式：`SCAN`/`SSCAN`/`HSCAN`/`ZSCAN`，`KEYS` 模式兼容更丰富 glob（已实现基础 `SCAN` + `MATCH`/`COUNT` 以及 `KEYS` glob 匹配；`ZSCAN` 已实现）。
- [x] 有序集合：`ZADD`/`ZREM`/`ZRANGE`/`ZREVRANGE`/`ZCARD`/`ZINCRBY`/`ZSCORE`/`ZSCAN` 等基础子集（已实现并通过端到端测试）。
- [ ] 流（Streams）：`XADD`/`XRANGE`/`XREAD`/`XDEL` 等基础读写。
- [x] 事务：`MULTI`/`EXEC`/`DISCARD`/`WATCH`/`UNWATCH`（已实现，支持命令队列和乐观锁）。
- [x] Lua 脚本：`EVAL`/`EVALSHA`/`SCRIPT LOAD|EXISTS|FLUSH`（基础版，暂不支持 `redis.call`/`redis.pcall`）。
- [x] 持久化控制命令：`SAVE`/`BGSAVE`/`LASTSAVE`，RDB 快照已实现。
- [ ] 复制命令子集（社区版）：`REPLCONF`/`PSYNC`/`SLAVEOF`/`REPLICAOF`（主从握手与增量复制）。
- [ ] 客户端/运维：`CONFIG GET/SET` 子集、`SLOWLOG`、`CLIENT LIST`/`PAUSE`/`UNBLOCK`。

---

## Phase B 展望（性能、安全、运维强化）

- SET/过期策略调优：采样过期、指标观测、maxmemory 策略对齐。
- 安全：ACL 子集、TLS。
- 运维：更丰富的 CONFIG/CLIENT/ADMIN 命令，slowlog/监控细节。
- 性能：并发与内存优化、benchmark 与 profile 跟进。

---

## Phase C（企业版）超出当前范围

- Sentinel/Cluster、高可用、跨机房复制。
- 高级 ACL、审计日志、多租户隔离。

## Redis 全量命令总览（按模块）

> 说明：
> - `[x]` 表示 Redust 当前版本已实现该命令的**核心子集语义**。
> - `[ ]` 表示尚未实现。
> - 不区分大小写，以下均以大写列出以方便对照官方文档。

### Generic / Keys

- [x] DEL
- [x] EXISTS
- [ ] TOUCH
- [x] TYPE
- [x] KEYS
- [ ] SCAN
- [ ] RANDOMKEY
- [x] RENAME
- [x] RENAMENX
- [ ] MOVE
- [ ] DUMP
- [ ] RESTORE
- [ ] MIGRATE
- [ ] OBJECT

### Expire / TTL

- [x] EXPIRE
- [x] PEXPIRE
- [ ] EXPIREAT
- [ ] PEXPIREAT
- [x] TTL
- [x] PTTL
- [x] PERSIST

### Connection / Server 级

- [x] PING
- [x] ECHO
- [x] QUIT
- [ ] AUTH
- [x] CLIENT LIST - 列出客户端连接信息
- [x] CLIENT ID - 获取当前连接 ID
- [x] CLIENT SETNAME - 设置连接名称
- [x] CLIENT GETNAME - 获取连接名称
- [ ] CLIENT PAUSE / UNBLOCK / KILL / REPLY
- [ ] HELLO
- [ ] SELECT
- [ ] INFO
- [x] CONFIG GET - 获取配置参数（支持模式匹配）
- [x] CONFIG SET - 设置配置参数（大多数参数不可动态修改）
- [ ] CONFIG RESETSTAT
- [ ] MONITOR
- [x] SLOWLOG GET - 获取慢日志（当前返回空）
- [x] SLOWLOG RESET - 重置慢日志
- [x] SLOWLOG LEN - 获取慢日志长度
- [ ] TIME
- [ ] COMMAND *（完整 COMMAND 系列）*

### Strings

- [x] SET
- [x] GET
- [x] INCR
- [x] DECR
- [x] APPEND
- [x] GETSET
- [x] MGET
- [x] MSET
- [x] MSETNX
- [x] STRLEN
- [x] INCRBY
- [x] DECRBY
- [x] INCRBYFLOAT
- [x] SETEX
- [x] PSETEX
- [x] SETNX
- [x] GETRANGE
- [x] SETRANGE
- [ ] SUBSTR（已废弃，等价 GETRANGE）

### Hashes

- [x] HSET
- [x] HGET
- [x] HGETALL
- [x] HDEL
- [x] HEXISTS
- [ ] HINCRBY
- [ ] HINCRBYFLOAT
- [ ] HKEYS
- [ ] HLEN
- [ ] HMGET
- [ ] HMSET
- [ ] HSETNX
- [ ] HSTRLEN
- [ ] HVALS
- [ ] HSCAN

### Lists

- [x] LPUSH
- [x] RPUSH
- [x] LPOP
- [x] RPOP
- [x] LRANGE
- [x] LLEN
- [x] LINDEX
- [ ] LSET
- [ ] LINSERT
- [x] LREM
- [ ] BLPOP
- [ ] BRPOP
- [ ] BRPOPLPUSH
- [ ] RPOPLPUSH

### Sets

- [x] SADD
- [x] SREM
- [x] SMEMBERS
- [x] SCARD
- [x] SISMEMBER
- [x] SUNION
- [x] SINTER
- [x] SDIFF
- [x] SUNIONSTORE
- [x] SINTERSTORE
- [x] SDIFFSTORE
- [ ] SSCAN

### Sorted Sets (ZSets)

- [x] ZADD
- [x] ZREM
- [x] ZCARD
- [ ] ZCOUNT
- [x] ZINCRBY
- [ ] ZINTER / ZINTERSTORE
- [ ] ZUNION / ZUNIONSTORE
- [ ] ZDIFF / ZDIFFSTORE
- [x] ZRANGE / ZRANGEBYSCORE / ZRANGEBYLEX
- [x] ZREVRANGE / ZREVRANGEBYSCORE / ZREVRANGEBYLEX
- [ ] ZPOPMIN / ZPOPMAX
- [ ] BZPOPMIN / BZPOPMAX
- [ ] ZLEXCOUNT
- [ ] ZMSCORE
- [ ] ZRANK / ZREVRANK
- [x] ZSCORE
- [x] ZSCAN

### Streams

- [ ] XADD
- [ ] XDEL
- [ ] XREAD
- [ ] XREADGROUP
- [ ] XRANGE / XREVRANGE
- [ ] XACK
- [ ] XCLAIM / XAUTOCLAIM
- [ ] XGROUP *（CREATE/SETID/DESTROY/DELCONSUMER）*
- [ ] XINFO *（STREAM/CONSUMERS/GROUPS）*
- [ ] XLEN
- [ ] XPENDING

### Pub/Sub

- [x] PUBLISH / SUBSCRIBE / UNSUBSCRIBE
- [x] PSUBSCRIBE / PUNSUBSCRIBE
- [x] 分片：SPUBLISH / SSUBSCRIBE / SUNSUBSCRIBE
- [x] PUBSUB *（CHANNELS/NUMSUB/NUMPAT/SHARDCHANNELS/SHARDNUMSUB/HELP）*

### Transactions

- [x] MULTI
- [x] EXEC
- [x] DISCARD
- [x] WATCH
- [x] UNWATCH

### Scripting / Functions

- [x] EVAL（基础版，暂不支持 redis.call/pcall）
- [x] EVALSHA（基础版，暂不支持 redis.call/pcall）
- [x] SCRIPT *（LOAD/FLUSH/EXISTS，暂不支持 KILL/DEBUG）*
- [ ] FUNCTION *（LOAD/DELETE/FLUSH/LIST/DUMP/RESTORE/HELP）*

### Geo

- [ ] GEOADD
- [ ] GEOPOS
- [ ] GEODIST
- [ ] GEOHASH
- [ ] GEORADIUS / GEORADIUSBYMEMBER （已被 GEOSEARCH 等命令取代）
- [ ] GEOSEARCH
- [ ] GEOSEARCHSTORE

### HyperLogLog

- [ ] PFADD
- [ ] PFCOUNT
- [ ] PFMERGE

### Bitmaps

- [ ] SETBIT
- [ ] GETBIT
- [ ] BITCOUNT
- [ ] BITPOS
- [ ] BITOP
- [ ] BITFIELD
- [ ] BITFIELD_RO

### Modules / ACL / Cluster 等

- [ ] MODULE *（LIST/LOAD/UNLOAD）*
- [ ] ACL *（LIST/SETUSER/DELUSER/LOAD/SAVE/LOG 等）*
- [ ] CLUSTER *（各类子命令）*
- [ ] SHUTDOWN
- [ ] REPLICAOF / SLAVEOF
- [ ] REPLCONF

> 注：以上列表不保证与 Redis 最新版本 100% 同步，但已覆盖主流命令族。可以将其视作“Redust 与 Redis 的差距清单”，后续实现某个命令时，只需在此文中将对应条目标记为 `[x]` 并补充子语义说明即可。
