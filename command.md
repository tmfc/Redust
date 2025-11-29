# Redust 命令实现进度

> 使用 todo list 记录命令实现状态；勾选表示当前版本已具备“基本可用”的行为。

---

## 连接与调试类

- [x] **PING**
  - 当前：`PING` → `+PONG`，参数过多时报错。
  - TODO：暂不需要扩展。
- [x] **ECHO**
  - 当前：`ECHO msg` → bulk string；参数个数校验完整。
- [x] **QUIT**
  - 当前：`QUIT` → `+OK` 并关闭连接。

---

## 字符串 String

- [x] **SET key value**
  - 当前：基础 `SET key value` 行为 + 覆盖已有值，单元/集成测试完善。
- [x] **SET key value EX seconds / PX milliseconds**
  - 当前：
    - 解析 `EX`/`PX` 选项，非负整数校验。
    - 通过 `expire_millis` 写入过期时间（相对时间）。
    - 懒删除 + 定期删除策略生效。
  - TODO：
    - 支持 `NX` / `XX` / `KEEPTTL` / `GET` 等高级选项组合。
    - 支持 `EXAT` / `PXAT` 绝对时间语义。
- [x] **GET key**
  - 当前：
    - 未过期：返回 bulk string。
    - 过期/不存在：返回 `$-1`。

- [x] **INCR / DECR / INCRBY / DECRBY**
  - 当前：
    - `INCR` / `DECR`：基于字符串整数值 +1 / -1，非整数或溢出时报 `-ERR value is not an integer or out of range`。
    - `INCRBY key delta` / `DECRBY key delta`：在上述语义基础上支持带步长的自增/自减，delta 为 `i64`，错误语义同上。
    - 不存在的 key 视为 `0` 再进行运算。
  - TODO：后续可考虑 `INCRBYFLOAT` 等扩展命令。

- [x] **MSET key value [key value ...] / MGET key [key ...]**
  - 当前：
    - `MSET`：要求参数个数为偶数且 >= 2，原子性简化为“要么全部 set，要么参数错误直接报错不写入”；成功返回 `+OK`。
    - `MGET`：对每个 key 独立调用当前 `GET` 语义，组成数组返回；不存在或类型不匹配的 key 返回 `nil`。

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
  - 当前：支持 `*` 和精确匹配；不支持通配表达式（如 `user:*`）。
  - TODO：实现简单模式匹配（glob 风格）。

---

## 列表 List

- [x] **LPUSH key value [value ...]**
- [x] **RPUSH key value [value ...]**
- [x] **LRANGE key start stop**
- [x] **LPOP key**
- [x] **RPOP key**

状态说明：
- 已有较完整的行为覆盖测试（边界下标、空列表、多个客户端可见性等）。
- TODO：尚未实现 `LLEN`、`LREM`、`LINDEX` 等其他列表命令。

---

## 集合 Set

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
- TODO：暂未实现 `SUNIONSTORE` / `SINTERSTORE` / `SDIFFSTORE` 等写入型命令。

---

## 哈希 Hash

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

## 过期与 TTL

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

## 计划中的命令（尚未实现）

- [ ] **高级 SET 选项**
  - `SET key value NX|XX [EX seconds|PX milliseconds|EXAT unix-time|PXAT ms-unix-time] [KEEPTTL] [GET]`
- [ ] **键空间扫描**
  - `SCAN` 及其与模式匹配的组合。
- [ ] **键/过期查询增强**
  - `EXISTS`、`KEYS` 的模式匹配兼容更多 Redis 语义。
- [ ] **信息与监控相关命令**
  - `INFO` 子集（连接数、key 数、命令计数、内存估算等）。
- [ ] **持久化相关命令（待结合持久化 PoC 再定）**
  - 如：`SAVE` / `BGSAVE` / `LASTSAVE` / 简化版 AOF 控制命令等。

---

## 部分完成 / 仍需补充的工作概览

- **SET 带 EX/PX**：
  - 已有：基础相对过期语义 + lazy/active 删除 + TTL/PTTL/PERSIST 配套。
  - 待补：NX/XX/EXAT/PXAT/KEEPTTL/GET 组合语义与冲突规则。
- **KEYS**：
  - 已有：`*` + 精确匹配。
  - 待补：简单 glob 模式实现与测试。
- **过期策略**：
  - 已有：固定间隔 + 固定样本数的定期清理；语义上接近 Redis，参数尚未调优。
  - 待补：
    - 抽样规则更细化（例如优先抽有过期时间的 key）。
    - 简单的指标观测（如每轮扫描删除数、过期 key 总数）。

---

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
- [ ] RENAME
- [ ] RENAMENX
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
- [ ] CLIENT *（如 CLIENT LIST / SETNAME / GETNAME 等）*
- [ ] HELLO
- [ ] SELECT
- [ ] INFO
- [ ] CONFIG *（GET/SET/RESETSTAT 等）*
- [ ] MONITOR
- [ ] SLOWLOG
- [ ] TIME
- [ ] COMMAND *（完整 COMMAND 系列）*

### Strings

- [x] SET
- [x] GET
- [x] INCR
- [x] DECR
- [ ] APPEND
- [ ] GETSET
- [x] MGET
- [x] MSET
- [ ] MSETNX
- [ ] STRLEN
- [x] INCRBY
- [x] DECRBY
- [ ] INCRBYFLOAT
- [x] SETEX
- [x] PSETEX
- [x] SETNX
- [ ] GETRANGE
- [ ] SETRANGE
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
- [ ] LLEN
- [ ] LINDEX
- [ ] LSET
- [ ] LINSERT
- [ ] LREM
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
- [ ] SUNIONSTORE
- [ ] SINTERSTORE
- [ ] SDIFFSTORE
- [ ] SSCAN

### Sorted Sets (ZSets)

- [ ] ZADD
- [ ] ZREM
- [ ] ZCARD
- [ ] ZCOUNT
- [ ] ZINCRBY
- [ ] ZINTER / ZINTERSTORE
- [ ] ZUNION / ZUNIONSTORE
- [ ] ZDIFF / ZDIFFSTORE
- [ ] ZRANGE / ZRANGEBYSCORE / ZRANGEBYLEX
- [ ] ZREVRANGE / ZREVRANGEBYSCORE / ZREVRANGEBYLEX
- [ ] ZPOPMIN / ZPOPMAX
- [ ] BZPOPMIN / BZPOPMAX
- [ ] ZLEXCOUNT
- [ ] ZMSCORE
- [ ] ZRANK / ZREVRANK
- [ ] ZSCORE
- [ ] ZSCAN

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

- [ ] PUBLISH
- [ ] SUBSCRIBE
- [ ] PSUBSCRIBE
- [ ] UNSUBSCRIBE
- [ ] PUNSUBSCRIBE
- [ ] PUBSUB *（CHANNELS/NUMSUB/NUMPAT）*

### Transactions

- [ ] MULTI
- [ ] EXEC
- [ ] DISCARD
- [ ] WATCH
- [ ] UNWATCH

### Scripting / Functions

- [ ] EVAL
- [ ] EVALSHA
- [ ] SCRIPT *（LOAD/FLUSH/EXISTS/KILL DEBUG）*
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
