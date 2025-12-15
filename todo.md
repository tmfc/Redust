# TODO (Phase D)

## ACL 增强

- [x] Redis 客户端兼容性测试补全（集成测试 + 行为对齐）

## 客户端矩阵（真实客户端回归）

- [x] `redis-rs`：pipeline（多命令批量发送）
- [x] `redis-rs`：事务 `MULTI/EXEC`（含 `DISCARD`）
- [x] `redis-rs`：`WATCH/UNWATCH` 基本语义（key 变更触发 abort）
- [x] `redis-rs`：`PUBSUB`（订阅/取消订阅/模式订阅）
- [x] `redis-rs`：连接断开/重连后继续可用（最小场景）

- [x] `go-redis`：基础命令 + pipeline（含 `TxPipeline`）
- [x] `go-redis`：事务（`WATCH` + `TxPipelined`）
- [x] `go-redis`：阻塞命令（如 `BLPOP`）超时/取消

- [ ] `Jedis`：基础命令 + pipeline
- [ ] `Lettuce`：异步 API（future）+ 连接复用

- [ ] `node-redis`：基础命令 + pipeline
- [ ] `ioredis`：`cluster` 模式下的单节点兼容（不做集群，只验证客户端握手/常见选项不报错）

## 连接与会话行为（网络/协议层）

- [x] RESP 解析：分片/粘包/大包（单条命令拆成多段发送）
- [x] RESP 解析：多条命令连发（同一 TCP 包内多个请求）
- [ ] inline command：`PING`/`QUIT`/`INFO`（如果支持）
- [ ] 大量 pipeline：1w 条 `INCR` 压测（主要验证正确性与不崩）
- [ ] 多连接并发：N=50 并发 GET/SET（验证无数据竞争与响应完整）
- [ ] 连接关闭语义：客户端半关闭/EOF 时 server 侧清理资源

## 认证与权限（与主流客户端交互一致）

- [ ] 未认证连接：只能 `PING/ECHO/QUIT/AUTH`（客户端收到的错误类型/文案对齐）
- [ ] `AUTH`：错误密码返回（redis-rs/go-redis/jedis/node-redis 的错误解析是否正常）
- [ ] `AUTH username password`：与 ACL 用户交互（如果已支持）

## 错误类型与返回值兼容性（重点：客户端能否正确解码）

- [ ] `-ERR`/`-WRONGTYPE`：客户端错误类型映射一致（至少不 panic）
- [ ] Null bulk / Null array：`GET` miss、`HGET` miss、`BLPOP` timeout 的返回形态
- [ ] 整数回复：`INCR/LLEN/SCARD/ZCARD` 等返回 i64
- [ ] 空集合/空数组：`LRANGE` 空、`SMEMBERS` 空、`HGETALL` 空
- [ ] 字符串二进制安全：value 含 `\0`、非 UTF-8（客户端 roundtrip 不截断）

## 事务/脚本/发布订阅（高交互特性）

- [ ] `MULTI/EXEC`：排队回复（QUEUED）与实际结果数组
- [ ] `MULTI` 中错误命令：`EXECABORT` 行为（命令语法错误 vs 运行时错误）
- [ ] `WATCH`：在不同连接修改 key 触发事务失败
- [ ] Lua：`EVAL` 返回类型（int/bulk/array/map）
- [ ] Lua：脚本报错（`-ERR` + 堆栈）客户端侧可读

- [ ] Pub/Sub：订阅后普通命令是否被拒绝/如何返回（与 Redis 行为对齐）
- [ ] Pub/Sub：`SUBSCRIBE` / `UNSUBSCRIBE` 返回帧结构

## 兼容性回归组织方式（测试工程化）

- [ ] 将现有 `tests/redis_compat.rs` 扩展为：按 feature 分组的多个测试文件（如 `client_compat_redis_rs.rs`）
- [ ] 为外部语言客户端回归（go/java/node）提供可重复脚本（CI 可选，不强制）
- [ ] 为每个客户端测试建立最小用例清单（命令集 + 期望）
