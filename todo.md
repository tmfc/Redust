# Redust 项目待办事项
 
本文件用于跟踪当前「可以立刻开干」的工作项。中长期路线请参考 `roadmap.md`。

---

## 当前聚焦的小类工作（从 roadmap 挑选）

- [x] **统一日志框架替代 println!（日志与可观测性）**
  - 目标：用一个简单的日志库替换当前散落在各处的 `println!` / `eprintln!`，至少支持日志等级（info/warn/error）和模块前缀（如 [conn] / [resp] / [rdb]）。
  - 初版范围：只在 server 启动、连接处理、RDB 加载/保存、metrics 导出等关键路径接入，不做复杂配置系统。

- [x] **扩展客户端兼容性测试（redis-cli / go-redis 子集）**（已在 `future.md` 中登记为后续工作）
  - 目标：在现有 redis-rs 端到端测试基础上，增加一组基于 `redis-cli` 与 Go `go-redis` 的基础命令回归，用于发现协议/行为差异。
  - 初版范围：覆盖 string（SET/GET/INCR）、list（RPUSH/LRANGE）、hash（HSET/HGET/HGETALL）和基本过期命令（EXPIRE/TTL）。

- [x] **在现有类型上补常用命令（Hash & Set & SET 扩展）**
  - Hash：`HINCRBY` / `HLEN` / `HKEYS` / `HVALS` / `HMGET` 等高频命令已补全，并有端到端测试覆盖。
  - Set：补充 `SPOP` / `SRANDMEMBER` / `SUNIONSTORE` / `SINTERSTORE` / `SDIFFSTORE` 等操作，完善集合读写与运算语义。
  - SET 扩展：从未来规划中提前实现完整 `SET` 选项组合，支持 `NX` / `XX` / `KEEPTTL` / `GET` 等，并与现有 EX/PX 实现对齐 Redis 行为。

- [x] **Pub/Sub MVP（SUBSCRIBE / UNSUBSCRIBE / PUBLISH 子集）**
  - 已实现：channel 级订阅与广播、订阅模式下命令限制、`subscribe`/`unsubscribe`/`message` 事件、订阅模式的 `PING`。
  - 后续扩展（向 Redis 对齐）：
    - [x] 模式订阅：`PSUBSCRIBE` / `PUNSUBSCRIBE`，推送 `pmessage` 事件。
    - [x] 查询命令：`PUBSUB CHANNELS|NUMSUB|NUMPAT`，含 pattern 统计。
    - [ ] 订阅生命周期：连接关闭/超时自动退订，空 channel 回收。
    - [x] 兼容性细节：`UNSUBSCRIBE` 无参数时的返回形态、多 channel 退订的计数一致性测试。
    - [ ] backpressure：broadcast 缓冲策略与订阅/消息速率指标，防止订阅者过慢导致发送失败。
    - [x] AUTH 交互：未认证禁止发布/订阅，认证后保持订阅。
