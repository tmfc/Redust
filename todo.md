# Redust 项目待办事项
 
本文件用于跟踪当前「可以立刻开干」的工作项。中长期路线请参考 `roadmap.md`。

---

## 当前聚焦的小类工作（Pub/Sub 差异清单）

- [x] **分片 Pub/Sub（Redis 7 SHARD 系列）**
  - 已支持 `SSUBSCRIBE` / `SUNSUBSCRIBE` / `SPUBLISH`，复用现有 hub 发送 `message` 事件；`PUBSUB SHARDCHANNELS/SHARDNUMSUB` 也已返回订阅列表与计数。

- [x] **PUBSUB 子命令补全**
  - 已补充 HELP / SHARDCHANNELS / SHARDNUMSUB，保持与 Redis 接口一致（SHARD 系列当前返回本地订阅结果）。

- [x] **订阅模式下 PING 兼容性**
  - 已支持携带 payload 的 `PING`，订阅模式下返回 `pong <payload>`，并添加端到端测试覆盖。

- [ ] **投递与缓冲策略对齐 Redis**
  - 现状：滞后时直接丢弃旧消息以保持订阅；Redis 使用输出缓冲策略（可能阻塞或断开）。
  - 目标：实现可配置的输出缓冲/断开策略，并暴露观察指标。

- [x] **指标与观测**
  - 已在 Prometheus/INFO 暴露订阅数量、分片订阅数量，以及消息投递/丢弃计数。
