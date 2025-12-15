# TODO (Phase D)

## ACL 增强

- [x] AUTH 与 ACL 集成（支持 `AUTH username password`）
- [x] 命令执行前的权限检查钩子
- [x] ACL SAVE/LOAD 持久化用户配置

## 运维增强

- [x] **INFO 命令实现**
  - [x] 实现 `INFO [section]` 命令
  - [x] 支持 section：server, clients, memory, persistence, stats, replication, pubsub, keyspace
  - [x] 复制状态（replication section）- 基础实现（role:master）

## 性能优化

- [x] **HyperLogLog 稀疏表示优化**
  - [x] 实现稀疏/密集双模式存储
  - [x] 小基数时节省 90%+ 内存（100 元素时仅占用 ~300 字节 vs 16KB）
