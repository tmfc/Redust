# TODO（Phase A 优先级）

- [ ] Sorted Sets：实现存储层与命令族（ZADD/ZCARD/ZRANGE/ZREVRANGE/ZSCORE/ZREM/ZINCRBY/ZSCAN），覆盖 WRONGTYPE/过期语义与端到端测试。
- [ ] 持久化基线：AOF everysec 写入与启动加载；支持 SAVE/BGSAVE/LASTSAVE，后续补充 RDB 导出/导入。
- [ ] 复制握手：REPLCONF/PSYNC/REPLICAOF（兼容 SLAVEOF 别名），提供只读从库的全量+增量同步路径。
- [ ] 事务与脚本：MULTI/EXEC/DISCARD/WATCH 语义对齐；EVAL/EVALSHA 简化 Lua 环境，覆盖错误与并发场景。
- [ ] 运维命令子集：CONFIG GET/SET（核心配置），CLIENT LIST/PAUSE/UNBLOCK，SLOWLOG GET/RESET，对齐 INFO/Prometheus 输出字段。
- [ ] 数据结构补齐：Streams 起步（XADD/XRANGE/XREAD 基本流），Geo/HyperLogLog/Bitmaps 预研，保证客户端兼容。
