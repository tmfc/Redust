# TODO（Phase A 优先级）

- [x] Sorted Sets：实现存储层与命令族（ZADD/ZCARD/ZRANGE/ZREVRANGE/ZSCORE/ZREM/ZINCRBY/ZSCAN），覆盖 WRONGTYPE/过期语义与端到端测试。
- [x] 持久化基线：AOF everysec 写入与启动加载；支持 SAVE/BGSAVE/LASTSAVE，后续补充 RDB 导出/导入。
- [x] 事务基础：MULTI/EXEC/DISCARD/WATCH/UNWATCH 语义对齐，支持命令队列和乐观锁。
- [x] Lua 脚本基础：EVAL/EVALSHA/SCRIPT LOAD|EXISTS|FLUSH，支持 KEYS/ARGV 访问和基本返回值类型。
  - [x] redis.call()/redis.pcall() 回调实现（支持 40+ 常用命令）。
- [x] 运维命令子集：CONFIG GET/SET（核心配置），CLIENT LIST/ID/SETNAME/GETNAME，SLOWLOG GET/RESET/LEN。
- [ ] 数据结构补齐：Streams 起步（XADD/XRANGE/XREAD 基本流），Geo/HyperLogLog/Bitmaps 预研，保证客户端兼容。
