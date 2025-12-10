# TODO

## 高优先级（需立即修复）

- [x] **慢日志锁竞争优化** (src/server.rs) ✅
  - 已使用 parking_lot::Mutex 替代 std::sync::Mutex
  - parking_lot 性能更好、不会 panic、占用空间更小

- [x] **Lua 脚本资源限制** (src/scripting.rs) ✅
  - 已添加执行超时限制（lua-time-limit，默认 5000ms）
  - 已添加内存使用限制（lua-max-memory，默认 10MB）
  - 支持环境变量配置：REDUST_LUA_TIME_LIMIT_MS、REDUST_LUA_MAX_MEMORY
  - 支持 CONFIG GET/SET 动态调整

## 中优先级

- [x] **RDB 文件损坏处理优化** (src/storage.rs:load_rdb) ✅
  - 已改进错误处理：魔数/版本不匹配、UTF-8 解析失败等情况现在返回明确错误
  - 添加了详细的错误日志（eprintln! 输出）
  - 返回 io::Error 而非静默忽略，让调用方可以决定如何处理

- [x] **错误信息规范化** (src/command.rs, src/server.rs) ✅
  - 已将 "ERR value exceeds REDUST_MAXVALUE_BYTES" 改为 "ERR value exceeds maximum allowed size"
  - 隐藏了内部环境变量名，使用通用错误描述

## 低优先级（代码质量改进）

- [x] **参数验证逻辑重构** (src/command.rs) ✅
  - 添加辅助函数：require_key, require_i64, require_f64, ensure_no_more_args, collect_keys
  - 添加 try_cmd! 宏简化错误处理
  - 重构了 GET, GETDEL, STRLEN, INCR, DECR, INCRBY, INCRBYFLOAT, DECRBY, DEL, UNLINK, HSTRLEN 等命令

- [x] **Prometheus 指标完善** ✅
  - 新增 redust_used_memory_bytes（内存使用量）
  - 新增 redust_maxmemory_bytes（最大内存限制）
  - 新增 redust_slowlog_entries_total（慢日志条目总数）

## 进行中

- [x] **Hash 命令补全** (src/command.rs, src/server.rs, src/storage.rs) ✅
  - [x] HINCRBY - 对 hash field 做整数自增
  - [x] HINCRBYFLOAT - 对 hash field 做浮点自增
  - [x] HSETNX - 仅当 field 不存在时设置
  - [x] HSTRLEN - 获取 field 值的字符串长度
  - [x] HMGET - 批量获取多个 field
  - [x] HMSET - 批量设置多个 field（已废弃但仍需支持）
  - [x] HKEYS - 获取所有 field 名
  - [x] HVALS - 获取所有 field 值
  - [x] HLEN - 获取 hash 的 field 数量
  - [x] HSCAN - 增量迭代 hash 的 field

- [x] **List 命令补全** (src/command.rs, src/server.rs, src/storage.rs) ✅
  - [x] LSET - 设置指定索引的元素
  - [x] LINSERT - 在指定元素前/后插入
  - [x] RPOPLPUSH - 从源列表弹出并推入目标列表
  - [x] BLPOP - 阻塞式左弹出
  - [x] BRPOP - 阻塞式右弹出
  - 注：BRPOPLPUSH 已废弃，推荐使用 BLMOVE

- [x] **Set 命令补全** ✅
  - [x] SSCAN - 增量迭代集合成员

- [x] **ZSet 命令补全** ✅
  - [x] ZCOUNT - 统计分数范围内的成员数
  - [x] ZINTER / ZINTERSTORE - 交集运算
  - [x] ZUNION / ZUNIONSTORE - 并集运算
  - [x] ZDIFF / ZDIFFSTORE - 差集运算
  - [x] ZPOPMIN / ZPOPMAX - 弹出最小/最大分数成员
  - [x] ZLEXCOUNT - 统计字典序范围内的成员数
  - [x] ZRANK / ZREVRANK - 获取成员排名
  - [x] ZMSCORE - 批量获取分数
  - 待实现：BZPOPMIN/BZPOPMAX（阻塞式）

- [x] **简单命令补全** ✅
  - [x] TIME - 返回服务器时间
  - [x] RANDOMKEY - 随机返回一个 key
