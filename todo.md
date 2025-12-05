# TODO

## 高优先级（小改动，高收益）

- [x] **SCAN TYPE 选项** ✅
  - `SCAN cursor TYPE string|list|set|hash|zset` 按类型过滤
  - 预估：小（已完成）

- [x] **SLOWLOG 实际实现** ✅
  - 记录执行时间超过阈值的命令
  - 维护固定大小的慢日志队列
  - 完善 SLOWLOG GET/RESET/LEN 语义
  - 环境变量：REDUST_SLOWLOG_SLOWER_THAN（微秒）、REDUST_SLOWLOG_MAX_LEN

- [x] **HSCAN/ZSCAN NOVALUES 选项** ✅
  - 仅返回 field/member 不返回 value，减少网络开销
  - 预估：小（已完成）

## 中优先级（中等改动）

- [x] **事务中更多命令支持** ✅
  - 支持 TYPE/KEYS/SCAN 等命令在事务中执行
  - 预估：中（已完成）

- [x] **CONFIG 动态配置支持** ✅
  - 支持运行时修改 maxmemory、timeout、slowlog-log-slower-than 等配置
  - 预估：中（已完成）

- [x] **CLIENT 命令扩展** ✅
  - CLIENT PAUSE/UNPAUSE（全局暂停/恢复）
  - 预估：中（已完成）

## 低优先级（较大改动或探索性）

- [ ] **SCAN 游标稳定性优化**
  - 改进游标机制，避免并发写入时的重复/遗漏
  - 预估：大

- [ ] **事务中 Lua 脚本支持**
  - 支持 EVAL/EVALSHA 在 MULTI 中执行
  - 预估：大

- [ ] **集合性能优化**
  - SUNION/SINTER/SDIFF 大集合场景 Profiling 与优化
  - 预估：中

---

## 已完成（2025-12）

### 高优先级功能
- ✅ Set 命令补全：SMOVE（SPOP/SRANDMEMBER 已存在）
- ✅ 阻塞列表命令：BLPOP、BRPOP（轮询实现）
- ✅ HyperLogLog 稀疏表示优化：节省 90%+ 内存（小基数场景）

### 模式匹配增强
- ✅ `[^abc]` 取反字符集支持（KEYS/SCAN 命令）

### 命令补全批次
- ✅ Hash 命令：HSETNX、HSTRLEN、HMSET
- ✅ List 命令：LTRIM、LSET、LINSERT、RPOPLPUSH、LPOS
- ✅ Sorted Set 命令：ZCOUNT、ZRANK/ZREVRANK、ZPOPMIN/ZPOPMAX、ZINTER/ZUNION/ZDIFF 及 STORE 变体、ZLEXCOUNT
- ✅ Generic 命令：COPY、UNLINK、TOUCH、OBJECT ENCODING
- ✅ Expire 命令：EXPIREAT、PEXPIREAT、EXPIRETIME、PEXPIRETIME
