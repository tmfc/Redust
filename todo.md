# TODO

> 当前无待办任务。高优先级功能已全部完成。

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
