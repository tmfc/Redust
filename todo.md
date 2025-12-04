# TODO - 命令补全

> 目标：补全常用 Redis 命令，提升兼容性。

---

## 当前任务：Hash 命令补全

### 已实现（确认）
- [x] HINCRBY - 哈希字段整数自增
- [x] HINCRBYFLOAT - 哈希字段浮点自增
- [x] HKEYS - 获取所有字段名
- [x] HVALS - 获取所有字段值
- [x] HLEN - 获取字段数量
- [x] HMGET - 批量获取字段值
- [x] HSCAN - 增量迭代哈希

### 已完成
- [x] HSETNX - 字段不存在时设置
- [x] HSTRLEN - 获取字段值长度
- [x] HMSET - 批量设置字段（已废弃，但常用）

### List 命令（已完成）
- [x] LTRIM - 裁剪列表
- [x] LSET - 设置指定索引元素
- [x] LINSERT - 在指定元素前/后插入
- [x] RPOPLPUSH - 弹出并推入另一列表
- [x] LPOS - 查找元素位置

### Sorted Set 命令（已完成）
- [x] ZCOUNT - 统计分数范围内的成员数
- [x] ZRANK / ZREVRANK - 获取成员排名
- [x] ZPOPMIN / ZPOPMAX - 弹出最小/最大分数成员
- [x] ZINTER / ZUNION / ZDIFF - 集合运算
- [x] ZINTERSTORE / ZUNIONSTORE / ZDIFFSTORE - 集合运算并存储
- [x] ZLEXCOUNT - 字典序范围计数

### Generic 命令（已完成）
- [x] SCAN - 增量迭代键空间
- [x] COPY - 复制键
- [x] UNLINK - 异步删除（简化实现，与 DEL 相同）
- [x] TOUCH - 更新访问时间
- [x] OBJECT ENCODING - 获取对象编码类型

### Expire 命令（已完成）
- [x] EXPIREAT - 设置绝对过期时间（Unix 秒）
- [x] PEXPIREAT - 设置绝对过期时间（Unix 毫秒）
- [x] EXPIRETIME - 获取绝对过期时间（Unix 秒）
- [x] PEXPIRETIME - 获取绝对过期时间（Unix 毫秒）

---

请选择要实现的命令类别或具体命令。
