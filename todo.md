# TODO - Phase B: HyperLogLog 支持

> 目标：实现 Redis 兼容的 HyperLogLog 基数统计功能，支持 PFADD/PFCOUNT/PFMERGE 三个核心命令。

## 1. HyperLogLog 算法研究与设计 📚

- [x] **算法原理学习**
  - HyperLogLog 基本原理（基数估计、哈希分桶）
  - Redis 实现细节（16384 个寄存器，6 位精度）
  - 误差率分析（标准误差约 0.81%）
  - 内存占用（每个 HLL 约 12KB）

- [x] **数据结构设计**
  - 定义 `HyperLogLog` 结构体
  - 16384 个 6-bit 寄存器的存储方案
  - 稀疏表示优化（小基数时节省内存）——未实现，留作未来优化
  - 与现有 `StorageValue` 枚举集成

- [x] **哈希函数选择**
  - 使用 Rust 标准库 DefaultHasher（基于 SipHash）
  - 64-bit 哈希值分解：前 14 位做索引，后 50 位计算前导零

## 2. 存储层实现 🔧

- [x] **HyperLogLog 核心算法**
  - `src/hyperloglog.rs` 新建模块
  - `HyperLogLog::new()` - 创建空 HLL
  - `HyperLogLog::add(&mut self, element: &[u8])` - 添加元素
  - `HyperLogLog::count(&self) -> u64` - 估算基数
  - `HyperLogLog::merge(&mut self, other: &HyperLogLog)` - 合并 HLL
  - 稀疏/密集表示自动转换——未实现，留作未来优化

- [x] **Storage 集成**
  - 在 `StorageValue` 枚举中添加 `HyperLogLog` 变体
  - `storage.pfadd(key, elements)` - 添加元素到 HLL
  - `storage.pfcount(keys)` - 统计单个或多个 HLL 的基数
  - `storage.pfmerge(dest, sources)` - 合并多个 HLL

- [x] **类型检查与错误处理**
  - WRONGTYPE 错误（操作非 HLL 键）
  - 空键处理（返回 0）
  - 多键合并时的类型校验

## 3. 命令层实现 ⚙️

- [x] **Command 枚举扩展**
  - `src/command.rs` 添加 `Pfadd`, `Pfcount`, `Pfmerge` 变体
  - RESP 协议解析（支持多参数）
  - 参数校验（最少参数数量）

- [x] **命令处理逻辑**
  - `src/server.rs` 中添加命令分发
  - **PFADD key element [element ...]**
    - 返回 0（未改变）或 1（已改变）
    - 支持批量添加
  - **PFCOUNT key [key ...]**
    - 单键：返回估算基数
    - 多键：临时合并后返回并集基数
  - **PFMERGE destkey sourcekey [sourcekey ...]**
    - 合并多个 HLL 到目标键
    - 返回 +OK

- [x] **WATCH 集成**
  - PFADD/PFMERGE 触发键版本更新
  - 事务中的 HLL 操作正确性

## 4. 持久化支持 💾

- [x] **RDB 序列化**
  - `src/storage.rs` 添加 HLL 类型标记 (type_byte = 5)
  - 序列化 16384 个寄存器
  - 反序列化并恢复 HLL 状态

- [ ] **AOF 记录**（未实现，留作未来优化）
  - PFADD/PFMERGE 命令记录到 AOF
  - 启动时正确重放 HLL 操作

## 5. 测试覆盖 ✅

- [x] **单元测试** (`src/hyperloglog.rs`)
  - 基本添加与计数
  - 基数估算精度（与真实基数对比）
  - 合并操作正确性
  - 边界情况（空 HLL、大量元素）

- [x] **集成测试** (`tests/hyperloglog.rs`)
  - PFADD 返回值正确性
  - PFCOUNT 单键/多键场景
  - PFMERGE 合并逻辑
  - WRONGTYPE 错误处理
  - 与其他数据类型混合操作

- [ ] **性能测试**
  - 大量元素添加性能
  - 内存占用验证（约 12KB）
  - 多键合并性能

- [ ] **持久化测试**
  - RDB 保存与加载
  - AOF 重放正确性
  - 重启后基数一致性

## 6. 文档更新 📝

- [x] **command.md**
  - 添加 HyperLogLog 章节
  - 标记 PFADD/PFCOUNT/PFMERGE 为已完成
  - 说明误差率和内存占用

- [ ] **README.md**
  - 更新支持的数据结构列表
  - 添加 HyperLogLog 使用示例

- [ ] **future.md**
  - 记录 HyperLogLog 完成状态
  - 列出可能的优化方向（稀疏表示优化等）

## 实现顺序建议

1. **第一步**：算法研究与数据结构设计（1-2 天）
2. **第二步**：核心算法实现与单元测试（2-3 天）
3. **第三步**：Storage 集成与命令层（1-2 天）
4. **第四步**：持久化支持（1 天）
5. **第五步**：集成测试与文档（1 天）

**预计总时间**: 6-9 天

---

## 技术参考

- [Redis HyperLogLog 实现](https://redis.io/docs/data-types/probabilistic/hyperloglogs/)
- [HyperLogLog 论文](http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf)
- [Redis 源码 hyperloglog.c](https://github.com/redis/redis/blob/unstable/src/hyperloglog.c)
