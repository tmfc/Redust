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

- [ ] **错误信息规范化** (src/command.rs, src/server.rs)
  - 问题：某些错误响应可能暴露内部实现细节（如环境变量名）
  - 建议：统一错误响应格式，隐藏内部细节
  - 预估：小

## 低优先级（代码质量改进）

- [ ] **参数验证逻辑重构** (src/command.rs)
  - 问题：参数验证逻辑存在重复代码
  - 建议：提取公共验证函数，减少代码重复
  - 预估：中

- [ ] **Prometheus 指标完善**
  - 问题：可能缺少某些关键性能指标
  - 建议：增加命令延迟分布、内存使用详情等指标
  - 预估：小
