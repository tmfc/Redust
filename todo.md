# TODO (Phase C)

## 运维增强

- [ ] **INFO 命令完善**
  - [ ] 复制状态（replication section）- 待主从复制实现后补充

- [x] **客户端管理**
  - [x] CLIENT UNBLOCK - 解除阻塞

## 安全增强

- [ ] **ACL 访问控制**
  - [ ] 用户管理（ACL SETUSER/DELUSER/LIST）
  - [ ] 命令权限控制
  - [ ] 频道权限控制

- [ ] **TLS 支持**
  - [ ] 内建 TLS 或 stunnel 方案预研

---

## 暂缓 (Phase D)

- [ ] **主从复制**
  - [ ] REPLICAOF/SLAVEOF 命令
  - [ ] 全量同步（RDB 传输）
  - [ ] 增量命令流同步
  - [ ] 只读从库模式
