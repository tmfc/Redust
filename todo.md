# TODO (Phase C)

## 数据结构扩展

- [x] **Bitmaps 位图操作**
  - [x] SETBIT - 设置指定位
  - [x] GETBIT - 获取指定位
  - [x] BITCOUNT - 统计置位数量
  - [x] BITOP - 位运算（AND/OR/XOR/NOT）
  - [x] BITPOS - 查找第一个 0 或 1 的位置
  - [x] BITFIELD - 位域操作

- [ ] **Streams 消息队列**
  - [x] XADD - 添加消息
  - [x] XREAD - 读取消息
  - [x] XRANGE/XREVRANGE - 范围查询
  - [x] XLEN - 获取流长度
  - [x] XINFO - 流信息查询
  - [x] XGROUP - 消费者组管理 (CREATE/SETID/DESTROY/CREATECONSUMER/DELCONSUMER)
  - [x] XREADGROUP - 消费者组读取
  - [x] XACK - 消息确认
  - [x] XPENDING - 待处理消息查询
  - [ ] XCLAIM - 消息转移

- [ ] **Geo 地理位置**
  - [x] GEOADD - 添加地理位置
  - [x] GEODIST - 计算距离
  - [x] GEOHASH - 获取 geohash
  - [x] GEOPOS - 获取坐标
  - [ ] GEOSEARCH/GEORADIUS - 范围搜索

## 主从复制

- [ ] **基础复制功能**
  - [ ] REPLICAOF/SLAVEOF 命令
  - [ ] 全量同步（RDB 传输）
  - [ ] 增量命令流同步
  - [ ] 只读从库模式

## 运维增强

- [ ] **INFO 命令完善**
  - [ ] 内存统计（used_memory_*）
  - [ ] 复制状态（replication section）
  - [ ] Pub/Sub 统计

- [ ] **客户端管理**
  - [ ] CLIENT PAUSE - 暂停客户端
  - [ ] CLIENT UNBLOCK - 解除阻塞

- [ ] **部署支持**
  - [ ] Docker 镜像
  - [ ] systemd 配置示例

## 安全增强

- [ ] **ACL 访问控制**
  - [ ] 用户管理（ACL SETUSER/DELUSER/LIST）
  - [ ] 命令权限控制
  - [ ] 频道权限控制

- [ ] **TLS 支持**
  - [ ] 内建 TLS 或 stunnel 方案预研
