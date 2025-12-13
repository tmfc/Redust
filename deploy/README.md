# 部署指南

## systemd 服务配置

### 安装步骤

1. **编译 Redust**

   ```bash
   cargo build --release
   sudo cp target/release/redust /usr/local/bin/
   ```

2. **创建用户和目录**

   ```bash
   sudo useradd -r -s /sbin/nologin redust
   sudo mkdir -p /var/lib/redust /opt/redust
   sudo chown redust:redust /var/lib/redust
   ```

3. **安装 systemd 服务文件**

   ```bash
   sudo cp deploy/redust.service /etc/systemd/system/
   sudo systemctl daemon-reload
   ```

4. **启动服务**

   ```bash
   sudo systemctl enable redust
   sudo systemctl start redust
   ```

5. **查看状态和日志**

   ```bash
   sudo systemctl status redust
   sudo journalctl -u redust -f
   ```

### 配置说明

编辑 `/etc/systemd/system/redust.service` 修改以下环境变量：

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `REDUST_ADDR` | `0.0.0.0:6379` | 监听地址 |
| `REDUST_RDB_PATH` | `/var/lib/redust/redust.rdb` | RDB 文件路径 |
| `REDUST_RDB_AUTO_SAVE_SECS` | `300` | 自动保存间隔（秒） |
| `REDUST_AUTH_PASSWORD` | 未设置 | 认证密码 |
| `REDUST_MAXMEMORY_BYTES` | 未设置 | 内存限制 |

修改后重新加载：

```bash
sudo systemctl daemon-reload
sudo systemctl restart redust
```

### 卸载

```bash
sudo systemctl stop redust
sudo systemctl disable redust
sudo rm /etc/systemd/system/redust.service
sudo systemctl daemon-reload
```
