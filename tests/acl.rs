//! ACL 命令集成测试

mod env_guard;

use env_guard::set_env;
use std::future;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::timeout;

async fn spawn_server() -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        redust::serve(listener, future::pending::<()>())
            .await
            .unwrap();
    });

    // 等待服务器启动
    tokio::time::sleep(Duration::from_millis(50)).await;
    port
}

struct TestClient {
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: tokio::net::tcp::OwnedWriteHalf,
}

impl TestClient {
    async fn connect(port: u16) -> Self {
        let stream = TcpStream::connect(format!("127.0.0.1:{}", port))
            .await
            .unwrap();
        let (read_half, write_half) = stream.into_split();
        Self {
            reader: BufReader::new(read_half),
            writer: write_half,
        }
    }

    async fn send(&mut self, cmd: &str) {
        self.writer.write_all(cmd.as_bytes()).await.unwrap();
    }

    async fn read_line(&mut self) -> String {
        let mut line = String::new();
        timeout(Duration::from_secs(2), self.reader.read_line(&mut line))
            .await
            .expect("read timeout")
            .expect("read error");
        line
    }

    async fn read_bulk(&mut self) -> String {
        let header = self.read_line().await;
        if header.starts_with('$') {
            let len: i64 = header[1..].trim().parse().unwrap();
            if len < 0 {
                return String::new();
            }
            let mut buf = vec![0u8; len as usize + 2]; // +2 for \r\n
            tokio::io::AsyncReadExt::read_exact(&mut self.reader, &mut buf)
                .await
                .unwrap();
            String::from_utf8_lossy(&buf[..len as usize]).to_string()
        } else {
            header
        }
    }

    async fn read_array(&mut self) -> Vec<String> {
        let header = self.read_line().await;
        if !header.starts_with('*') {
            return vec![header];
        }
        let count: i64 = header[1..].trim().parse().unwrap();
        if count < 0 {
            return vec![];
        }
        let mut result = Vec::new();
        for _ in 0..count {
            result.push(self.read_bulk().await);
        }
        result
    }
}

#[tokio::test]
async fn acl_whoami_returns_default() {
    let port = spawn_server().await;
    let mut client = TestClient::connect(port).await;

    // ACL WHOAMI 应该返回 "default"
    client.send("*2\r\n$3\r\nACL\r\n$6\r\nWHOAMI\r\n").await;
    let result = client.read_bulk().await;
    assert_eq!(result, "default");
}

#[tokio::test]
async fn acl_users_returns_default() {
    let port = spawn_server().await;
    let mut client = TestClient::connect(port).await;

    // ACL USERS 应该至少包含 "default"
    client.send("*2\r\n$3\r\nACL\r\n$5\r\nUSERS\r\n").await;
    let result = client.read_array().await;
    assert!(result.contains(&"default".to_string()));
}

#[tokio::test]
async fn acl_list_returns_default_user() {
    let port = spawn_server().await;
    let mut client = TestClient::connect(port).await;

    // ACL LIST 应该返回默认用户信息
    client.send("*2\r\n$3\r\nACL\r\n$4\r\nLIST\r\n").await;
    let result = client.read_array().await;
    assert!(!result.is_empty());
    // 默认用户应该包含 "user default"
    assert!(result.iter().any(|s| s.contains("user default")));
}

#[tokio::test]
async fn acl_setuser_creates_user() {
    let port = spawn_server().await;
    let mut client = TestClient::connect(port).await;

    // ACL SETUSER alice on >password ~* +@all
    client
        .send("*7\r\n$3\r\nACL\r\n$7\r\nSETUSER\r\n$5\r\nalice\r\n$2\r\non\r\n$9\r\n>password\r\n$2\r\n~*\r\n$5\r\n+@all\r\n")
        .await;
    let result = client.read_line().await;
    assert!(result.starts_with("+OK"));

    // 验证用户已创建
    client.send("*2\r\n$3\r\nACL\r\n$5\r\nUSERS\r\n").await;
    let users = client.read_array().await;
    assert!(users.contains(&"alice".to_string()));
}

#[tokio::test]
async fn acl_deluser_removes_user() {
    let port = spawn_server().await;
    let mut client = TestClient::connect(port).await;

    // 先创建用户
    client
        .send("*4\r\n$3\r\nACL\r\n$7\r\nSETUSER\r\n$3\r\nbob\r\n$2\r\non\r\n")
        .await;
    let _ = client.read_line().await;

    // 删除用户
    client
        .send("*3\r\n$3\r\nACL\r\n$7\r\nDELUSER\r\n$3\r\nbob\r\n")
        .await;
    let result = client.read_line().await;
    assert!(result.starts_with(":1")); // 删除了 1 个用户

    // 验证用户已删除
    client.send("*2\r\n$3\r\nACL\r\n$5\r\nUSERS\r\n").await;
    let users = client.read_array().await;
    assert!(!users.contains(&"bob".to_string()));
}

#[tokio::test]
async fn acl_deluser_cannot_delete_default() {
    let port = spawn_server().await;
    let mut client = TestClient::connect(port).await;

    // 尝试删除默认用户
    client
        .send("*3\r\n$3\r\nACL\r\n$7\r\nDELUSER\r\n$7\r\ndefault\r\n")
        .await;
    let result = client.read_line().await;
    assert!(result.starts_with(":0")); // 删除了 0 个用户

    // 默认用户仍然存在
    client.send("*2\r\n$3\r\nACL\r\n$5\r\nUSERS\r\n").await;
    let users = client.read_array().await;
    assert!(users.contains(&"default".to_string()));
}

#[tokio::test]
async fn acl_getuser_returns_user_info() {
    let port = spawn_server().await;
    let mut client = TestClient::connect(port).await;

    // 获取默认用户信息
    client
        .send("*3\r\n$3\r\nACL\r\n$7\r\nGETUSER\r\n$7\r\ndefault\r\n")
        .await;
    let result = client.read_array().await;
    
    // 应该包含 flags, passwords, commands, keys, channels
    assert!(result.contains(&"flags".to_string()));
    assert!(result.contains(&"passwords".to_string()));
    assert!(result.contains(&"commands".to_string()));
    assert!(result.contains(&"keys".to_string()));
    assert!(result.contains(&"channels".to_string()));
}

#[tokio::test]
async fn acl_getuser_nonexistent_returns_null() {
    let port = spawn_server().await;
    let mut client = TestClient::connect(port).await;

    // 获取不存在的用户
    client
        .send("*3\r\n$3\r\nACL\r\n$7\r\nGETUSER\r\n$11\r\nnonexistent\r\n")
        .await;
    let result = client.read_line().await;
    assert!(result.starts_with("*-1")); // null array
}

#[tokio::test]
async fn acl_cat_returns_categories() {
    let port = spawn_server().await;
    let mut client = TestClient::connect(port).await;

    // ACL CAT 返回所有类别
    client.send("*2\r\n$3\r\nACL\r\n$3\r\nCAT\r\n").await;
    let result = client.read_array().await;
    
    // 应该包含常见类别
    assert!(result.contains(&"string".to_string()));
    assert!(result.contains(&"list".to_string()));
    assert!(result.contains(&"set".to_string()));
    assert!(result.contains(&"hash".to_string()));
    assert!(result.contains(&"sortedset".to_string()));
    assert!(result.contains(&"pubsub".to_string()));
}

#[tokio::test]
async fn auth_with_acl_user() {
    let port = spawn_server().await;
    let mut client = TestClient::connect(port).await;

    // 创建一个需要密码的用户
    client
        .send("*6\r\n$3\r\nACL\r\n$7\r\nSETUSER\r\n$5\r\nalice\r\n$2\r\non\r\n$7\r\n>secret\r\n$5\r\n+@all\r\n")
        .await;
    let result = client.read_line().await;
    assert!(result.starts_with("+OK"));

    // 使用 AUTH username password 格式认证
    client
        .send("*3\r\n$4\r\nAUTH\r\n$5\r\nalice\r\n$6\r\nsecret\r\n")
        .await;
    let result = client.read_line().await;
    assert!(result.starts_with("+OK"));

    // 验证当前用户已切换
    client.send("*2\r\n$3\r\nACL\r\n$6\r\nWHOAMI\r\n").await;
    let result = client.read_bulk().await;
    assert_eq!(result, "alice");
}

#[tokio::test]
async fn auth_with_wrong_password() {
    let port = spawn_server().await;
    let mut client = TestClient::connect(port).await;

    // 创建一个需要密码的用户
    client
        .send("*6\r\n$3\r\nACL\r\n$7\r\nSETUSER\r\n$3\r\nbob\r\n$2\r\non\r\n$8\r\n>mypass\r\n$5\r\n+@all\r\n")
        .await;
    let _ = client.read_line().await;

    // 需要用新连接来测试认证（ACL 是全局共享的）
    let mut client2 = TestClient::connect(port).await;
    
    // 使用错误密码认证
    client2
        .send("*3\r\n$4\r\nAUTH\r\n$3\r\nbob\r\n$5\r\nwrong\r\n")
        .await;
    let result = client2.read_line().await;
    assert!(result.contains("WRONGPASS") || result.contains("invalid"));
}

#[tokio::test]
async fn auth_single_password_format() {
    let port = spawn_server().await;
    let mut client = TestClient::connect(port).await;

    // 默认用户是 nopass，所以单密码格式的 AUTH 应该成功
    // AUTH password 格式（使用 default 用户）
    client
        .send("*2\r\n$4\r\nAUTH\r\n$8\r\nanything\r\n")
        .await;
    let result = client.read_line().await;
    // 默认用户是 nopass，任何密码都应该成功
    assert!(result.starts_with("+OK"));

    // 验证当前用户是 default
    client.send("*2\r\n$3\r\nACL\r\n$6\r\nWHOAMI\r\n").await;
    let result = client.read_bulk().await;
    assert_eq!(result, "default");
}

#[tokio::test]
async fn acl_command_permission_check() {
    let port = spawn_server().await;
    let mut client = TestClient::connect(port).await;

    // 创建一个只允许 GET 命令的用户
    client
        .send("*7\r\n$3\r\nACL\r\n$7\r\nSETUSER\r\n$8\r\nreadonly\r\n$2\r\non\r\n$6\r\nnopass\r\n$4\r\n+get\r\n$5\r\n~foo*\r\n")
        .await;
    let _ = client.read_line().await;

    // 用新连接以 readonly 用户登录
    let mut client2 = TestClient::connect(port).await;
    client2
        .send("*3\r\n$4\r\nAUTH\r\n$8\r\nreadonly\r\n$3\r\nany\r\n")
        .await;
    let result = client2.read_line().await;
    assert!(result.starts_with("+OK"));

    // GET 命令应该被允许（在允许的 key 上）
    // 先用 default 用户设置一个 key
    client.send("*3\r\n$3\r\nSET\r\n$4\r\nfoo1\r\n$3\r\nbar\r\n").await;
    let _ = client.read_line().await;

    client2.send("*2\r\n$3\r\nGET\r\n$4\r\nfoo1\r\n").await;
    let _ = client2.read_line().await; // $3
    let result = client2.read_line().await;
    assert_eq!(result.trim(), "bar");

    // SET 命令应该被拒绝
    client2.send("*3\r\n$3\r\nSET\r\n$4\r\nfoo2\r\n$3\r\nbaz\r\n").await;
    let result = client2.read_line().await;
    assert!(result.contains("NOPERM"));
}

#[tokio::test]
async fn acl_key_permission_check() {
    let port = spawn_server().await;
    let mut client = TestClient::connect(port).await;

    // 创建一个只允许访问 user:* 模式 key 的用户
    client
        .send("*7\r\n$3\r\nACL\r\n$7\r\nSETUSER\r\n$8\r\nuseronly\r\n$2\r\non\r\n$6\r\nnopass\r\n$5\r\n+@all\r\n$7\r\n~user:*\r\n")
        .await;
    let _ = client.read_line().await;

    // 用新连接以 useronly 用户登录
    let mut client2 = TestClient::connect(port).await;
    client2
        .send("*3\r\n$4\r\nAUTH\r\n$8\r\nuseronly\r\n$3\r\nany\r\n")
        .await;
    let result = client2.read_line().await;
    assert!(result.starts_with("+OK"));

    // 访问 user:123 应该被允许
    client2.send("*3\r\n$3\r\nSET\r\n$8\r\nuser:123\r\n$5\r\nhello\r\n").await;
    let result = client2.read_line().await;
    assert!(result.starts_with("+OK"));

    // 访问 admin:123 应该被拒绝
    client2.send("*3\r\n$3\r\nSET\r\n$9\r\nadmin:123\r\n$5\r\nworld\r\n").await;
    let result = client2.read_line().await;
    assert!(result.contains("NOPERM"));
}

#[tokio::test]
#[serial_test::serial]
async fn acl_save_and_load() {
    // 使用临时文件路径，避免污染项目根目录
    let temp_dir = std::env::temp_dir();
    let acl_file = temp_dir.join(format!("redust_test_{}.acl", std::process::id()));
    let _guard = set_env("REDUST_ACL_FILE", acl_file.to_str().unwrap());
    
    let port = spawn_server().await;
    let mut client = TestClient::connect(port).await;

    // 创建一个测试用户
    client
        .send("*6\r\n$3\r\nACL\r\n$7\r\nSETUSER\r\n$8\r\ntestuser\r\n$2\r\non\r\n$6\r\nnopass\r\n$5\r\n+@all\r\n")
        .await;
    let _ = client.read_line().await;

    // ACL SAVE 应该成功
    client.send("*2\r\n$3\r\nACL\r\n$4\r\nSAVE\r\n").await;
    let result = client.read_line().await;
    assert!(result.starts_with("+OK"));

    // 删除用户
    client.send("*3\r\n$3\r\nACL\r\n$7\r\nDELUSER\r\n$8\r\ntestuser\r\n").await;
    let _ = client.read_line().await;

    // 验证用户已删除
    client.send("*2\r\n$3\r\nACL\r\n$5\r\nUSERS\r\n").await;
    let users = client.read_array().await;
    assert!(!users.contains(&"testuser".to_string()));

    // ACL LOAD 应该恢复用户
    client.send("*2\r\n$3\r\nACL\r\n$4\r\nLOAD\r\n").await;
    let result = client.read_line().await;
    assert!(result.starts_with("+OK"));

    // 验证用户已恢复
    client.send("*2\r\n$3\r\nACL\r\n$5\r\nUSERS\r\n").await;
    let users = client.read_array().await;
    assert!(users.contains(&"testuser".to_string()));
    
    // 清理临时文件
    let _ = std::fs::remove_file(&acl_file);
}
