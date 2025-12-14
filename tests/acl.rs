//! ACL 命令集成测试

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
