//! INFO 命令测试

use std::future;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

async fn spawn_server() -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        redust::serve(listener, future::pending::<()>())
            .await
            .unwrap();
    });

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

    async fn send(&mut self, data: &str) {
        self.writer.write_all(data.as_bytes()).await.unwrap();
    }

    async fn read_line(&mut self) -> String {
        let mut line = String::new();
        self.reader.read_line(&mut line).await.unwrap();
        line
    }

    async fn read_bulk(&mut self) -> String {
        let mut line = String::new();
        self.reader.read_line(&mut line).await.unwrap();
        
        if line.starts_with('$') {
            let len: i64 = line.trim_start_matches('$').trim().parse().unwrap_or(-1);
            if len < 0 {
                return String::new();
            }
            let mut content = vec![0u8; len as usize + 2]; // +2 for \r\n
            tokio::io::AsyncReadExt::read_exact(&mut self.reader, &mut content).await.unwrap();
            String::from_utf8_lossy(&content[..len as usize]).to_string()
        } else {
            line
        }
    }
}

#[tokio::test]
async fn info_returns_all_sections() {
    let port = spawn_server().await;
    let mut client = TestClient::connect(port).await;

    // INFO without section returns all sections
    client.send("*1\r\n$4\r\nINFO\r\n").await;
    let result = client.read_bulk().await;

    // 验证包含各个 section
    assert!(result.contains("# Server"));
    assert!(result.contains("# Memory"));
    assert!(result.contains("# Clients"));
    assert!(result.contains("# Stats"));
    assert!(result.contains("# Persistence"));
    assert!(result.contains("# Replication"));
    assert!(result.contains("# Keyspace"));

    // 验证包含关键字段
    assert!(result.contains("redust_version:"));
    assert!(result.contains("uptime_in_seconds:"));
    assert!(result.contains("connected_clients:"));
    assert!(result.contains("used_memory:"));
}

#[tokio::test]
async fn info_server_section() {
    let port = spawn_server().await;
    let mut client = TestClient::connect(port).await;

    // INFO server
    client.send("*2\r\n$4\r\nINFO\r\n$6\r\nserver\r\n").await;
    let result = client.read_bulk().await;

    // 验证只包含 Server section
    assert!(result.contains("# Server"));
    assert!(result.contains("redust_version:"));
    assert!(result.contains("uptime_in_seconds:"));
    
    // 不应该包含其他 section
    assert!(!result.contains("# Clients"));
    assert!(!result.contains("# Stats"));
}

#[tokio::test]
async fn info_memory_section() {
    let port = spawn_server().await;
    let mut client = TestClient::connect(port).await;

    // INFO memory
    client.send("*2\r\n$4\r\nINFO\r\n$6\r\nmemory\r\n").await;
    let result = client.read_bulk().await;

    // 验证只包含 Memory section
    assert!(result.contains("# Memory"));
    assert!(result.contains("used_memory:"));
    assert!(result.contains("maxmemory:"));
    
    // 不应该包含其他 section
    assert!(!result.contains("# Server"));
    assert!(!result.contains("# Clients"));
}

#[tokio::test]
async fn info_clients_section() {
    let port = spawn_server().await;
    let mut client = TestClient::connect(port).await;

    // INFO clients
    client.send("*2\r\n$4\r\nINFO\r\n$7\r\nclients\r\n").await;
    let result = client.read_bulk().await;

    // 验证只包含 Clients section
    assert!(result.contains("# Clients"));
    assert!(result.contains("connected_clients:"));
    
    // 不应该包含其他 section
    assert!(!result.contains("# Server"));
    assert!(!result.contains("# Memory"));
}

#[tokio::test]
async fn info_stats_section() {
    let port = spawn_server().await;
    let mut client = TestClient::connect(port).await;

    // INFO stats
    client.send("*2\r\n$4\r\nINFO\r\n$5\r\nstats\r\n").await;
    let result = client.read_bulk().await;

    // 验证只包含 Stats section
    assert!(result.contains("# Stats"));
    assert!(result.contains("total_commands_processed:"));
    assert!(result.contains("total_connections_received:"));
    
    // 不应该包含其他 section
    assert!(!result.contains("# Server"));
    assert!(!result.contains("# Memory"));
}

#[tokio::test]
async fn info_keyspace_section() {
    let port = spawn_server().await;
    let mut client = TestClient::connect(port).await;

    // 先设置一些 key
    client.send("*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n").await;
    let _ = client.read_line().await;

    // INFO keyspace
    client.send("*2\r\n$4\r\nINFO\r\n$8\r\nkeyspace\r\n").await;
    let result = client.read_bulk().await;

    // 验证只包含 Keyspace section
    assert!(result.contains("# Keyspace"));
    assert!(result.contains("db0:keys="));
    
    // 不应该包含其他 section
    assert!(!result.contains("# Server"));
    assert!(!result.contains("# Memory"));
}

#[tokio::test]
async fn info_replication_section() {
    let port = spawn_server().await;
    let mut client = TestClient::connect(port).await;

    // INFO replication
    client.send("*2\r\n$4\r\nINFO\r\n$11\r\nreplication\r\n").await;
    let result = client.read_bulk().await;

    // 验证只包含 Replication section
    assert!(result.contains("# Replication"));
    assert!(result.contains("role:master"));
    assert!(result.contains("connected_slaves:0"));
    
    // 不应该包含其他 section
    assert!(!result.contains("# Server"));
    assert!(!result.contains("# Memory"));
}
