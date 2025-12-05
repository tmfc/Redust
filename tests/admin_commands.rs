//! Integration tests for admin/ops commands (CONFIG, CLIENT, SLOWLOG)

use std::net::SocketAddr;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

use redust::server::serve;

mod env_guard;
use env_guard::set_env;

async fn spawn_server() -> (
    SocketAddr,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<tokio::io::Result<()>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind server");
    let addr = listener.local_addr().expect("local addr");
    let (tx, rx) = oneshot::channel();
    let handle = tokio::spawn(async move {
        serve(listener, async move {
            let _ = rx.await;
        })
        .await
    });
    (addr, tx, handle)
}

struct TestClient {
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: tokio::net::tcp::OwnedWriteHalf,
}

impl TestClient {
    async fn connect(addr: SocketAddr) -> Self {
        let stream = TcpStream::connect(addr).await.unwrap();
        let (read_half, write_half) = stream.into_split();
        let reader = BufReader::new(read_half);
        TestClient {
            reader,
            writer: write_half,
        }
    }

    async fn send_array(&mut self, parts: &[&str]) {
        let mut buf = String::new();
        buf.push_str(&format!("*{}\r\n", parts.len()));
        for p in parts {
            buf.push_str(&format!("${}\r\n{}\r\n", p.len(), p));
        }
        self.writer.write_all(buf.as_bytes()).await.unwrap();
    }

    async fn read_line(&mut self) -> String {
        let mut line = String::new();
        self.reader.read_line(&mut line).await.unwrap();
        line
    }

    async fn read_bulk_string(&mut self) -> String {
        let len_line = self.read_line().await;
        if len_line.starts_with("$-1") {
            return String::new();
        }
        // Parse length from "$<len>\r\n"
        let len_str = len_line.trim_start_matches('$').trim();
        let len: usize = len_str.parse().unwrap();
        let mut buf = vec![0u8; len + 2]; // +2 for \r\n
        tokio::io::AsyncReadExt::read_exact(&mut self.reader, &mut buf).await.unwrap();
        String::from_utf8_lossy(&buf[..len]).to_string()
    }
}

#[tokio::test]
async fn config_get_returns_values() {
    let _guard = set_env("REDUST_DISABLE_PERSISTENCE", "1");
    
    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // CONFIG GET maxmemory
    client.send_array(&["CONFIG", "GET", "maxmemory"]).await;
    let line = client.read_line().await;
    assert!(line.starts_with("*"), "Expected array response, got: {}", line);
    
    // Read the key-value pair
    let key = client.read_bulk_string().await;
    assert_eq!(key, "maxmemory");
    
    let value = client.read_bulk_string().await;
    assert_eq!(value, "0");

    // CONFIG GET *
    client.send_array(&["CONFIG", "GET", "*"]).await;
    let line = client.read_line().await;
    assert!(line.starts_with("*"), "Expected array response, got: {}", line);

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn config_set_returns_error() {
    let _guard = set_env("REDUST_DISABLE_PERSISTENCE", "1");
    
    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // CONFIG SET maxmemory 100mb - should return error (not supported)
    client.send_array(&["CONFIG", "SET", "maxmemory", "100mb"]).await;
    let line = client.read_line().await;
    assert!(line.starts_with("-ERR"), "Expected error response, got: {}", line);

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn client_id_returns_integer() {
    let _guard = set_env("REDUST_DISABLE_PERSISTENCE", "1");
    
    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // CLIENT ID
    client.send_array(&["CLIENT", "ID"]).await;
    let line = client.read_line().await;
    assert!(line.starts_with(":"), "Expected integer response, got: {}", line);
    let id: i64 = line[1..].trim().parse().unwrap();
    assert!(id > 0, "Client ID should be positive");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn client_setname_getname_roundtrip() {
    let _guard = set_env("REDUST_DISABLE_PERSISTENCE", "1");
    
    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // CLIENT GETNAME - should be null initially
    client.send_array(&["CLIENT", "GETNAME"]).await;
    let line = client.read_line().await;
    assert!(line.starts_with("$-1"), "Expected null bulk string, got: {}", line);

    // CLIENT SETNAME myconn
    client.send_array(&["CLIENT", "SETNAME", "myconn"]).await;
    let line = client.read_line().await;
    assert_eq!(line, "+OK\r\n");

    // CLIENT GETNAME - should return "myconn"
    client.send_array(&["CLIENT", "GETNAME"]).await;
    let name = client.read_bulk_string().await;
    assert_eq!(name, "myconn");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn client_list_returns_info() {
    let _guard = set_env("REDUST_DISABLE_PERSISTENCE", "1");
    
    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // CLIENT LIST
    client.send_array(&["CLIENT", "LIST"]).await;
    let info = client.read_bulk_string().await;
    assert!(info.contains("id="), "Expected client info with id, got: {}", info);
    assert!(info.contains("addr="), "Expected client info with addr, got: {}", info);

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn slowlog_commands_work() {
    let _guard = set_env("REDUST_DISABLE_PERSISTENCE", "1");
    
    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // SLOWLOG RESET 先清空
    client.send_array(&["SLOWLOG", "RESET"]).await;
    let line = client.read_line().await;
    assert_eq!(line, "+OK\r\n");

    // SLOWLOG LEN
    client.send_array(&["SLOWLOG", "LEN"]).await;
    let line = client.read_line().await;
    assert!(line.starts_with(":"), "Expected integer response, got: {}", line);

    // SLOWLOG GET - 返回空数组
    client.send_array(&["SLOWLOG", "GET"]).await;
    let line = client.read_line().await;
    assert!(line.starts_with("*"), "Expected array response, got: {}", line);

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn slowlog_records_slow_commands() {
    // 设置非常低的阈值（1微秒）以确保命令被记录
    let _guard1 = set_env("REDUST_DISABLE_PERSISTENCE", "1");
    let _guard2 = set_env("REDUST_SLOWLOG_SLOWER_THAN", "1");
    
    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // 先重置慢日志
    client.send_array(&["SLOWLOG", "RESET"]).await;
    let _ = client.read_line().await;

    // 获取重置后的长度（SLOWLOG RESET 本身也会被记录）
    client.send_array(&["SLOWLOG", "LEN"]).await;
    let line = client.read_line().await;
    let initial_len: i64 = line[1..].trim().parse().unwrap();

    // 执行一些命令
    client.send_array(&["SET", "foo", "bar"]).await;
    let _ = client.read_line().await;
    
    client.send_array(&["GET", "foo"]).await;
    let _ = client.read_line().await;
    let _ = client.read_line().await; // bulk string value

    // 检查慢日志长度增加了
    client.send_array(&["SLOWLOG", "LEN"]).await;
    let line = client.read_line().await;
    let new_len: i64 = line[1..].trim().parse().unwrap();
    assert!(new_len > initial_len, "Expected slow log to grow, initial: {}, new: {}", initial_len, new_len);

    // 重置后长度应该为 0（或只有 RESET 命令本身）
    client.send_array(&["SLOWLOG", "RESET"]).await;
    let _ = client.read_line().await;
    
    client.send_array(&["SLOWLOG", "LEN"]).await;
    let line = client.read_line().await;
    let len: i64 = line[1..].trim().parse().unwrap();
    // SLOWLOG RESET 本身也会被记录，所以可能是 1
    assert!(len <= 1, "Expected 0 or 1 entries after reset, got: {}", len);

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}
