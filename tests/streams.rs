use std::net::SocketAddr;

use redust::server::serve;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::oneshot;

async fn spawn_server() -> (
    SocketAddr,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<tokio::io::Result<()>>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = oneshot::channel();
    let shutdown = async move {
        let _ = rx.await;
    };
    let handle = tokio::spawn(async move { serve(listener, shutdown).await });
    (addr, tx, handle)
}

struct TestClient {
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: tokio::net::tcp::OwnedWriteHalf,
}

impl TestClient {
    async fn connect(addr: SocketAddr) -> Self {
        let stream = TcpStream::connect(addr).await.expect("Failed to connect");
        let (read_half, write_half) = stream.into_split();
        Self {
            reader: BufReader::new(read_half),
            writer: write_half,
        }
    }

    async fn send_command(&mut self, args: &[&str]) {
        let mut cmd = format!("*{}\r\n", args.len());
        for arg in args {
            cmd.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
        }
        self.writer.write_all(cmd.as_bytes()).await.unwrap();
    }

    async fn read_integer(&mut self) -> i64 {
        let mut line = String::new();
        self.reader.read_line(&mut line).await.unwrap();
        assert!(line.starts_with(':'), "Expected integer, got: {}", line);
        line.trim_start_matches(':').trim().parse().unwrap()
    }

    async fn read_simple_string(&mut self) -> String {
        let mut line = String::new();
        self.reader.read_line(&mut line).await.unwrap();
        assert!(line.starts_with('+'), "Expected simple string, got: {}", line);
        line.trim_start_matches('+').trim().to_string()
    }

    async fn read_error(&mut self) -> String {
        let mut line = String::new();
        self.reader.read_line(&mut line).await.unwrap();
        assert!(line.starts_with('-'), "Expected error, got: {}", line);
        line.trim_start_matches('-').trim().to_string()
    }

    async fn read_bulk_string(&mut self) -> Option<String> {
        let mut line = String::new();
        self.reader.read_line(&mut line).await.unwrap();
        if line.starts_with("$-1") {
            return None;
        }
        assert!(line.starts_with('$'), "Expected bulk string, got: {}", line);
        let len: usize = line.trim_start_matches('$').trim().parse().unwrap();
        let mut buf = vec![0u8; len + 2];
        self.reader.read_exact(&mut buf).await.unwrap();
        Some(String::from_utf8_lossy(&buf[..len]).to_string())
    }

    async fn read_array_len(&mut self) -> usize {
        let mut line = String::new();
        self.reader.read_line(&mut line).await.unwrap();
        assert!(line.starts_with('*'), "Expected array, got: {}", line);
        line.trim_start_matches('*').trim().parse().unwrap()
    }
}

#[tokio::test]
async fn test_xadd_xlen_xrange_basic() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // XLEN on missing key
    client.send_command(&["XLEN", "mystream"]).await;
    assert_eq!(client.read_integer().await, 0);

    // XADD *
    client
        .send_command(&["XADD", "mystream", "*", "f1", "v1", "f2", "v2"])
        .await;
    let id1 = client.read_bulk_string().await.unwrap();
    assert!(id1.contains('-'));

    // XLEN == 1
    client.send_command(&["XLEN", "mystream"]).await;
    assert_eq!(client.read_integer().await, 1);

    // XADD explicit id should succeed if greater
    client
        .send_command(&["XADD", "mystream", "9999999999999-0", "a", "b"])
        .await;
    let id2 = client.read_bulk_string().await.unwrap();
    assert_eq!(id2, "9999999999999-0");

    // XRANGE - +
    client.send_command(&["XRANGE", "mystream", "-", "+"]).await;
    let n = client.read_array_len().await;
    assert_eq!(n, 2);

    // entry 1: [id, [field,value...]]
    let _entry1_len = client.read_array_len().await;
    let _ = client.read_bulk_string().await.unwrap();
    let fv_len = client.read_array_len().await;
    assert_eq!(fv_len, 4);
    let _ = client.read_bulk_string().await.unwrap();
    let _ = client.read_bulk_string().await.unwrap();
    let _ = client.read_bulk_string().await.unwrap();
    let _ = client.read_bulk_string().await.unwrap();

    // entry 2
    let _entry2_len = client.read_array_len().await;
    let _ = client.read_bulk_string().await.unwrap();
    let fv_len = client.read_array_len().await;
    assert_eq!(fv_len, 2);
    let _ = client.read_bulk_string().await.unwrap();
    let _ = client.read_bulk_string().await.unwrap();

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_xadd_id_must_be_increasing() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    client
        .send_command(&["XADD", "mystream", "1-0", "f", "v"])
        .await;
    let id1 = client.read_bulk_string().await.unwrap();
    assert_eq!(id1, "1-0");

    // same id -> error
    client
        .send_command(&["XADD", "mystream", "1-0", "f", "v"])
        .await;
    let err = client.read_error().await;
    assert!(err.contains("ID") || err.contains("smaller"));

    // smaller id -> error
    client
        .send_command(&["XADD", "mystream", "0-1", "f", "v"])
        .await;
    let err = client.read_error().await;
    assert!(err.contains("ID") || err.contains("smaller"));

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_xrange_count() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    for i in 0..3 {
        client
            .send_command(&["XADD", "mystream", "*", "f", &format!("v{}", i)])
            .await;
        let _ = client.read_bulk_string().await.unwrap();
    }

    client
        .send_command(&["XRANGE", "mystream", "-", "+", "COUNT", "2"])
        .await;
    let n = client.read_array_len().await;
    assert_eq!(n, 2);

    let _ = shutdown.send(());
}
