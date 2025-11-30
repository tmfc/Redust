use std::net::SocketAddr;
use std::env;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

use redust::server::serve;

async fn spawn_server(
) -> (SocketAddr, oneshot::Sender<()>, tokio::task::JoinHandle<tokio::io::Result<()>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind server");
    let addr = listener.local_addr().expect("local addr");
    let (tx, rx) = oneshot::channel();
    let handle = tokio::spawn(async move {
        serve(
            listener,
            async move {
                let _ = rx.await;
            },
        )
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
        TestClient { reader, writer: write_half }
    }

    async fn send_array(&mut self, parts: &[&str]) {
        let mut buf = String::new();
        buf.push_str(&format!("*{}\r\n", parts.len()));
        for p in parts {
            buf.push_str(&format!("${}\r\n{}\r\n", p.len(), p));
        }
        self.writer.write_all(buf.as_bytes()).await.unwrap();
    }

    async fn read_simple_line(&mut self) -> String {
        let mut line = String::new();
        self.reader.read_line(&mut line).await.unwrap();
        line
    }

    async fn set(&mut self, key: &str, value: &str) {
        self.send_array(&["SET", key, value]).await;
        let line = self.read_simple_line().await;
        assert_eq!(line, "+OK\r\n");
    }

    async fn dbsize(&mut self) -> i64 {
        self.send_array(&["DBSIZE"]).await;
        let line = self.read_simple_line().await;
        assert!(line.starts_with(":"));
        line[1..line.len() - 2].parse().unwrap()
    }
}

#[tokio::test]
async fn basic_maxmemory_eviction_smoke() {
    // 设置较小的 maxmemory（64 KiB），使用纯字节值以匹配当前解析逻辑
    env::set_var("REDUST_MAXMEMORY_BYTES", "65536");

    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    let large_value = "x".repeat(1024); // 1KB 左右的 value
    let total_keys = 2000;

    for i in 0..total_keys {
        let key = format!("hot_k{}", i);
        client.set(&key, &large_value).await;
    }

    let final_dbsize = client.dbsize().await;

    // 只要小于写入总数，就说明发生了淘汰（否则等于 total_keys）
    assert!(final_dbsize < total_keys as i64);

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}
