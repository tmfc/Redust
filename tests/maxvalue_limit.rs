use std::net::SocketAddr;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

use redust::server::serve;

mod env_guard;
use env_guard::{set_env, ENV_LOCK};

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
}

#[tokio::test]
async fn rejects_value_exceeding_limit() {
    let _lock = ENV_LOCK.lock().unwrap();
    let _guard = set_env("REDUST_MAXVALUE_BYTES", "16");
    // Limit to 16 bytes

    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    let small = "1234567890abcdef"; // 16 bytes
    let large = "1234567890abcdefX"; // 17 bytes

    // small should succeed
    client.send_array(&["SET", "k1", small]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "+OK\r\n");

    // large should be rejected
    client.send_array(&["SET", "k2", large]).await;
    let line = client.read_simple_line().await;
    assert!(line.starts_with("-ERR value exceeds REDUST_MAXVALUE_BYTES"));

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn hincrby_respects_limit() {
    let _lock = ENV_LOCK.lock().unwrap();
    let _guard = set_env("REDUST_MAXVALUE_BYTES", "4");

    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // 先写入一个小整数 9
    client.send_array(&["HSET", "numhash", "f", "9"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, ":1\r\n");

    // 多次 HINCRBY 使结果变为 10000（长度 5），超过 4 字节限制
    client.send_array(&["HINCRBY", "numhash", "f", "9991"]).await;
    let line = client.read_simple_line().await;
    assert!(line.starts_with("-ERR value exceeds REDUST_MAXVALUE_BYTES"));

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn mset_respects_limit() {
    let _lock = ENV_LOCK.lock().unwrap();
    let _guard = set_env("REDUST_MAXVALUE_BYTES", "8");

    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // v1 ok, v2 too large
    client.send_array(&["MSET", "a", "12345678", "b", "123456789"]).await;
    let line = client.read_simple_line().await;
    assert!(line.starts_with("-ERR value exceeds REDUST_MAXVALUE_BYTES"));

    // ensure that failed MSET did not partially write key a
    client.send_array(&["GET", "a"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "$-1\r\n");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}
