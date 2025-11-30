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
async fn lpush_rpush_respect_value_limit() {
    let _lock = ENV_LOCK.lock().unwrap();

    let _guard = set_env("REDUST_MAXVALUE_BYTES", "4");

    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // value 刚好等于限制
    client.send_array(&["LPUSH", "mylist", "1234"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, ":1\r\n");

    // 超出限制的 value 被拒绝
    client.send_array(&["RPUSH", "mylist", "12345"]).await;
    let line = client.read_simple_line().await;
    assert!(line.starts_with("-ERR value exceeds REDUST_MAXVALUE_BYTES"));

    // 列表长度仍然为 1
    client.send_array(&["LLEN", "mylist"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, ":1\r\n");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn sadd_hset_respect_value_limit() {
    let _lock = ENV_LOCK.lock().unwrap();

    let _guard = set_env("REDUST_MAXVALUE_BYTES", "3");

    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // SADD 中包含超限成员时整体失败
    client
        .send_array(&["SADD", "myset", "ok", "toolong"])
        .await;
    let line = client.read_simple_line().await;
    assert!(line.starts_with("-ERR value exceeds REDUST_MAXVALUE_BYTES"));

    // 集合应保持为空
    client.send_array(&["SCARD", "myset"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, ":0\r\n");

    // HSET 的 value 超限被拒绝
    client
        .send_array(&["HSET", "myhash", "field", "abcd"])
        .await;
    let line = client.read_simple_line().await;
    assert!(line.starts_with("-ERR value exceeds REDUST_MAXVALUE_BYTES"));

    // 不超限的 HSET 正常生效
    client
        .send_array(&["HSET", "myhash", "field", "ok"])
        .await;
    let line = client.read_simple_line().await;
    assert_eq!(line, ":1\r\n");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}
