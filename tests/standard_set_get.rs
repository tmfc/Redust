use std::net::SocketAddr;

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

    async fn read_bulk(&mut self) -> Option<String> {
        let mut header = String::new();
        self.reader.read_line(&mut header).await.unwrap();

        if header == "$-1\r\n" {
            return None;
        }

        assert!(header.starts_with('$'));
        let len_str = &header[1..header.len() - 2];
        let len: usize = len_str.parse().unwrap();

        let mut value = String::new();
        self.reader.read_line(&mut value).await.unwrap();

        assert_eq!(value.len(), len + 2);
        Some(value.trim_end_matches("\r\n").to_string())
    }

    async fn set(&mut self, key: &str, value: &str) {
        self.send_array(&["SET", key, value]).await;
        let line = self.read_simple_line().await;
        assert_eq!(line, "+OK\r\n");
    }

    async fn get(&mut self, key: &str) -> Option<String> {
        self.send_array(&["GET", key]).await;
        self.read_bulk().await
    }
}

#[tokio::test]
async fn standard_set_get_roundtrip() {
    let (addr, shutdown, handle) = spawn_server().await;

    let mut client = TestClient::connect(addr).await;

    client.set("foo", "bar").await;
    let value = client.get("foo").await;
    assert_eq!(value.as_deref(), Some("bar"));

    let missing = client.get("missing").await;
    assert_eq!(missing, None);

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn pexpire_and_pttl_roundtrip() {
    let (addr, shutdown, handle) = spawn_server().await;

    let mut client = TestClient::connect(addr).await;

    // SET foo bar
    client.set("foo", "bar").await;

    // PEXPIRE foo 500
    client.send_array(&["PEXPIRE", "foo", "500"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, ":1\r\n");

    // 立刻 PTTL foo，应返回非负值
    client.send_array(&["PTTL", "foo"]).await;
    let pttl_line = client.read_simple_line().await;
    assert!(pttl_line.starts_with(":"));

    // 等待超过 500ms 后，key 应该过期
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;

    let v = client.get("foo").await;
    assert_eq!(v, None);

    client.send_array(&["PTTL", "foo"]).await;
    let pttl_after = client.read_simple_line().await;
    assert_eq!(pttl_after, ":-2\r\n");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn persist_clears_expiration() {
    let (addr, shutdown, handle) = spawn_server().await;

    let mut client = TestClient::connect(addr).await;

    // SET foo bar EX 10
    client
        .send_array(&["SET", "foo", "bar", "EX", "10"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "+OK\r\n");

    // TTL foo 应为非负
    client.send_array(&["TTL", "foo"]).await;
    let ttl_before = client.read_simple_line().await;
    assert!(ttl_before.starts_with(":"));

    // PERSIST foo -> 1
    client.send_array(&["PERSIST", "foo"]).await;
    let persist_resp = client.read_simple_line().await;
    assert_eq!(persist_resp, ":1\r\n");

    // 现在 TTL foo -> -1（存在但无过期）
    client.send_array(&["TTL", "foo"]).await;
    let ttl_after = client.read_simple_line().await;
    assert_eq!(ttl_after, ":-1\r\n");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn ttl_and_expire_roundtrip() {
    let (addr, shutdown, handle) = spawn_server().await;

    let mut client = TestClient::connect(addr).await;

    // SET foo bar EX 1
    client
        .send_array(&["SET", "foo", "bar", "EX", "1"]) // 1 秒过期
        .await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "+OK\r\n");

    // 立即 TTL foo，应该 >=0
    client.send_array(&["TTL", "foo"]).await;
    let ttl_line = client.read_simple_line().await;
    assert!(ttl_line.starts_with(":"));

    // 等待超过 1 秒后，key 应该过期
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;

    // GET foo -> nil
    let v = client.get("foo").await;
    assert_eq!(v, None);

    // TTL foo -> -2
    client.send_array(&["TTL", "foo"]).await;
    let ttl_after = client.read_simple_line().await;
    assert_eq!(ttl_after, ":-2\r\n");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn standard_set_overwrite() {
    let (addr, shutdown, handle) = spawn_server().await;

    let mut client = TestClient::connect(addr).await;

    client.set("foo", "bar").await;
    let v1 = client.get("foo").await;
    assert_eq!(v1.as_deref(), Some("bar"));

    client.set("foo", "baz").await;
    let v2 = client.get("foo").await;
    assert_eq!(v2.as_deref(), Some("baz"));

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}
