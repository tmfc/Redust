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

#[tokio::test]
async fn mset_and_mget_roundtrip() {
    let (addr, shutdown, handle) = spawn_server().await;

    let mut client = TestClient::connect(addr).await;

    client
        .mset(&[("k1", "v1"), ("k2", "v2")])
        .await;

    let values = client.mget(&["k1", "k2", "missing"]).await;
    assert_eq!(values.len(), 3);
    assert_eq!(values[0].as_deref(), Some("v1"));
    assert_eq!(values[1].as_deref(), Some("v2"));
    assert_eq!(values[2], None);

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn command_argument_and_integer_errors_match_redis() {
    let (addr, shutdown, handle) = spawn_server().await;

    let mut client = TestClient::connect(addr).await;

    // SET with missing value -> ERR wrong number of arguments for 'set' command
    client.send_array(&["SET", "foo"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "-ERR wrong number of arguments for 'set' command\r\n");

    // GET with missing key -> ERR wrong number of arguments for 'get' command
    client.send_array(&["GET"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "-ERR wrong number of arguments for 'get' command\r\n");

    // EXPIRE with missing seconds -> ERR wrong number of arguments for 'expire' command
    client.send_array(&["EXPIRE", "foo"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "-ERR wrong number of arguments for 'expire' command\r\n");

    // EXPIRE with non-integer seconds -> ERR value is not an integer or out of range
    client.send_array(&["EXPIRE", "foo", "notint"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "-ERR value is not an integer or out of range\r\n");

    // PEXPIRE with non-integer millis -> same integer error
    client.send_array(&["PEXPIRE", "foo", "notint"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "-ERR value is not an integer or out of range\r\n");

    // SETEX with non-integer seconds -> integer error
    client.send_array(&["SETEX", "foo", "notint", "bar"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "-ERR value is not an integer or out of range\r\n");

    // PSETEX with non-integer millis -> integer error
    client.send_array(&["PSETEX", "foo", "notint", "bar"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "-ERR value is not an integer or out of range\r\n");

    // INCRBY with non-integer delta -> integer error
    client.send_array(&["INCRBY", "foo", "notint"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "-ERR value is not an integer or out of range\r\n");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn setnx_semantics() {
    let (addr, shutdown, handle) = spawn_server().await;

    let mut client = TestClient::connect(addr).await;

    let first = client.setnx("foo", "bar").await;
    assert_eq!(first, 1);
    let second = client.setnx("foo", "baz").await;
    assert_eq!(second, 0);

    let v = client.get("foo").await;
    assert_eq!(v.as_deref(), Some("bar"));

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn setex_and_psetex_roundtrip() {
    let (addr, shutdown, handle) = spawn_server().await;

    let mut client = TestClient::connect(addr).await;

    client.setex("foo", 1, "bar").await;
    let v = client.get("foo").await;
    assert_eq!(v.as_deref(), Some("bar"));

    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    let v_after = client.get("foo").await;
    assert_eq!(v_after, None);

    client.psetex("baz", 200, "qux").await;
    let v2 = client.get("baz").await;
    assert_eq!(v2.as_deref(), Some("qux"));
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    let v2_after = client.get("baz").await;
    assert_eq!(v2_after, None);

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn incrby_and_decrby_roundtrip() {
    let (addr, shutdown, handle) = spawn_server().await;

    let mut client = TestClient::connect(addr).await;

    let v1 = client.incrby("counter", 10).await.unwrap();
    assert_eq!(v1, 10);
    let v2 = client.decrby("counter", 3).await.unwrap();
    assert_eq!(v2, 7);

    // 非数字值应返回错误
    client.set("notint", "abc").await;
    let err = client.incrby("notint", 1).await.err().unwrap();
    assert!(err.starts_with("-ERR value is not an integer or out of range"));

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
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

    async fn mset(&mut self, pairs: &[(&str, &str)]) {
        let mut parts: Vec<&str> = Vec::with_capacity(1 + pairs.len() * 2);
        parts.push("MSET");
        for (k, v) in pairs {
            parts.push(k);
            parts.push(v);
        }
        self.send_array(&parts).await;
        let line = self.read_simple_line().await;
        assert_eq!(line, "+OK\r\n");
    }

    async fn mget(&mut self, keys: &[&str]) -> Vec<Option<String>> {
        let mut parts: Vec<&str> = Vec::with_capacity(1 + keys.len());
        parts.push("MGET");
        parts.extend_from_slice(keys);

        self.send_array(&parts).await;

        let mut header = String::new();
        self.reader.read_line(&mut header).await.unwrap();
        assert!(header.starts_with('*'));
        let len_str = &header[1..header.len() - 2];
        let len: usize = len_str.parse().unwrap();

        let mut result = Vec::with_capacity(len);
        for _ in 0..len {
            // 重用 bulk 读取逻辑，但这里直接展开
            let mut bulk_header = String::new();
            self.reader.read_line(&mut bulk_header).await.unwrap();
            if bulk_header == "$-1\r\n" {
                result.push(None);
                continue;
            }
            assert!(bulk_header.starts_with('$'));
            let len_str = &bulk_header[1..bulk_header.len() - 2];
            let bulk_len: usize = len_str.parse().unwrap();

            let mut value = String::new();
            self.reader.read_line(&mut value).await.unwrap();
            assert_eq!(value.len(), bulk_len + 2);
            result.push(Some(value.trim_end_matches("\r\n").to_string()));
        }

        result
    }

    async fn setnx(&mut self, key: &str, value: &str) -> i64 {
        self.send_array(&["SETNX", key, value]).await;
        let line = self.read_simple_line().await;
        assert!(line.starts_with(":"));
        line[1..line.len() - 2].parse().unwrap()
    }

    async fn setex(&mut self, key: &str, seconds: i64, value: &str) {
        let secs = seconds.to_string();
        self.send_array(&["SETEX", key, &secs, value]).await;
        let line = self.read_simple_line().await;
        assert_eq!(line, "+OK\r\n");
    }

    async fn psetex(&mut self, key: &str, millis: i64, value: &str) {
        let ms = millis.to_string();
        self.send_array(&["PSETEX", key, &ms, value]).await;
        let line = self.read_simple_line().await;
        assert_eq!(line, "+OK\r\n");
    }

    async fn incrby(&mut self, key: &str, delta: i64) -> Result<i64, String> {
        let d = delta.to_string();
        self.send_array(&["INCRBY", key, &d]).await;
        let mut line = String::new();
        self.reader.read_line(&mut line).await.unwrap();
        if line.starts_with(":") {
            Ok(line[1..line.len() - 2].parse().unwrap())
        } else {
            Err(line)
        }
    }

    async fn decrby(&mut self, key: &str, delta: i64) -> Result<i64, String> {
        let d = delta.to_string();
        self.send_array(&["DECRBY", key, &d]).await;
        let mut line = String::new();
        self.reader.read_line(&mut line).await.unwrap();
        if line.starts_with(":") {
            Ok(line[1..line.len() - 2].parse().unwrap())
        } else {
            Err(line)
        }
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
