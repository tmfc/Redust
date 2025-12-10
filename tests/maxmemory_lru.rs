use std::net::SocketAddr;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

use redust::server::serve;
mod env_guard;
use env_guard::{set_env, ENV_LOCK};

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

    async fn send_and_read_line(&mut self, parts: &[&str]) -> String {
        self.send_array(parts).await;
        self.read_simple_line().await
    }

    async fn dbsize(&mut self) -> i64 {
        self.send_array(&["DBSIZE"]).await;
        let line = self.read_simple_line().await;
        assert!(line.starts_with(":"));
        line[1..line.len() - 2].parse().unwrap()
    }

    async fn get(&mut self, key: &str) -> Option<String> {
        self.send_array(&["GET", key]).await;
        let mut line = String::new();
        self.reader.read_line(&mut line).await.unwrap();
        if line.starts_with("$-1") {
            return None;
        }
        assert!(line.starts_with('$'), "expected bulk string, got {}", line);
        let len: usize = line[1..line.len() - 2].parse().unwrap();
        let mut buf = vec![0u8; len + 2];
        self.reader.read_exact(&mut buf).await.unwrap();
        Some(String::from_utf8_lossy(&buf[..len]).to_string())
    }

    async fn info_memory(&mut self) -> String {
        self.send_array(&["INFO"]).await;
        let mut first = String::new();
        self.reader.read_line(&mut first).await.unwrap();
        assert!(
            first.starts_with('$'),
            "expected bulk string, got {first:?}"
        );
        let len: usize = first[1..first.len() - 2].parse().unwrap();
        let mut buf = vec![0u8; len + 2];
        self.reader.read_exact(&mut buf).await.unwrap();
        String::from_utf8_lossy(&buf[..len]).to_string()
    }
}

fn parse_info_value(info: &str, key: &str) -> Option<u64> {
    info.lines()
        .find(|line| line.starts_with(key))
        .and_then(|line| line.split_once(':'))
        .and_then(|(_, v)| v.trim().parse::<u64>().ok())
}

#[tokio::test]
async fn basic_maxmemory_eviction_smoke() {
    let _lock = ENV_LOCK.lock().unwrap();
    let _guard_disable = set_env("REDUST_DISABLE_PERSISTENCE", "1");
    let _guard_aof = set_env("REDUST_AOF_ENABLED", "0");
    // 设置较小的 maxmemory（64 KiB），使用纯字节值以匹配当前解析逻辑
    let _guard = set_env("REDUST_MAXMEMORY_BYTES", "65536");

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

#[tokio::test]
async fn volatile_random_eviction_preserves_persistent_keys() {
    let _lock = ENV_LOCK.lock().unwrap();
    let _guard_disable = set_env("REDUST_DISABLE_PERSISTENCE", "1");
    let _guard_aof = set_env("REDUST_AOF_ENABLED", "0");
    let _guard_bytes = set_env("REDUST_MAXMEMORY_BYTES", "65536");
    let _guard_policy = set_env("REDUST_MAXMEMORY_POLICY", "volatile-random");
    assert_eq!(
        std::env::var("REDUST_MAXMEMORY_BYTES").unwrap(),
        "65536"
    );

    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // 一个较大的持久 key（无 TTL），不应被 volatile-* 策略淘汰
    let persistent_value = "P".repeat(32768);
    client.set("persistent", &persistent_value).await;
    let info = client.info_memory().await;
    let used = parse_info_value(&info, "used_memory").unwrap();
    let maxmemory = parse_info_value(&info, "maxmemory").unwrap();
    assert!(
        used < maxmemory,
        "expected used_memory < maxmemory after persistent write, got used={} max={}",
        used,
        maxmemory
    );

    // 写入一批带 TTL 的 key，触发淘汰，验证持久 key 仍存在
    let ttl_value = "t".repeat(8192);
    let ttl_keys = 10;
    for i in 0..ttl_keys {
        let key = format!("ttl{}", i);
        client
            .send_array(&["SET", &key, &ttl_value, "PX", "60000"])
            .await;
        let line = client.read_simple_line().await;
        assert_eq!(line, "+OK\r\n");
    }

    // 持久 key 不应被淘汰
    let persisted = client.get("persistent").await;
    assert_eq!(persisted, Some(persistent_value));

    // 淘汰应发生：至少有部分 TTL key 被驱逐，但持久 key 仍保留
    let mut remaining_ttl = 0;
    for i in 0..ttl_keys {
        let key = format!("ttl{}", i);
        if client.get(&key).await.is_some() {
            remaining_ttl += 1;
        }
    }
    assert!(
        remaining_ttl < ttl_keys,
        "expected some TTL keys to be evicted, still have {}",
        remaining_ttl
    );

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn noeviction_policy_returns_oom() {
    let _lock = ENV_LOCK.lock().unwrap();
    let _guard_disable = set_env("REDUST_DISABLE_PERSISTENCE", "1");
    let _guard_aof = set_env("REDUST_AOF_ENABLED", "0");
    let _guard_bytes = set_env("REDUST_MAXMEMORY_BYTES", "1024");
    let _guard_policy = set_env("REDUST_MAXMEMORY_POLICY", "noeviction");

    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    let big_value = "x".repeat(2048);
    let line = client
        .send_and_read_line(&["SET", "oom-key", &big_value])
        .await;
    assert!(line.starts_with("-OOM"), "expected OOM error, got {line:?}");
    assert_eq!(client.get("oom-key").await, None);

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn volatile_policy_without_ttl_ooms() {
    let _lock = ENV_LOCK.lock().unwrap();
    let _guard_disable = set_env("REDUST_DISABLE_PERSISTENCE", "1");
    let _guard_aof = set_env("REDUST_AOF_ENABLED", "0");
    let _guard_bytes = set_env("REDUST_MAXMEMORY_BYTES", "1024");
    let _guard_policy = set_env("REDUST_MAXMEMORY_POLICY", "volatile-lru");

    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    let big_value = "y".repeat(2048);
    let line = client
        .send_and_read_line(&["SET", "persistent", &big_value])
        .await;
    assert!(
        line.starts_with("-OOM"),
        "expected OOM error under volatile policy without TTL, got {line:?}"
    );
    assert_eq!(client.get("persistent").await, None);

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}
