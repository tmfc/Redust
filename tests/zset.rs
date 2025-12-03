use std::net::SocketAddr;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

use redust::server::serve;

async fn spawn_server() -> (
    SocketAddr,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<tokio::io::Result<()>>,
) {
    std::env::set_var("REDUST_DISABLE_PERSISTENCE", "1");
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

    async fn read_bulk_string(&mut self) -> Option<String> {
        let mut header = String::new();
        self.reader.read_line(&mut header).await.unwrap();
        if header == "$-1\r\n" {
            return None;
        }
        assert!(
            header.starts_with('$'),
            "expected bulk string header, got {:?}",
            header.trim_end()
        );
        let len: usize = header[1..header.len() - 2].parse().unwrap();
        let mut buf = vec![0u8; len];
        self.reader.read_exact(&mut buf).await.unwrap();
        let mut crlf = [0u8; 2];
        self.reader.read_exact(&mut crlf).await.unwrap();
        assert_eq!(&crlf, b"\r\n");
        Some(String::from_utf8(buf).unwrap())
    }

    async fn read_array_of_bulk(&mut self) -> Vec<String> {
        let mut header = String::new();
        self.reader.read_line(&mut header).await.unwrap();
        assert!(header.starts_with('*'));
        let len_str = &header[1..header.len() - 2];
        let len: usize = len_str.parse().unwrap();

        let mut items = Vec::with_capacity(len);
        for _ in 0..len {
            let mut bulk_header = String::new();
            self.reader.read_line(&mut bulk_header).await.unwrap();
            if bulk_header == "$-1\r\n" {
                items.push(String::new());
                continue;
            }
            assert!(
                bulk_header.starts_with('$'),
                "expected bulk, got {:?}",
                bulk_header.trim_end()
            );
            let bulk_len: usize = bulk_header[1..bulk_header.len() - 2].parse().unwrap();
            let mut buf = vec![0u8; bulk_len];
            self.reader.read_exact(&mut buf).await.unwrap();
            let mut crlf = [0u8; 2];
            self.reader.read_exact(&mut crlf).await.unwrap();
            assert_eq!(&crlf, b"\r\n");
            items.push(String::from_utf8(buf).unwrap());
        }

        items
    }
}

#[tokio::test]
async fn zadd_range_and_score() {
    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    client
        .send_array(&["ZADD", "myz", "1", "one", "2", "two", "3", "three"])
        .await;
    let added = client.read_simple_line().await;
    assert_eq!(added, ":3\r\n");

    client.send_array(&["ZCARD", "myz"]).await;
    let card = client.read_simple_line().await;
    assert_eq!(card, ":3\r\n");

    client
        .send_array(&["ZRANGE", "myz", "0", "-1", "WITHSCORES"])
        .await;
    let range = client.read_array_of_bulk().await;
    assert_eq!(range, vec!["one", "1", "two", "2", "three", "3"]);

    client.send_array(&["ZREVRANGE", "myz", "0", "1"]).await;
    let rev = client.read_array_of_bulk().await;
    assert_eq!(rev, vec!["three".to_string(), "two".to_string()]);

    client.send_array(&["ZSCORE", "myz", "two"]).await;
    let score = client.read_bulk_string().await.unwrap();
    assert_eq!(score, "2");

    client.send_array(&["ZREM", "myz", "two"]).await;
    let removed = client.read_simple_line().await;
    assert_eq!(removed, ":1\r\n");

    client.send_array(&["ZCARD", "myz"]).await;
    let card_after = client.read_simple_line().await;
    assert_eq!(card_after, ":2\r\n");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn zincrby_updates_scores() {
    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    client
        .send_array(&["ZINCRBY", "myz", "1.5", "alpha"])
        .await;
    let s1 = client.read_bulk_string().await.unwrap();
    assert_eq!(s1, "1.5");

    client
        .send_array(&["ZINCRBY", "myz", "2.0", "alpha"])
        .await;
    let s2 = client.read_bulk_string().await.unwrap();
    assert_eq!(s2, "3.5");

    client
        .send_array(&["ZINCRBY", "myz", "4", "beta"])
        .await;
    let s3 = client.read_bulk_string().await.unwrap();
    assert_eq!(s3, "4");

    client
        .send_array(&["ZRANGE", "myz", "0", "-1", "WITHSCORES"])
        .await;
    let range = client.read_array_of_bulk().await;
    assert_eq!(range, vec!["alpha", "3.5", "beta", "4"]);

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn zscan_roundtrip_and_wrongtype() {
    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    client.send_array(&["SET", "foo", "bar"]).await;
    let _ = client.read_simple_line().await;

    client.send_array(&["ZSCAN", "foo", "0"]).await;
    let mut err = String::new();
    client.reader.read_line(&mut err).await.unwrap();
    assert!(err.starts_with("-WRONGTYPE"));

    client
        .send_array(&["ZADD", "myz", "1", "a", "2", "b", "3", "c", "4", "aa"])
        .await;
    let _ = client.read_simple_line().await;

    let mut cursor = 0u64;
    let mut seen = Vec::new();
    loop {
        let mut owned: Vec<String> = Vec::new();
        owned.push("ZSCAN".to_string());
        owned.push("myz".to_string());
        owned.push(cursor.to_string());
        owned.push("MATCH".to_string());
        owned.push("a*".to_string());
        owned.push("COUNT".to_string());
        owned.push("2".to_string());
        let refs: Vec<&str> = owned.iter().map(|s| s.as_str()).collect();
        client.send_array(&refs).await;

        let mut outer = String::new();
        client.reader.read_line(&mut outer).await.unwrap();
        assert_eq!(outer, "*2\r\n");

        let cursor_line = client.read_bulk_string().await.unwrap();
        let next: u64 = cursor_line.parse().unwrap();

        let flat = client.read_array_of_bulk().await;
        assert_eq!(flat.len() % 2, 0);
        for chunk in flat.chunks(2) {
            if chunk[0].is_empty() {
                continue;
            }
            seen.push(chunk[0].clone());
        }

        if next == 0 {
            break;
        }
        cursor = next;
    }

    seen.sort();
    assert_eq!(seen, vec!["a".to_string(), "aa".to_string()]);

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

/// 测试 ZSET 持久化（RDB save/load）
#[tokio::test]
async fn zset_persistence_roundtrip() {
    use std::path::PathBuf;
    use redust::storage::Storage;

    let temp_dir = std::env::temp_dir();
    let rdb_path: PathBuf = temp_dir.join(format!("test_zset_{}.rdb", std::process::id()));

    // 创建 storage 并添加 ZSET 数据
    let storage = Storage::new(None);
    assert!(storage.zadd("0:myzset", &[(1.5, "a".to_string()), (2.5, "b".to_string()), (3.0, "c".to_string())]).is_ok());
    assert!(storage.zadd("0:anotherzset", &[(100.0, "x".to_string())]).is_ok());

    // 保存 RDB
    storage.save_rdb(&rdb_path).expect("save_rdb should succeed");

    // 创建新的 storage 并加载
    let storage2 = Storage::new(None);
    storage2.load_rdb(&rdb_path).expect("load_rdb should succeed");

    // 验证 ZSET 数据已恢复
    let score_a = storage2.zscore("0:myzset", "a").unwrap();
    assert_eq!(score_a, Some(1.5), "score of 'a' should be 1.5");

    let score_b = storage2.zscore("0:myzset", "b").unwrap();
    assert_eq!(score_b, Some(2.5), "score of 'b' should be 2.5");

    let score_c = storage2.zscore("0:myzset", "c").unwrap();
    assert_eq!(score_c, Some(3.0), "score of 'c' should be 3.0");

    let score_x = storage2.zscore("0:anotherzset", "x").unwrap();
    assert_eq!(score_x, Some(100.0), "score of 'x' should be 100.0");

    // ZCARD 验证
    let card = storage2.zcard("0:myzset").unwrap();
    assert_eq!(card, 3, "myzset should have 3 members");

    // 清理
    let _ = std::fs::remove_file(&rdb_path);
}
