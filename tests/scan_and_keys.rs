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
            assert!(bulk_header.starts_with('$'));
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

    async fn scan(
        &mut self,
        cursor: u64,
        pattern: Option<&str>,
        count: Option<u64>,
    ) -> (u64, Vec<String>) {
        let mut owned: Vec<String> = Vec::new();
        owned.push("SCAN".to_string());
        owned.push(cursor.to_string());
        if let Some(p) = pattern {
            owned.push("MATCH".to_string());
            owned.push(p.to_string());
        }
        if let Some(c) = count {
            owned.push("COUNT".to_string());
            owned.push(c.to_string());
        }
        let refs: Vec<&str> = owned.iter().map(|s| s.as_str()).collect();
        self.send_array(&refs).await;

        // *2
        let mut outer = String::new();
        self.reader.read_line(&mut outer).await.unwrap();
        assert_eq!(outer, "*2\r\n");

        // cursor line (bulk string)
        let cursor_line = self.read_bulk_string().await.unwrap();
        let next: u64 = cursor_line.parse().unwrap();

        // array of keys
        let keys = self.read_array_of_bulk().await;
        (next, keys)
    }
}

#[tokio::test]
async fn keys_glob_matching_basic() {
    let (addr, shutdown, handle) = spawn_server().await;

    let mut client = TestClient::connect(addr).await;

    // 写入一些 key
    client.send_array(&["SET", "foo", "v1"]).await;
    let _ = client.read_simple_line().await; // +OK
    client.send_array(&["SET", "bar", "v2"]).await;
    let _ = client.read_simple_line().await;
    client.send_array(&["SET", "user:1", "u1"]).await;
    let _ = client.read_simple_line().await;
    client.send_array(&["SET", "user:2", "u2"]).await;
    let _ = client.read_simple_line().await;

    // KEYS *
    client.send_array(&["KEYS", "*"]).await;
    let mut header = String::new();
    client.reader.read_line(&mut header).await.unwrap();
    assert!(header.starts_with('*'));

    // 读取 N 个 bulk key
    let len: usize = header[1..header.len() - 2].parse().unwrap();
    let mut all_keys = Vec::with_capacity(len);
    for _ in 0..len {
        let mut bulk_header = String::new();
        client.reader.read_line(&mut bulk_header).await.unwrap();
        let blen: usize = bulk_header[1..bulk_header.len() - 2].parse().unwrap();
        let mut buf = vec![0u8; blen];
        client.reader.read_exact(&mut buf).await.unwrap();
        let mut crlf = [0u8; 2];
        client.reader.read_exact(&mut crlf).await.unwrap();
        all_keys.push(String::from_utf8(buf).unwrap());
    }

    // 至少包含我们写入的四个 key
    for k in &["foo", "bar", "user:1", "user:2"] {
        assert!(all_keys.contains(&k.to_string()));
    }

    // KEYS user:*
    client.send_array(&["KEYS", "user:*"]).await;
    header.clear();
    client.reader.read_line(&mut header).await.unwrap();
    assert!(header.starts_with('*'));
    let len2: usize = header[1..header.len() - 2].parse().unwrap();
    let mut user_keys = Vec::with_capacity(len2);
    for _ in 0..len2 {
        let mut bulk_header = String::new();
        client.reader.read_line(&mut bulk_header).await.unwrap();
        let blen: usize = bulk_header[1..bulk_header.len() - 2].parse().unwrap();
        let mut buf = vec![0u8; blen];
        client.reader.read_exact(&mut buf).await.unwrap();
        let mut crlf = [0u8; 2];
        client.reader.read_exact(&mut crlf).await.unwrap();
        user_keys.push(String::from_utf8(buf).unwrap());
    }
    user_keys.sort();
    assert_eq!(user_keys, vec!["user:1".to_string(), "user:2".to_string()]);

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn scan_basic_iteration_with_match_and_count() {
    let (addr, shutdown, handle) = spawn_server().await;

    let mut client = TestClient::connect(addr).await;

    // 写入一批 key
    for i in 0..10 {
        let k = format!("user:{}", i);
        let v = format!("v{}", i);
        client.send_array(&["SET", &k, &v]).await;
        let _ = client.read_simple_line().await; // +OK
    }
    client.send_array(&["SET", "other", "x"]).await;
    let _ = client.read_simple_line().await;

    // 使用 COUNT=3 和 MATCH user:* 扫描所有 user:* key
    let mut cursor = 0u64;
    let mut seen = Vec::new();
    loop {
        let (next, keys) = client.scan(cursor, Some("user:*"), Some(3)).await;
        for k in keys {
            if !k.is_empty() {
                seen.push(k);
            }
        }
        if next == 0 {
            break;
        }
        cursor = next;
    }

    seen.sort();
    let expected: Vec<String> = (0..10).map(|i| format!("user:{}", i)).collect();
    assert_eq!(seen, expected);

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn scan_argument_and_integer_errors() {
    let (addr, shutdown, handle) = spawn_server().await;

    let mut client = TestClient::connect(addr).await;

    // 缺少 cursor -> wrong args
    client.send_array(&["SCAN"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(
        line,
        "-ERR wrong number of arguments for 'scan' command\r\n"
    );

    // COUNT 为非整数 -> integer error
    client.send_array(&["SCAN", "0", "COUNT", "notint"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "-ERR value is not an integer or out of range\r\n");

    // 负 cursor -> integer error
    client.send_array(&["SCAN", "-1"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "-ERR value is not an integer or out of range\r\n");

    // 未知选项 -> syntax error
    client.send_array(&["SCAN", "0", "FOO", "bar"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "-ERR syntax error\r\n");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn sscan_basic_roundtrip() {
    let (addr, shutdown, handle) = spawn_server().await;

    let mut client = TestClient::connect(addr).await;

    // SADD myset a b c d e f g h i j
    client
        .send_array(&[
            "SADD", "myset", "a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
        ])
        .await;
    let _ = client.read_simple_line().await; // :10

    // SSCAN myset 0 MATCH a* COUNT 3 -> 收集所有 a 开头的成员（这里只放一个 a 用于简单验证）
    let mut cursor = 0u64;
    let mut seen = Vec::new();
    loop {
        // 构造 SSCAN 命令
        let mut owned: Vec<String> = Vec::new();
        owned.push("SSCAN".to_string());
        owned.push("myset".to_string());
        owned.push(cursor.to_string());
        owned.push("MATCH".to_string());
        owned.push("a*".to_string());
        owned.push("COUNT".to_string());
        owned.push("3".to_string());
        let refs: Vec<&str> = owned.iter().map(|s| s.as_str()).collect();
        client.send_array(&refs).await;

        // 解析 SCAN 风格响应
        let mut outer = String::new();
        client.reader.read_line(&mut outer).await.unwrap();
        assert_eq!(outer, "*2\r\n");

        let cursor_line = client.read_bulk_string().await.unwrap();
        let next: u64 = cursor_line.parse().unwrap();

        let keys = client.read_array_of_bulk().await;
        for k in keys {
            if !k.is_empty() {
                seen.push(k);
            }
        }
        if next == 0 {
            break;
        }
        cursor = next;
    }

    seen.sort();
    assert_eq!(seen, vec!["a".to_string()]);

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn hscan_basic_roundtrip() {
    let (addr, shutdown, handle) = spawn_server().await;

    let mut client = TestClient::connect(addr).await;

    // HSET myhash f0 v0 ... f9 v9
    for i in 0..10 {
        let f = format!("f{}", i);
        let v = format!("v{}", i);
        client.send_array(&["HSET", "myhash", &f, &v]).await;
        let _ = client.read_simple_line().await; // :1 or :0
    }

    // HSCAN myhash 0 MATCH f* COUNT 100
    let mut fields = Vec::new();
    let mut values = Vec::new();

    let mut owned: Vec<String> = Vec::new();
    owned.push("HSCAN".to_string());
    owned.push("myhash".to_string());
    owned.push("0".to_string());
    owned.push("MATCH".to_string());
    owned.push("f*".to_string());
    owned.push("COUNT".to_string());
    owned.push("100".to_string());
    let refs: Vec<&str> = owned.iter().map(|s| s.as_str()).collect();
    client.send_array(&refs).await;

    let mut outer = String::new();
    client.reader.read_line(&mut outer).await.unwrap();
    assert_eq!(outer, "*2\r\n");

    let cursor_line = client.read_bulk_string().await.unwrap();
    let next: u64 = cursor_line.parse().unwrap();
    assert_eq!(next, 0);

    let flat = client.read_array_of_bulk().await;

    // flat 长度应为偶数：field,value,...
    assert_eq!(flat.len() % 2, 0);
    for pair in flat.chunks(2) {
        if pair[0].is_empty() {
            continue;
        }
        fields.push(pair[0].clone());
        values.push(pair[1].clone());
    }

    fields.sort();
    values.sort();
    let expected_fields: Vec<String> = (0..10).map(|i| format!("f{}", i)).collect();
    let expected_values: Vec<String> = (0..10).map(|i| format!("v{}", i)).collect();
    assert_eq!(fields, expected_fields);
    assert_eq!(values, expected_values);

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn zscan_wrongtype_returns_error() {
    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    client.send_array(&["SET", "foo", "bar"]).await;
    let _ = client.read_simple_line().await;

    client.send_array(&["ZSCAN", "foo", "0"]).await;
    let mut line = String::new();
    client.reader.read_line(&mut line).await.unwrap();
    assert!(line.starts_with("-WRONGTYPE"));

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn zscan_missing_key_returns_empty() {
    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    client.send_array(&["ZSCAN", "missing", "0"]).await;

    let mut outer = String::new();
    client.reader.read_line(&mut outer).await.unwrap();
    assert_eq!(outer, "*2\r\n");

    let cursor = client.read_bulk_string().await.unwrap();
    assert_eq!(cursor, "0");

    let flat = client.read_array_of_bulk().await;
    assert!(flat.is_empty());

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

/// 测试 `?` 单字符通配符
#[tokio::test]
async fn keys_glob_question_mark() {
    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // 写入 key: a1, a2, a10, ab
    client.send_array(&["SET", "a1", "v"]).await;
    let _ = client.read_simple_line().await;
    client.send_array(&["SET", "a2", "v"]).await;
    let _ = client.read_simple_line().await;
    client.send_array(&["SET", "a10", "v"]).await;
    let _ = client.read_simple_line().await;
    client.send_array(&["SET", "ab", "v"]).await;
    let _ = client.read_simple_line().await;

    // KEYS a? 应该匹配 a1, a2, ab（两个字符），不匹配 a10
    client.send_array(&["KEYS", "a?"]).await;
    let keys = client.read_array_of_bulk().await;
    let mut keys_sorted = keys.clone();
    keys_sorted.sort();
    assert_eq!(keys_sorted, vec!["a1", "a2", "ab"]);

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

/// 测试 `[abc]` 字符集匹配
#[tokio::test]
async fn keys_glob_character_set() {
    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // 写入 key: xa, xb, xc, xd, xe
    for c in ['a', 'b', 'c', 'd', 'e'] {
        let k = format!("x{}", c);
        client.send_array(&["SET", &k, "v"]).await;
        let _ = client.read_simple_line().await;
    }

    // KEYS x[abc] 应该匹配 xa, xb, xc
    client.send_array(&["KEYS", "x[abc]"]).await;
    let keys = client.read_array_of_bulk().await;
    let mut keys_sorted = keys.clone();
    keys_sorted.sort();
    assert_eq!(keys_sorted, vec!["xa", "xb", "xc"]);

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

/// 测试 `[a-z]` 字符范围匹配
#[tokio::test]
async fn keys_glob_character_range() {
    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // 写入 key: k0, k5, ka, kz, kA
    client.send_array(&["SET", "k0", "v"]).await;
    let _ = client.read_simple_line().await;
    client.send_array(&["SET", "k5", "v"]).await;
    let _ = client.read_simple_line().await;
    client.send_array(&["SET", "ka", "v"]).await;
    let _ = client.read_simple_line().await;
    client.send_array(&["SET", "kz", "v"]).await;
    let _ = client.read_simple_line().await;
    client.send_array(&["SET", "kA", "v"]).await;
    let _ = client.read_simple_line().await;

    // KEYS k[a-z] 应该匹配 ka, kz（小写字母）
    client.send_array(&["KEYS", "k[a-z]"]).await;
    let keys = client.read_array_of_bulk().await;
    let mut keys_sorted = keys.clone();
    keys_sorted.sort();
    assert_eq!(keys_sorted, vec!["ka", "kz"]);

    // KEYS k[0-9] 应该匹配 k0, k5
    client.send_array(&["KEYS", "k[0-9]"]).await;
    let keys = client.read_array_of_bulk().await;
    let mut keys_sorted = keys.clone();
    keys_sorted.sort();
    assert_eq!(keys_sorted, vec!["k0", "k5"]);

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

/// 测试转义字符 `\*` `\?`
#[tokio::test]
async fn keys_glob_escape_special_chars() {
    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // 写入 key: foo*, foo?, foobar
    client.send_array(&["SET", "foo*", "v"]).await;
    let _ = client.read_simple_line().await;
    client.send_array(&["SET", "foo?", "v"]).await;
    let _ = client.read_simple_line().await;
    client.send_array(&["SET", "foobar", "v"]).await;
    let _ = client.read_simple_line().await;

    // KEYS foo\* 应该只匹配 foo*（字面星号）
    client.send_array(&["KEYS", r"foo\*"]).await;
    let keys = client.read_array_of_bulk().await;
    assert_eq!(keys, vec!["foo*"]);

    // KEYS foo\? 应该只匹配 foo?（字面问号）
    client.send_array(&["KEYS", r"foo\?"]).await;
    let keys = client.read_array_of_bulk().await;
    assert_eq!(keys, vec!["foo?"]);

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

/// 测试复合模式 `user:*:profile`
#[tokio::test]
async fn keys_glob_compound_pattern() {
    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // 写入 key
    client.send_array(&["SET", "user:1:profile", "v"]).await;
    let _ = client.read_simple_line().await;
    client.send_array(&["SET", "user:2:profile", "v"]).await;
    let _ = client.read_simple_line().await;
    client.send_array(&["SET", "user:1:settings", "v"]).await;
    let _ = client.read_simple_line().await;
    client.send_array(&["SET", "user:abc:profile", "v"]).await;
    let _ = client.read_simple_line().await;

    // KEYS user:*:profile 应该匹配 user:1:profile, user:2:profile, user:abc:profile
    client.send_array(&["KEYS", "user:*:profile"]).await;
    let keys = client.read_array_of_bulk().await;
    let mut keys_sorted = keys.clone();
    keys_sorted.sort();
    assert_eq!(
        keys_sorted,
        vec!["user:1:profile", "user:2:profile", "user:abc:profile"]
    );

    // KEYS user:?:profile 应该只匹配单字符 ID
    client.send_array(&["KEYS", "user:?:profile"]).await;
    let keys = client.read_array_of_bulk().await;
    let mut keys_sorted = keys.clone();
    keys_sorted.sort();
    assert_eq!(keys_sorted, vec!["user:1:profile", "user:2:profile"]);

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

/// 测试 SCAN 带 MATCH 使用各种 glob 模式
#[tokio::test]
async fn scan_with_various_glob_patterns() {
    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // 写入 key
    for i in 0..5 {
        let k = format!("item:{}", i);
        client.send_array(&["SET", &k, "v"]).await;
        let _ = client.read_simple_line().await;
    }
    client.send_array(&["SET", "item:abc", "v"]).await;
    let _ = client.read_simple_line().await;
    client.send_array(&["SET", "other", "v"]).await;
    let _ = client.read_simple_line().await;

    // SCAN with MATCH item:? (单字符)
    let mut cursor = 0u64;
    let mut seen = Vec::new();
    loop {
        let (next, keys) = client.scan(cursor, Some("item:?"), Some(100)).await;
        for k in keys {
            if !k.is_empty() {
                seen.push(k);
            }
        }
        if next == 0 {
            break;
        }
        cursor = next;
    }
    seen.sort();
    let expected: Vec<String> = (0..5).map(|i| format!("item:{}", i)).collect();
    assert_eq!(seen, expected);

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}
