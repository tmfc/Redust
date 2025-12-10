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

    #[allow(dead_code)]
    async fn read_array_of_bulk(&mut self) -> Vec<String> {
        let mut header = String::new();
        self.reader.read_line(&mut header).await.unwrap();
        if header == "*-1\r\n" {
            return vec![];
        }
        assert!(header.starts_with('*'));
        let len_str = &header[1..header.len() - 2];
        let len: usize = len_str.parse().unwrap();

        let mut items = Vec::with_capacity(len);
        for _ in 0..len {
            let s = self.read_bulk_string().await.unwrap_or_default();
            items.push(s);
        }

        items
    }

    async fn read_array_len(&mut self) -> Option<usize> {
        let mut header = String::new();
        self.reader.read_line(&mut header).await.unwrap();
        if header == "*-1\r\n" {
            return None;
        }
        assert!(header.starts_with('*'));
        let len_str = &header[1..header.len() - 2];
        let len: usize = len_str.parse().unwrap();
        Some(len)
    }

    async fn read_n_bulk_strings(&mut self, n: usize) -> Vec<String> {
        let mut items = Vec::with_capacity(n);
        for _ in 0..n {
            let s = self.read_bulk_string().await.unwrap_or_default();
            items.push(s);
        }
        items
    }
}

/// 测试基本的 MULTI/EXEC 流程
#[tokio::test]
async fn multi_exec_basic() {
    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // MULTI
    client.send_array(&["MULTI"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "+OK\r\n");

    // SET foo bar -> QUEUED
    client.send_array(&["SET", "foo", "bar"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "+QUEUED\r\n");

    // GET foo -> QUEUED
    client.send_array(&["GET", "foo"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "+QUEUED\r\n");

    // INCR counter -> QUEUED
    client.send_array(&["INCR", "counter"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "+QUEUED\r\n");

    // EXEC -> 返回数组
    client.send_array(&["EXEC"]).await;
    // 读取数组头
    let mut header = String::new();
    client.reader.read_line(&mut header).await.unwrap();
    assert_eq!(header, "*3\r\n");

    // 第一个结果：SET -> +OK
    let line = client.read_simple_line().await;
    assert_eq!(line, "+OK\r\n");

    // 第二个结果：GET -> $3\r\nbar\r\n
    let val = client.read_bulk_string().await;
    assert_eq!(val, Some("bar".to_string()));

    // 第三个结果：INCR -> :1
    let line = client.read_simple_line().await;
    assert_eq!(line, ":1\r\n");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

/// 事务中支持 TYPE/KEYS/SCAN 命令
#[tokio::test]
async fn meta_commands_inside_transaction() {
    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // 准备一些数据
    client.send_array(&["SET", "foo", "bar"]).await;
    let _ = client.read_simple_line().await;
    client.send_array(&["LPUSH", "list", "a"]).await;
    let _ = client.read_simple_line().await;

    // MULTI
    client.send_array(&["MULTI"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "+OK\r\n");

    // QUEUE TYPE
    client.send_array(&["TYPE", "foo"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "+QUEUED\r\n");

    // QUEUE KEYS
    client.send_array(&["KEYS", "*"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "+QUEUED\r\n");

    // QUEUE SCAN
    client
        .send_array(&["SCAN", "0", "COUNT", "10", "TYPE", "string"])
        .await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "+QUEUED\r\n");

    // EXEC
    client.send_array(&["EXEC"]).await;
    let len = client.read_array_len().await;
    assert_eq!(len, Some(3));

    // TYPE -> simple string
    let line = client.read_simple_line().await;
    assert_eq!(line, "+string\r\n");

    // KEYS -> array contains foo and list (order by storage sorting)
    let keys_len = client.read_array_len().await.unwrap();
    let keys = client.read_n_bulk_strings(keys_len).await;
    assert!(keys.contains(&"foo".to_string()));
    assert!(keys.contains(&"list".to_string()));

    // SCAN -> array of [cursor, items]; ensure foo present, only strings
    let scan_outer_len = client.read_array_len().await.unwrap();
    assert_eq!(scan_outer_len, 2);
    let cursor_bulk = client.read_bulk_string().await.unwrap();
    let scan_items_len = client.read_array_len().await.unwrap();
    let scan_items = client.read_n_bulk_strings(scan_items_len).await;
    // 游标应为 0 结束
    assert_eq!(cursor_bulk, "0");
    // 只应返回 string 类型 key：foo
    assert!(scan_items.contains(&"foo".to_string()));
    assert!(!scan_items.contains(&"list".to_string()));

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

/// 测试 DISCARD 取消事务
#[tokio::test]
async fn multi_discard() {
    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // 先设置一个值
    client.send_array(&["SET", "key1", "original"]).await;
    let _ = client.read_simple_line().await;

    // MULTI
    client.send_array(&["MULTI"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "+OK\r\n");

    // SET key1 modified -> QUEUED
    client.send_array(&["SET", "key1", "modified"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "+QUEUED\r\n");

    // DISCARD
    client.send_array(&["DISCARD"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "+OK\r\n");

    // 验证值未被修改
    client.send_array(&["GET", "key1"]).await;
    let val = client.read_bulk_string().await;
    assert_eq!(val, Some("original".to_string()));

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

/// 测试 EXEC without MULTI 错误
#[tokio::test]
async fn exec_without_multi_error() {
    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    client.send_array(&["EXEC"]).await;
    let line = client.read_simple_line().await;
    assert!(line.starts_with("-ERR EXEC without MULTI"));

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

/// 测试 DISCARD without MULTI 错误
#[tokio::test]
async fn discard_without_multi_error() {
    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    client.send_array(&["DISCARD"]).await;
    let line = client.read_simple_line().await;
    assert!(line.starts_with("-ERR DISCARD without MULTI"));

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

/// 测试嵌套 MULTI 错误
#[tokio::test]
async fn nested_multi_error() {
    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    client.send_array(&["MULTI"]).await;
    let _ = client.read_simple_line().await;

    client.send_array(&["MULTI"]).await;
    let line = client.read_simple_line().await;
    assert!(line.starts_with("-ERR MULTI calls can not be nested"));

    // 清理：DISCARD
    client.send_array(&["DISCARD"]).await;
    let _ = client.read_simple_line().await;

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

/// 测试 WATCH 基本功能
#[tokio::test]
async fn watch_basic() {
    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // 设置初始值
    client.send_array(&["SET", "watched_key", "initial"]).await;
    let _ = client.read_simple_line().await;

    // WATCH
    client.send_array(&["WATCH", "watched_key"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "+OK\r\n");

    // MULTI
    client.send_array(&["MULTI"]).await;
    let _ = client.read_simple_line().await;

    // SET watched_key new_value -> QUEUED
    client
        .send_array(&["SET", "watched_key", "new_value"])
        .await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "+QUEUED\r\n");

    // EXEC（没有其他客户端修改，应该成功）
    client.send_array(&["EXEC"]).await;
    let mut header = String::new();
    client.reader.read_line(&mut header).await.unwrap();
    assert_eq!(header, "*1\r\n");

    // 读取 SET 结果
    let line = client.read_simple_line().await;
    assert_eq!(line, "+OK\r\n");

    // 验证值已更新
    client.send_array(&["GET", "watched_key"]).await;
    let val = client.read_bulk_string().await;
    assert_eq!(val, Some("new_value".to_string()));

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

/// 测试 WATCH 被其他客户端修改时事务失败
#[tokio::test]
async fn watch_aborted_by_concurrent_modification() {
    let (addr, shutdown, handle) = spawn_server().await;

    let mut client1 = TestClient::connect(addr).await;
    let mut client2 = TestClient::connect(addr).await;

    // client1: 设置初始值
    client1.send_array(&["SET", "key", "v1"]).await;
    let _ = client1.read_simple_line().await;

    // client1: WATCH key
    client1.send_array(&["WATCH", "key"]).await;
    let line = client1.read_simple_line().await;
    assert_eq!(line, "+OK\r\n");

    // client1: MULTI
    client1.send_array(&["MULTI"]).await;
    let _ = client1.read_simple_line().await;

    // client1: SET key v2 -> QUEUED
    client1.send_array(&["SET", "key", "v2"]).await;
    let line = client1.read_simple_line().await;
    assert_eq!(line, "+QUEUED\r\n");

    // client2: 修改 key（在 client1 EXEC 之前）
    client2.send_array(&["SET", "key", "v3"]).await;
    let _ = client2.read_simple_line().await;

    // client1: EXEC（应该失败，返回 null）
    client1.send_array(&["EXEC"]).await;
    let mut header = String::new();
    client1.reader.read_line(&mut header).await.unwrap();
    assert_eq!(
        header, "*-1\r\n",
        "EXEC should return null when WATCH fails"
    );

    // 验证值是 client2 设置的
    client1.send_array(&["GET", "key"]).await;
    let val = client1.read_bulk_string().await;
    assert_eq!(val, Some("v3".to_string()));

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

/// 测试 UNWATCH
#[tokio::test]
async fn unwatch_clears_watched_keys() {
    let (addr, shutdown, handle) = spawn_server().await;

    let mut client1 = TestClient::connect(addr).await;
    let mut client2 = TestClient::connect(addr).await;

    // client1: 设置初始值
    client1.send_array(&["SET", "key", "v1"]).await;
    let _ = client1.read_simple_line().await;

    // client1: WATCH key
    client1.send_array(&["WATCH", "key"]).await;
    let _ = client1.read_simple_line().await;

    // client1: UNWATCH
    client1.send_array(&["UNWATCH"]).await;
    let line = client1.read_simple_line().await;
    assert_eq!(line, "+OK\r\n");

    // client2: 修改 key
    client2.send_array(&["SET", "key", "v2"]).await;
    let _ = client2.read_simple_line().await;

    // client1: MULTI + SET + EXEC（应该成功，因为已经 UNWATCH）
    client1.send_array(&["MULTI"]).await;
    let _ = client1.read_simple_line().await;

    client1.send_array(&["SET", "key", "v3"]).await;
    let _ = client1.read_simple_line().await;

    client1.send_array(&["EXEC"]).await;
    let mut header = String::new();
    client1.reader.read_line(&mut header).await.unwrap();
    assert_eq!(header, "*1\r\n");

    // 读取 SET 结果
    let line = client1.read_simple_line().await;
    assert_eq!(line, "+OK\r\n");

    // 验证值是 client1 设置的
    client1.send_array(&["GET", "key"]).await;
    let val = client1.read_bulk_string().await;
    assert_eq!(val, Some("v3".to_string()));

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

/// 测试 WATCH inside MULTI 错误
#[tokio::test]
async fn watch_inside_multi_error() {
    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    client.send_array(&["MULTI"]).await;
    let _ = client.read_simple_line().await;

    client.send_array(&["WATCH", "key"]).await;
    let line = client.read_simple_line().await;
    assert!(line.starts_with("-ERR WATCH inside MULTI is not allowed"));

    // 清理
    client.send_array(&["DISCARD"]).await;
    let _ = client.read_simple_line().await;

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

/// 测试空事务
#[tokio::test]
async fn empty_transaction() {
    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    client.send_array(&["MULTI"]).await;
    let _ = client.read_simple_line().await;

    client.send_array(&["EXEC"]).await;
    let mut header = String::new();
    client.reader.read_line(&mut header).await.unwrap();
    assert_eq!(header, "*0\r\n");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

/// 测试 WATCH 检测 HSET 修改（P1 修复验证）
#[tokio::test]
async fn watch_detects_hset_modification() {
    let (addr, shutdown, handle) = spawn_server().await;

    let mut client1 = TestClient::connect(addr).await;
    let mut client2 = TestClient::connect(addr).await;

    // client1: WATCH foo
    client1.send_array(&["WATCH", "foo"]).await;
    let line = client1.read_simple_line().await;
    assert_eq!(line, "+OK\r\n");

    // client1: MULTI
    client1.send_array(&["MULTI"]).await;
    let _ = client1.read_simple_line().await;

    // client1: SET foo bar -> QUEUED
    client1.send_array(&["SET", "foo", "bar"]).await;
    let line = client1.read_simple_line().await;
    assert_eq!(line, "+QUEUED\r\n");

    // client2: HSET foo field value（修改 foo，应该触发 WATCH）
    client2.send_array(&["HSET", "foo", "field", "value"]).await;
    let _ = client2.read_simple_line().await;

    // client1: EXEC（应该失败，因为 foo 被 HSET 修改）
    client1.send_array(&["EXEC"]).await;
    let mut header = String::new();
    client1.reader.read_line(&mut header).await.unwrap();
    assert_eq!(
        header, "*-1\r\n",
        "EXEC should return null when WATCH fails due to HSET"
    );

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

/// 测试 WATCH 检测 LPUSH 修改（P1 修复验证）
#[tokio::test]
async fn watch_detects_lpush_modification() {
    let (addr, shutdown, handle) = spawn_server().await;

    let mut client1 = TestClient::connect(addr).await;
    let mut client2 = TestClient::connect(addr).await;

    // client1: WATCH mylist
    client1.send_array(&["WATCH", "mylist"]).await;
    let _ = client1.read_simple_line().await;

    // client1: MULTI
    client1.send_array(&["MULTI"]).await;
    let _ = client1.read_simple_line().await;

    // client1: RPUSH mylist a -> QUEUED
    client1.send_array(&["RPUSH", "mylist", "a"]).await;
    let _ = client1.read_simple_line().await;

    // client2: LPUSH mylist b（修改 mylist）
    client2.send_array(&["LPUSH", "mylist", "b"]).await;
    let _ = client2.read_simple_line().await;

    // client1: EXEC（应该失败）
    client1.send_array(&["EXEC"]).await;
    let mut header = String::new();
    client1.reader.read_line(&mut header).await.unwrap();
    assert_eq!(
        header, "*-1\r\n",
        "EXEC should return null when WATCH fails due to LPUSH"
    );

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

/// 测试事务中解析错误导致 EXEC 中止（P2 修复验证）
#[tokio::test]
async fn transaction_aborted_on_parse_error() {
    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // MULTI
    client.send_array(&["MULTI"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "+OK\r\n");

    // SET key（缺少 value，应该返回错误而不是 QUEUED）
    client.send_array(&["SET", "key"]).await;
    let line = client.read_simple_line().await;
    assert!(
        line.starts_with("-ERR"),
        "Expected error for wrong arity, got: {}",
        line
    );

    // SET key2 value2 -> QUEUED（正常命令仍然入队）
    client.send_array(&["SET", "key2", "value2"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "+QUEUED\r\n");

    // EXEC（应该返回 EXECABORT 错误）
    client.send_array(&["EXEC"]).await;
    let line = client.read_simple_line().await;
    assert!(
        line.contains("EXECABORT"),
        "Expected EXECABORT error, got: {}",
        line
    );

    // 验证 key2 没有被设置
    client.send_array(&["GET", "key2"]).await;
    let val = client.read_bulk_string().await;
    assert_eq!(
        val, None,
        "key2 should not be set after aborted transaction"
    );

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

/// 测试 DISCARD 后可以开始新事务
#[tokio::test]
async fn discard_after_parse_error_allows_new_transaction() {
    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // MULTI
    client.send_array(&["MULTI"]).await;
    let _ = client.read_simple_line().await;

    // SET key（缺少 value）
    client.send_array(&["SET", "key"]).await;
    let line = client.read_simple_line().await;
    assert!(line.starts_with("-ERR"));

    // DISCARD
    client.send_array(&["DISCARD"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "+OK\r\n");

    // 开始新事务
    client.send_array(&["MULTI"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "+OK\r\n");

    // SET key value -> QUEUED
    client.send_array(&["SET", "key", "value"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "+QUEUED\r\n");

    // EXEC（应该成功）
    client.send_array(&["EXEC"]).await;
    let mut header = String::new();
    client.reader.read_line(&mut header).await.unwrap();
    assert_eq!(header, "*1\r\n");

    // 读取 SET 结果
    let line = client.read_simple_line().await;
    assert_eq!(line, "+OK\r\n");

    // 验证 key 已设置
    client.send_array(&["GET", "key"]).await;
    let val = client.read_bulk_string().await;
    assert_eq!(val, Some("value".to_string()));

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

/// 测试 WATCH 检测 key 过期（TTL 过期时更新版本）
#[tokio::test]
async fn watch_detects_key_expiration() {
    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // 设置一个很短 TTL 的 key
    client
        .send_array(&["SET", "expiring", "value", "PX", "50"])
        .await;
    let _ = client.read_simple_line().await;

    // WATCH 这个 key
    client.send_array(&["WATCH", "expiring"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "+OK\r\n");

    // MULTI
    client.send_array(&["MULTI"]).await;
    let _ = client.read_simple_line().await;

    // 等待 key 过期
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // 触发过期检查（通过 GET）
    let mut client2 = TestClient::connect(addr).await;
    client2.send_array(&["GET", "expiring"]).await;
    let _ = client2.read_bulk_string().await;

    // SET something -> QUEUED
    client.send_array(&["SET", "other", "value"]).await;
    let _ = client.read_simple_line().await;

    // EXEC（应该失败，因为 watched key 过期了）
    client.send_array(&["EXEC"]).await;
    let mut header = String::new();
    client.reader.read_line(&mut header).await.unwrap();
    assert_eq!(
        header, "*-1\r\n",
        "EXEC should return null when watched key expires"
    );

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

/// 测试事务中 EVAL 脚本执行
#[tokio::test]
async fn eval_inside_transaction() {
    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // 设置初始值
    client.send_array(&["SET", "counter", "10"]).await;
    let _ = client.read_simple_line().await;

    // MULTI
    client.send_array(&["MULTI"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "+OK\r\n");

    // EVAL 脚本：增加 counter 并返回新值
    client
        .send_array(&[
            "EVAL",
            "return redis.call('INCRBY', KEYS[1], ARGV[1])",
            "1",
            "counter",
            "5",
        ])
        .await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "+QUEUED\r\n");

    // GET counter
    client.send_array(&["GET", "counter"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "+QUEUED\r\n");

    // EXEC
    client.send_array(&["EXEC"]).await;

    // 读取数组头
    let mut header = String::new();
    client.reader.read_line(&mut header).await.unwrap();
    assert_eq!(header, "*2\r\n");

    // 第一个结果：EVAL 返回 15
    let mut result1 = String::new();
    client.reader.read_line(&mut result1).await.unwrap();
    assert_eq!(result1, ":15\r\n");

    // 第二个结果：GET 返回 "15"
    let mut bulk_header = String::new();
    client.reader.read_line(&mut bulk_header).await.unwrap();
    assert_eq!(bulk_header, "$2\r\n");
    let mut value = String::new();
    client.reader.read_line(&mut value).await.unwrap();
    assert_eq!(value, "15\r\n");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

/// 测试事务中 EVALSHA 脚本执行
#[tokio::test]
async fn evalsha_inside_transaction() {
    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // 先加载脚本
    client
        .send_array(&[
            "SCRIPT",
            "LOAD",
            "return redis.call('SET', KEYS[1], ARGV[1])",
        ])
        .await;
    let mut bulk_header = String::new();
    client.reader.read_line(&mut bulk_header).await.unwrap();
    assert!(bulk_header.starts_with('$'));
    let len: usize = bulk_header[1..bulk_header.len() - 2].parse().unwrap();
    let mut sha1 = vec![0u8; len];
    client.reader.read_exact(&mut sha1).await.unwrap();
    let mut crlf = [0u8; 2];
    client.reader.read_exact(&mut crlf).await.unwrap();
    let sha1_str = String::from_utf8(sha1).unwrap();

    // MULTI
    client.send_array(&["MULTI"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "+OK\r\n");

    // EVALSHA
    client
        .send_array(&["EVALSHA", &sha1_str, "1", "mykey", "myvalue"])
        .await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "+QUEUED\r\n");

    // GET mykey
    client.send_array(&["GET", "mykey"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "+QUEUED\r\n");

    // EXEC
    client.send_array(&["EXEC"]).await;

    // 读取数组头
    let mut header = String::new();
    client.reader.read_line(&mut header).await.unwrap();
    assert_eq!(header, "*2\r\n");

    // 第一个结果：SET 返回 OK
    let mut result1 = String::new();
    client.reader.read_line(&mut result1).await.unwrap();
    assert_eq!(result1, "+OK\r\n");

    // 第二个结果：GET 返回 "myvalue"
    let mut bulk_header2 = String::new();
    client.reader.read_line(&mut bulk_header2).await.unwrap();
    assert_eq!(bulk_header2, "$7\r\n");
    let mut value = String::new();
    client.reader.read_line(&mut value).await.unwrap();
    assert_eq!(value, "myvalue\r\n");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

/// 测试事务中 SCRIPT LOAD/EXISTS 命令
#[tokio::test]
async fn script_commands_inside_transaction() {
    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // MULTI
    client.send_array(&["MULTI"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "+OK\r\n");

    // SCRIPT LOAD
    client.send_array(&["SCRIPT", "LOAD", "return 42"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "+QUEUED\r\n");

    // EXEC
    client.send_array(&["EXEC"]).await;

    // 读取数组头
    let mut header = String::new();
    client.reader.read_line(&mut header).await.unwrap();
    assert_eq!(header, "*1\r\n");

    // 读取 SHA1
    let mut bulk_header = String::new();
    client.reader.read_line(&mut bulk_header).await.unwrap();
    assert!(bulk_header.starts_with('$'));
    let len: usize = bulk_header[1..bulk_header.len() - 2].parse().unwrap();
    assert_eq!(len, 40); // SHA1 是 40 个十六进制字符

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}
