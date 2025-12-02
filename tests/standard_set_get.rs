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

#[tokio::test]
async fn mset_and_mget_roundtrip() {
    let (addr, shutdown, handle) = spawn_server().await;

    let mut client = TestClient::connect(addr).await;

    client.mset(&[("k1", "v1"), ("k2", "v2")]).await;

    let values = client.mget(&["k1", "k2", "missing"]).await;
    assert_eq!(values.len(), 3);
    assert_eq!(values[0].as_deref(), Some("v1"));
    assert_eq!(values[1].as_deref(), Some("v2"));
    assert_eq!(values[2], None);

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn set_with_exat_and_pxat_options() {
    use std::time::{SystemTime, UNIX_EPOCH};

    let (addr, shutdown, handle) = spawn_server().await;

    let mut client = TestClient::connect(addr).await;

    // 基本 EXAT 行为：设置绝对过期时间，TTL 应接近给定秒数
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let target_secs = now_secs + 2;

    client
        .send_array(&["SET", "exat", "v", "EXAT", &target_secs.to_string()])
        .await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "+OK\r\n");

    client.send_array(&["TTL", "exat"]).await;
    let ttl_line = client.read_simple_line().await;
    assert!(ttl_line.starts_with(":"));
    let ttl: i64 = ttl_line[1..ttl_line.len() - 2].parse().unwrap();
    // 允许一定抖动：应在 [0, 2] 区间内且不为负值（负值表示 key 已过期）
    assert!(ttl >= 0 && ttl <= 2);

    // 基本 PXAT 行为：设置绝对毫秒时间，PTTL 应较小但为正
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let target_ms = now_ms + 1500; // ~1.5s

    client
        .send_array(&["SET", "pxat", "v", "PXAT", &target_ms.to_string()])
        .await;
    let line2 = client.read_simple_line().await;
    assert_eq!(line2, "+OK\r\n");

    client.send_array(&["PTTL", "pxat"]).await;
    let pttl_line = client.read_simple_line().await;
    assert!(pttl_line.starts_with(":"));
    let pttl: i64 = pttl_line[1..pttl_line.len() - 2].parse().unwrap();
    // 预期在 (0, 1500]ms 之间
    assert!(pttl > 0 && pttl <= 1500);

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn setrange_does_not_truncate_tail() {
    let (addr, shutdown, handle) = spawn_server().await;

    let mut client = TestClient::connect(addr).await;

    // 初始值为 abcdef
    client.set("foo", "abcdef").await;

    // 在偏移 3 位置写入 "X"，应只覆盖一个字节，不截断尾部
    let new_len = client.setrange("foo", 3, "X").await;
    assert_eq!(new_len, 6); // Redis 返回 key 的最终长度

    let v = client.get("foo").await;
    assert_eq!(v.as_deref(), Some("abcXef"));

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn msetnx_atomicity_and_existing_keys() {
    let (addr, shutdown, handle) = spawn_server().await;

    let mut client = TestClient::connect(addr).await;

    // 所有 key 都不存在时，应一次性写入并返回 :1
    client.send_array(&["MSETNX", "k1", "v1", "k2", "v2"]).await;
    let line1 = client.read_simple_line().await;
    assert_eq!(line1, ":1\r\n");

    let v1 = client.get("k1").await;
    let v2 = client.get("k2").await;
    assert_eq!(v1.as_deref(), Some("v1"));
    assert_eq!(v2.as_deref(), Some("v2"));

    // 其中一个 key 已存在时，整个 MSETNX 不应写入任何 key
    client.set("k3", "old").await;
    client
        .send_array(&["MSETNX", "k3", "new", "k4", "v4"])
        .await;
    let line2 = client.read_simple_line().await;
    assert_eq!(line2, ":0\r\n");

    let v3 = client.get("k3").await;
    let v4 = client.get("k4").await;
    assert_eq!(v3.as_deref(), Some("old"));
    assert_eq!(v4, None);

    // 对比：MSET 在参数合法时始终写入
    client.mset(&[("k5", "a"), ("k6", "b")]).await;
    let v5 = client.get("k5").await;
    let v6 = client.get("k6").await;
    assert_eq!(v5.as_deref(), Some("a"));
    assert_eq!(v6.as_deref(), Some("b"));

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn incrbyfloat_roundtrip_and_error_cases() {
    let (addr, shutdown, handle) = spawn_server().await;

    let mut client = TestClient::connect(addr).await;

    // 不存在的 key 视为 0，自增浮点
    client.send_array(&["INCRBYFLOAT", "f1", "1.5"]).await;
    let v1 = client.read_bulk().await;
    assert_eq!(v1.as_deref(), Some("1.5"));

    // 再加 2.25 -> 3.75
    client.send_array(&["INCRBYFLOAT", "f1", "2.25"]).await;
    let v2 = client.read_bulk().await;
    assert_eq!(v2.as_deref(), Some("3.75"));

    // 对已是浮点字符串的 key 调用 INCRBY 应返回整数错误，不修改值
    client.send_array(&["INCRBY", "f1", "2"]).await;
    let int_err = client.read_simple_line().await;
    assert!(int_err.starts_with("-ERR value is not an integer or out of range"));

    // 继续用 INCRBYFLOAT 自增：3.75 + 0.75 = 4.5
    client.send_array(&["INCRBYFLOAT", "f1", "0.75"]).await;
    let v4 = client.read_bulk().await;
    assert_eq!(v4.as_deref(), Some("4.5"));

    // 非数字值应返回浮点错误
    client.set("notfloat", "abc").await;
    client.send_array(&["INCRBYFLOAT", "notfloat", "1.0"]).await;
    let err = client.read_simple_line().await;
    assert!(err.starts_with("-ERR"));

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn getrange_and_setrange_roundtrip() {
    let (addr, shutdown, handle) = spawn_server().await;

    let mut client = TestClient::connect(addr).await;

    // 对不存在的 key GETRANGE -> 空串
    let r0 = client.getrange("missing", 0, -1).await;
    assert_eq!(r0, "");

    // 基本 GETRANGE 行为
    client.set("foo", "foobar").await;
    let r1 = client.getrange("foo", 0, 2).await;
    assert_eq!(r1, "foo");
    let r2 = client.getrange("foo", 3, -1).await;
    assert_eq!(r2, "bar");
    let r3 = client.getrange("foo", -2, -1).await;
    assert_eq!(r3, "ar");

    // 越界下标裁剪为合法范围
    let r4 = client.getrange("foo", -100, 100).await;
    assert_eq!(r4, "foobar");

    // SETRANGE 覆盖已有值但不截断尾部：foobar -> fooZZr
    let new_len = client.setrange("foo", 3, "ZZ").await;
    assert_eq!(new_len, 6);
    let v = client.get("foo").await;
    assert_eq!(v.as_deref(), Some("fooZZr"));

    // 非 string 类型调用 GETRANGE/SETRANGE -> WRONGTYPE
    client.send_array(&["LPUSH", "list", "v1"]).await;
    let _ = client.read_simple_line().await; // :1

    client.send_array(&["GETRANGE", "list", "0", "-1"]).await;
    let line = client.read_simple_line().await;
    assert!(line.starts_with("-WRONGTYPE"));

    client.send_array(&["SETRANGE", "list", "0", "x"]).await;
    let line2 = client.read_simple_line().await;
    assert!(line2.starts_with("-WRONGTYPE"));

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn getrange_returns_raw_bytes_on_multibyte_values() {
    let (addr, shutdown, handle) = spawn_server().await;

    let mut client = TestClient::connect(addr).await;

    // "é" -> [0xC3, 0xA9]; requesting the second byte should return 0xA9
    client.set_bytes("u", "é".as_bytes()).await;
    let slice = client.getrange_bytes("u", 1, 1).await;
    assert_eq!(slice, vec![0xA9]);

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn setrange_overwrites_bytes_without_truncating_multibyte_values() {
    let (addr, shutdown, handle) = spawn_server().await;

    let mut client = TestClient::connect(addr).await;

    // "你好" bytes: e4 bd a0 e5 a5 bd
    client.set_bytes("wide", "你好".as_bytes()).await;

    // Overwrite the second byte with 'X' (0x58), producing invalid UTF-8 but keeping length
    let new_len = client.setrange_bytes("wide", 1, b"X").await;
    assert_eq!(new_len, 6);

    let bytes = client.get_bytes("wide").await.unwrap();
    assert_eq!(bytes, vec![0xE4, 0x58, 0xA0, 0xE5, 0xA5, 0xBD]);

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn set_with_nx_xx_semantics() {
    let (addr, shutdown, handle) = spawn_server().await;

    let mut client = TestClient::connect(addr).await;

    // 初次 SET foo bar NX -> 应写入
    client.send_array(&["SET", "foo", "bar", "NX"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "+OK\r\n");

    // 再次 SET foo baz NX -> 条件失败，返回 null bulk
    client.send_array(&["SET", "foo", "baz", "NX"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "$-1\r\n");

    // SET foo qux XX -> 应覆盖
    client.send_array(&["SET", "foo", "qux", "XX"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "+OK\r\n");

    let v = client.get("foo").await;
    assert_eq!(v.as_deref(), Some("qux"));

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn set_with_get_option() {
    let (addr, shutdown, handle) = spawn_server().await;

    let mut client = TestClient::connect(addr).await;

    // 初次 SET foo bar，无 GET
    client.set("foo", "bar").await;

    // SET foo baz GET -> 返回旧值 bar，key 变为 baz
    client.send_array(&["SET", "foo", "baz", "GET"]).await;
    let old = client.read_bulk().await;
    assert_eq!(old.as_deref(), Some("bar"));

    let v = client.get("foo").await;
    assert_eq!(v.as_deref(), Some("baz"));

    // SET foo qux NX GET，在 key 已存在时条件失败，返回当前值 baz 且不覆盖
    client.send_array(&["SET", "foo", "qux", "NX", "GET"]).await;
    let old2 = client.read_bulk().await;
    assert_eq!(old2.as_deref(), Some("baz"));

    let v2 = client.get("foo").await;
    assert_eq!(v2.as_deref(), Some("baz"));

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn set_with_keepttl_option() {
    let (addr, shutdown, handle) = spawn_server().await;

    let mut client = TestClient::connect(addr).await;

    // 使用 EX 设置带 TTL 的 key
    client.send_array(&["SET", "foo", "bar", "EX", "2"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "+OK\r\n");

    // 读取 TTL，确认有 TTL 存在
    client.send_array(&["TTL", "foo"]).await;
    let ttl_before = client.read_simple_line().await;
    assert!(ttl_before.starts_with(":"));

    // 使用 KEEPTTL 覆盖 value，TTL 应保留
    client.send_array(&["SET", "foo", "baz", "KEEPTTL"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "+OK\r\n");

    client.send_array(&["TTL", "foo"]).await;
    let ttl_after = client.read_simple_line().await;
    assert!(ttl_after.starts_with(":"));

    // 等待一段时间后 key 应过期（说明 TTL 仍然生效）
    tokio::time::sleep(std::time::Duration::from_millis(2200)).await;
    let v = client.get("foo").await;
    assert_eq!(v, None);

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn getdel_basic_behaviour_and_wrongtype() {
    let (addr, shutdown, handle) = spawn_server().await;

    let mut client = TestClient::connect(addr).await;

    // key 不存在时返回 nil
    client.send_array(&["GETDEL", "missing"]).await;
    let missing = client.read_bulk().await;
    assert_eq!(missing, None);

    // 基本 GETDEL 行为：返回旧值并删除 key
    client.set("foo", "bar").await;
    client.send_array(&["GETDEL", "foo"]).await;
    let old = client.read_bulk().await;
    assert_eq!(old.as_deref(), Some("bar"));

    let after = client.get("foo").await;
    assert_eq!(after, None);

    // 非 string 类型调用 GETDEL -> WRONGTYPE
    client.send_array(&["LPUSH", "list", "v1"]).await;
    let _ = client.read_simple_line().await; // :1

    client.send_array(&["GETDEL", "list"]).await;
    let line = client.read_simple_line().await;
    assert!(line.starts_with("-WRONGTYPE"));

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn getex_updates_ttl_and_persist_clears_it() {
    let (addr, shutdown, handle) = spawn_server().await;

    let mut client = TestClient::connect(addr).await;

    // 使用 SET EX 设置初始 TTL
    client
        .send_array(&["SET", "foo", "bar", "EX", "1"]) // 1 秒过期
        .await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "+OK\r\n");

    // GETEX foo EX 3 -> 返回当前值，并将 TTL 延长到 ~3s
    client.send_array(&["GETEX", "foo", "EX", "3"]).await;
    let v = client.read_bulk().await;
    assert_eq!(v.as_deref(), Some("bar"));

    client.send_array(&["TTL", "foo"]).await;
    let ttl_line = client.read_simple_line().await;
    assert!(ttl_line.starts_with(":"));

    // 使用 GETEX foo PX 500 缩短 TTL 到 500ms 内
    client.send_array(&["GETEX", "foo", "PX", "500"]).await;
    let v2 = client.read_bulk().await;
    assert_eq!(v2.as_deref(), Some("bar"));

    client.send_array(&["PTTL", "foo"]).await;
    let pttl_line = client.read_simple_line().await;
    assert!(pttl_line.starts_with(":"));

    // 使用 GETEX foo PERSIST 清除 TTL
    client.send_array(&["GETEX", "foo", "PERSIST"]).await;
    let v3 = client.read_bulk().await;
    assert_eq!(v3.as_deref(), Some("bar"));

    client.send_array(&["TTL", "foo"]).await;
    let ttl_after = client.read_simple_line().await;
    assert_eq!(ttl_after, ":-1\r\n");

    // 对不存在 key 的 GETEX -> 返回 nil，不设置 TTL
    client.send_array(&["GETEX", "missing", "EX", "10"]).await;
    let missing = client.read_bulk().await;
    assert_eq!(missing, None);

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
    assert_eq!(
        line,
        "-ERR wrong number of arguments for 'expire' command\r\n"
    );

    // EXPIRE with non-integer seconds -> ERR value is not an integer or out of range
    client.send_array(&["EXPIRE", "foo", "notint"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "-ERR value is not an integer or out of range\r\n");

    // SET EX 与 PX/EXAT/PXAT 组合冲突 -> 语法错误
    client
        .send_array(&["SET", "foo", "bar", "EX", "10", "PX", "1000"])
        .await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "-ERR syntax error\r\n");

    client
        .send_array(&["SET", "foo", "bar", "EX", "10", "EXAT", "100"])
        .await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "-ERR syntax error\r\n");

    client
        .send_array(&["SET", "foo", "bar", "PXAT", "1000", "EXAT", "100"])
        .await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "-ERR syntax error\r\n");

    // EXAT/PXAT 参数为非整数（或负数）时，返回整数错误
    client
        .send_array(&["SET", "foo", "bar", "EXAT", "-1"])
        .await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "-ERR value is not an integer or out of range\r\n");

    client
        .send_array(&["SET", "foo", "bar", "PXAT", "notint"])
        .await;
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

    // LLEN with missing key -> ERR wrong number of arguments for 'llen' command
    client.send_array(&["LLEN"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(
        line,
        "-ERR wrong number of arguments for 'llen' command\r\n"
    );

    // LLEN with extra argument -> same wrong-args error
    client.send_array(&["LLEN", "foo", "extra"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(
        line,
        "-ERR wrong number of arguments for 'llen' command\r\n"
    );

    // LINDEX with non-integer index -> integer error
    client.send_array(&["LINDEX", "foo", "notint"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "-ERR value is not an integer or out of range\r\n");

    // HGETALL with wrong arg count -> wrong-args error
    client.send_array(&["HGETALL"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(
        line,
        "-ERR wrong number of arguments for 'hgetall' command\r\n"
    );

    client.send_array(&["HGETALL", "foo", "extra"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(
        line,
        "-ERR wrong number of arguments for 'hgetall' command\r\n"
    );

    // SCARD with wrong arg count -> wrong-args error
    client.send_array(&["SCARD"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(
        line,
        "-ERR wrong number of arguments for 'scard' command\r\n"
    );

    client.send_array(&["SCARD", "foo", "extra"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(
        line,
        "-ERR wrong number of arguments for 'scard' command\r\n"
    );

    // SISMEMBER with wrong arg count -> wrong-args error
    client.send_array(&["SISMEMBER"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(
        line,
        "-ERR wrong number of arguments for 'sismember' command\r\n"
    );

    client.send_array(&["SISMEMBER", "foo"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(
        line,
        "-ERR wrong number of arguments for 'sismember' command\r\n"
    );

    client
        .send_array(&["SISMEMBER", "foo", "bar", "extra"])
        .await;
    let line = client.read_simple_line().await;
    assert_eq!(
        line,
        "-ERR wrong number of arguments for 'sismember' command\r\n"
    );

    // SUNION/SINTER/SDIFF without any key -> wrong-args error
    client.send_array(&["SUNION"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(
        line,
        "-ERR wrong number of arguments for 'sunion' command\r\n"
    );

    client.send_array(&["SINTER"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(
        line,
        "-ERR wrong number of arguments for 'sinter' command\r\n"
    );

    client.send_array(&["SDIFF"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(
        line,
        "-ERR wrong number of arguments for 'sdiff' command\r\n"
    );

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

#[tokio::test]
async fn append_and_strlen_roundtrip() {
    let (addr, shutdown, handle) = spawn_server().await;

    let mut client = TestClient::connect(addr).await;

    // 对不存在的 key append，相当于新建
    let len1 = client.append("foo", "bar").await;
    assert_eq!(len1, 3);
    let v1 = client.get("foo").await;
    assert_eq!(v1.as_deref(), Some("bar"));

    // 再次 append
    let len2 = client.append("foo", "baz").await;
    assert_eq!(len2, 6);
    let v2 = client.get("foo").await;
    assert_eq!(v2.as_deref(), Some("barbaz"));

    // STRLEN 返回长度
    let slen = client.strlen("foo").await;
    assert_eq!(slen, 6);

    // 非 string 类型调用 APPEND/STRLEN -> WRONGTYPE
    client.send_array(&["LPUSH", "list", "v1"]).await;
    let _ = client.read_simple_line().await; // :1

    client.send_array(&["APPEND", "list", "x"]).await;
    let line = client.read_simple_line().await;
    assert!(line.starts_with("-WRONGTYPE"));

    client.send_array(&["STRLEN", "list"]).await;
    let line2 = client.read_simple_line().await;
    assert!(line2.starts_with("-WRONGTYPE"));

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn getset_basic_behaviour() {
    let (addr, shutdown, handle) = spawn_server().await;

    let mut client = TestClient::connect(addr).await;

    // key 不存在时返回 nil，并设置新值
    let old1 = client.getset("foo", "bar").await;
    assert_eq!(old1, None);
    let v1 = client.get("foo").await;
    assert_eq!(v1.as_deref(), Some("bar"));

    // key 存在时返回旧值并覆盖
    let old2 = client.getset("foo", "baz").await;
    assert_eq!(old2.as_deref(), Some("bar"));
    let v2 = client.get("foo").await;
    assert_eq!(v2.as_deref(), Some("baz"));

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn getset_preserves_ttl() {
    let (addr, shutdown, handle) = spawn_server().await;

    let mut client = TestClient::connect(addr).await;

    // 先用 SET EX 设置 TTL
    client.send_array(&["SET", "foo", "bar", "EX", "1"]).await;
    let line = client.read_simple_line().await;
    assert_eq!(line, "+OK\r\n");

    // 稍等一会儿，但先不要等到过期
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;

    // GETSET 返回旧值，并且不应清除 TTL
    let old = client.getset("foo", "baz").await;
    assert_eq!(old.as_deref(), Some("bar"));

    // 立即 TTL，应仍然 >=0
    client.send_array(&["TTL", "foo"]).await;
    let ttl_line = client.read_simple_line().await;
    assert!(ttl_line.starts_with(":"));

    // 再等一会儿让 key 过期
    tokio::time::sleep(std::time::Duration::from_millis(700)).await;
    let v = client.get("foo").await;
    assert_eq!(v, None);

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

    async fn send_array_bytes(&mut self, parts: &[&[u8]]) {
        let mut buf = Vec::new();
        buf.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
        for p in parts {
            buf.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
            buf.extend_from_slice(p);
            buf.extend_from_slice(b"\r\n");
        }
        self.writer.write_all(&buf).await.unwrap();
    }

    async fn read_simple_line(&mut self) -> String {
        let mut line = String::new();
        self.reader.read_line(&mut line).await.unwrap();
        line
    }

    async fn read_bulk_bytes(&mut self) -> Option<Vec<u8>> {
        let mut header = String::new();
        self.reader.read_line(&mut header).await.unwrap();

        if header == "$-1\r\n" {
            return None;
        }

        assert!(header.starts_with('$'));
        let len_str = &header[1..header.len() - 2];
        let len: usize = len_str.parse().unwrap();

        let mut value = vec![0u8; len];
        self.reader.read_exact(&mut value).await.unwrap();

        // consume CRLF
        let mut crlf = [0u8; 2];
        self.reader.read_exact(&mut crlf).await.unwrap();
        assert_eq!(&crlf, b"\r\n");

        Some(value)
    }

    async fn read_bulk(&mut self) -> Option<String> {
        self.read_bulk_bytes()
            .await
            .map(|b| String::from_utf8(b).unwrap())
    }

    async fn set(&mut self, key: &str, value: &str) {
        self.send_array(&["SET", key, value]).await;
        let line = self.read_simple_line().await;
        assert_eq!(line, "+OK\r\n");
    }

    async fn set_bytes(&mut self, key: &str, value: &[u8]) {
        self.send_array_bytes(&[b"SET", key.as_bytes(), value])
            .await;
        let line = self.read_simple_line().await;
        assert_eq!(line, "+OK\r\n");
    }

    async fn get(&mut self, key: &str) -> Option<String> {
        self.send_array(&["GET", key]).await;
        self.read_bulk().await
    }

    async fn get_bytes(&mut self, key: &str) -> Option<Vec<u8>> {
        self.send_array(&["GET", key]).await;
        self.read_bulk_bytes().await
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
            let item = self.read_bulk_bytes().await;
            let converted = item.map(|b| String::from_utf8(b).unwrap());
            result.push(converted);
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

    async fn dbsize(&mut self) -> i64 {
        self.send_array(&["DBSIZE"]).await;
        let line = self.read_simple_line().await;
        assert!(line.starts_with(":"));
        line[1..line.len() - 2].parse().unwrap()
    }

    async fn append(&mut self, key: &str, value: &str) -> i64 {
        self.send_array(&["APPEND", key, value]).await;
        let line = self.read_simple_line().await;
        assert!(line.starts_with(":"));
        line[1..line.len() - 2].parse().unwrap()
    }

    async fn strlen(&mut self, key: &str) -> i64 {
        self.send_array(&["STRLEN", key]).await;
        let line = self.read_simple_line().await;
        assert!(line.starts_with(":"));
        line[1..line.len() - 2].parse().unwrap()
    }

    async fn getset(&mut self, key: &str, value: &str) -> Option<String> {
        self.send_array(&["GETSET", key, value]).await;
        self.read_bulk().await
    }

    async fn getrange(&mut self, key: &str, start: isize, end: isize) -> String {
        let start_s = start.to_string();
        let end_s = end.to_string();
        self.send_array(&["GETRANGE", key, &start_s, &end_s]).await;
        // GETRANGE 对不存在 key 返回空串（长度 0 的 bulk），不会返回 nil
        self.read_bulk().await.unwrap_or_default()
    }

    async fn getrange_bytes(&mut self, key: &str, start: isize, end: isize) -> Vec<u8> {
        let start_s = start.to_string();
        let end_s = end.to_string();
        self.send_array(&["GETRANGE", key, &start_s, &end_s]).await;
        self.read_bulk_bytes().await.unwrap_or_default()
    }

    async fn setrange(&mut self, key: &str, offset: usize, value: &str) -> i64 {
        let off_s = offset.to_string();
        self.send_array(&["SETRANGE", key, &off_s, value]).await;
        let line = self.read_simple_line().await;
        assert!(line.starts_with(":"));
        line[1..line.len() - 2].parse().unwrap()
    }

    async fn setrange_bytes(&mut self, key: &str, offset: usize, value: &[u8]) -> i64 {
        let off_s = offset.to_string();
        self.send_array_bytes(&[b"SETRANGE", key.as_bytes(), off_s.as_bytes(), value])
            .await;
        let line = self.read_simple_line().await;
        assert!(line.starts_with(":"));
        line[1..line.len() - 2].parse().unwrap()
    }
}

#[tokio::test]
async fn basic_dbsize_behaviour() {
    let (addr, _shutdown, _handle) = spawn_server().await;

    let mut client = TestClient::connect(addr).await;

    // 初始应为 0
    let initial = client.dbsize().await;
    assert_eq!(initial, 0);

    // 写入若干 key
    client.set("k1", "v1").await;
    client.set("k2", "v2").await;
    client.set("k3", "v3").await;

    let after_set = client.dbsize().await;
    assert_eq!(after_set, 3);

    // 删除一个 key 后，dbsize 应减少
    client.send_array(&["DEL", "k1"]).await;
    let _ = client.read_simple_line().await; // :1
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
    client.send_array(&["SET", "foo", "bar", "EX", "10"]).await;
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
