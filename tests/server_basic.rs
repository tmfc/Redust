use std::env;
use std::net::SocketAddr;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::time::{Duration, Instant};

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

async fn send_array(writer: &mut tokio::net::tcp::OwnedWriteHalf, parts: &[&str]) {
    let mut buf = format!("*{}\r\n", parts.len());
    for p in parts {
        buf.push_str(&format!("${}\r\n{}\r\n", p.len(), p));
    }
    writer.write_all(buf.as_bytes()).await.unwrap();
}

#[tokio::test]
async fn responds_to_basic_commands() {
    let (addr, shutdown, handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    write_half.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
    let mut pong = String::new();
    reader.read_line(&mut pong).await.unwrap();
    assert_eq!(pong, "+PONG\r\n");

    write_half
        .write_all(b"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n")
        .await
        .unwrap();
    let mut bulk_header = String::new();
    reader.read_line(&mut bulk_header).await.unwrap();
    assert_eq!(bulk_header, "$5\r\n");
    let mut bulk_value = String::new();
    reader.read_line(&mut bulk_value).await.unwrap();
    assert_eq!(bulk_value, "hello\r\n");

    // SET key
    write_half
        .write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
        .await
        .unwrap();
    let mut ok = String::new();
    reader.read_line(&mut ok).await.unwrap();
    assert_eq!(ok, "+OK\r\n");

    // GET key
    write_half
        .write_all(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")
        .await
        .unwrap();
    let mut get_header = String::new();
    reader.read_line(&mut get_header).await.unwrap();
    assert_eq!(get_header, "$3\r\n");
    let mut get_value = String::new();
    reader.read_line(&mut get_value).await.unwrap();
    assert_eq!(get_value, "bar\r\n");

    // EXISTS key
    write_half
        .write_all(b"*2\r\n$6\r\nEXISTS\r\n$3\r\nfoo\r\n")
        .await
        .unwrap();
    let mut exists_resp = String::new();
    reader.read_line(&mut exists_resp).await.unwrap();
    assert_eq!(exists_resp, ":1\r\n");

    // DEL key
    write_half
        .write_all(b"*2\r\n$3\r\nDEL\r\n$3\r\nfoo\r\n")
        .await
        .unwrap();
    let mut del_resp = String::new();
    reader.read_line(&mut del_resp).await.unwrap();
    assert_eq!(del_resp, ":1\r\n");

    // GET missing key -> nil bulk string
    write_half
        .write_all(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")
        .await
        .unwrap();
    let mut nil_resp = String::new();
    reader.read_line(&mut nil_resp).await.unwrap();
    assert_eq!(nil_resp, "$-1\r\n");

    // INCR on new key -> 1
    write_half
        .write_all(b"*2\r\n$4\r\nINCR\r\n$3\r\ncnt\r\n")
        .await
        .unwrap();
    let mut incr_resp = String::new();
    reader.read_line(&mut incr_resp).await.unwrap();
    assert_eq!(incr_resp, ":1\r\n");

    // INCR again -> 2
    write_half
        .write_all(b"*2\r\n$4\r\nINCR\r\n$3\r\ncnt\r\n")
        .await
        .unwrap();
    let mut incr_resp2 = String::new();
    reader.read_line(&mut incr_resp2).await.unwrap();
    assert_eq!(incr_resp2, ":2\r\n");

    // DECR -> 1
    write_half
        .write_all(b"*2\r\n$4\r\nDECR\r\n$3\r\ncnt\r\n")
        .await
        .unwrap();
    let mut decr_resp = String::new();
    reader.read_line(&mut decr_resp).await.unwrap();
    assert_eq!(decr_resp, ":1\r\n");

    // TYPE existing key -> string
    write_half
        .write_all(b"*2\r\n$4\r\nTYPE\r\n$3\r\ncnt\r\n")
        .await
        .unwrap();
    let mut type_resp = String::new();
    reader.read_line(&mut type_resp).await.unwrap();
    assert_eq!(type_resp, "+string\r\n");

    // TYPE missing key -> none
    write_half
        .write_all(b"*2\r\n$4\r\nTYPE\r\n$7\r\nmissing\r\n")
        .await
        .unwrap();
    let mut type_none = String::new();
    reader.read_line(&mut type_none).await.unwrap();
    assert_eq!(type_none, "+none\r\n");

    // KEYS exact match pattern
    write_half
        .write_all(b"*2\r\n$4\r\nKEYS\r\n$3\r\ncnt\r\n")
        .await
        .unwrap();
    let mut array_header = String::new();
    reader.read_line(&mut array_header).await.unwrap();
    assert_eq!(array_header, "*1\r\n");
    let mut bulk_header2 = String::new();
    reader.read_line(&mut bulk_header2).await.unwrap();
    assert_eq!(bulk_header2, "$3\r\n");
    let mut key_value = String::new();
    reader.read_line(&mut key_value).await.unwrap();
    assert_eq!(key_value, "cnt\r\n");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn rename_and_flush_commands_behaviour() {
    let (addr, shutdown, handle) = spawn_server().await;

    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // 在 DB0: SET k v0
    write_half
        .write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$2\r\nv0\r\n")
        .await
        .unwrap();
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "+OK\r\n");

    // RENAME k k2 -> OK, k 消失, k2 变为 v0
    write_half
        .write_all(b"*3\r\n$6\r\nRENAME\r\n$1\r\nk\r\n$2\r\nk2\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "+OK\r\n");

    // GET k -> nil
    write_half
        .write_all(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "$-1\r\n");

    // GET k2 -> v0
    write_half
        .write_all(b"*2\r\n$3\r\nGET\r\n$2\r\nk2\r\n")
        .await
        .unwrap();
    let mut bulk_header = String::new();
    let mut value = String::new();
    reader.read_line(&mut bulk_header).await.unwrap();
    reader.read_line(&mut value).await.unwrap();
    assert_eq!(bulk_header, "$2\r\n");
    assert_eq!(value, "v0\r\n");

    // RENAME missing -> ERR no such key
    write_half
        .write_all(b"*3\r\n$6\r\nRENAME\r\n$7\r\nmissing\r\n$1\r\nx\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "-ERR no such key\r\n");

    // 准备 RENAMENX: SET a 1, SET b 2
    write_half
        .write_all(b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "+OK\r\n");

    write_half
        .write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "+OK\r\n");

    // RENAMENX a b -> 0 (b 已存在)
    write_half
        .write_all(b"*3\r\n$8\r\nRENAMENX\r\n$1\r\na\r\n$1\r\nb\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, ":0\r\n");

    // RENAMENX a c -> 1 (a -> c)
    write_half
        .write_all(b"*3\r\n$8\r\nRENAMENX\r\n$1\r\na\r\n$1\r\nc\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, ":1\r\n");

    // 切换到 DB1
    write_half
        .write_all(b"*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "+OK\r\n");

    // 在 DB1: SET x v1
    write_half
        .write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$2\r\nv1\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "+OK\r\n");

    // FLUSHDB (当前 DB1)
    write_half
        .write_all(b"*1\r\n$7\r\nFLUSHDB\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "+OK\r\n");

    // DB1 中 GET x -> nil
    write_half
        .write_all(b"*2\r\n$3\r\nGET\r\n$1\r\nx\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "$-1\r\n");

    // 切回 DB0，确认在 FLUSHALL 前还有 key
    write_half
        .write_all(b"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "+OK\r\n");

    // DBSIZE 应该大于 0
    write_half
        .write_all(b"*1\r\n$6\r\nDBSIZE\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert!(line.starts_with(":"));

    // FLUSHALL 清空所有 DB
    write_half
        .write_all(b"*1\r\n$8\r\nFLUSHALL\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "+OK\r\n");

    // DBSIZE 再次查看，应为 0
    write_half
        .write_all(b"*1\r\n$6\r\nDBSIZE\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, ":0\r\n");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn wrongtype_errors_for_sets_and_hashes() {
    let (addr, shutdown, handle) = spawn_server().await;

    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // SET strkey "v"
    let set_req = "*3\r\n$3\r\nSET\r\n$6\r\nstrkey\r\n$1\r\nv\r\n";
    write_half.write_all(set_req.as_bytes()).await.unwrap();
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "+OK\r\n");

    // SADD strkey a  -> WRONGTYPE (string vs set)
    let sadd_wrong = "*3\r\n$4\r\nSADD\r\n$6\r\nstrkey\r\n$1\r\na\r\n";
    write_half.write_all(sadd_wrong.as_bytes()).await.unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(
        line,
        "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
    );

    // HSET strkey f v -> WRONGTYPE (string vs hash)
    let hset_wrong = "*4\r\n$4\r\nHSET\r\n$6\r\nstrkey\r\n$1\r\nf\r\n$1\r\nv\r\n";
    write_half.write_all(hset_wrong.as_bytes()).await.unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(
        line,
        "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
    );

    // 建一个真正的 set，再对它做 list/hash 操作
    let sadd_real = "*3\r\n$4\r\nSADD\r\n$5\r\nmyset\r\n$1\r\na\r\n";
    write_half.write_all(sadd_real.as_bytes()).await.unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, ":1\r\n");

    // SPOP myset -> one element
    write_half
        .write_all(b"*2\r\n$4\r\nSPOP\r\n$5\r\nmyset\r\n")
        .await
        .unwrap();
    let mut bulk_header = String::new();
    let mut bulk_value = String::new();
    reader.read_line(&mut bulk_header).await.unwrap();
    reader.read_line(&mut bulk_value).await.unwrap();
    assert!(bulk_header.starts_with("$"));

    // Refill set for SRANDMEMBER tests
    write_half
        .write_all(b"*4\r\n$4\r\nSADD\r\n$5\r\nmyset\r\n$1\r\na\r\n$1\r\nb\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();

    // SRANDMEMBER myset -> single element
    write_half
        .write_all(b"*2\r\n$11\r\nSRANDMEMBER\r\n$5\r\nmyset\r\n")
        .await
        .unwrap();
    bulk_header.clear();
    bulk_value.clear();
    reader.read_line(&mut bulk_header).await.unwrap();
    reader.read_line(&mut bulk_value).await.unwrap();
    assert!(bulk_header.starts_with("$"));

    // SRANDMEMBER myset 2 -> array of up to 2 distinct elements
    write_half
        .write_all(b"*3\r\n$11\r\nSRANDMEMBER\r\n$5\r\nmyset\r\n$1\r\n2\r\n")
        .await
        .unwrap();
    let mut arr_header = String::new();
    reader.read_line(&mut arr_header).await.unwrap();
    assert!(arr_header.starts_with("*"));
    // 将 SRANDMEMBER 返回的数组元素读完以对齐后续协议流
    if let Some(len_str) = arr_header.strip_prefix("*") {
        if let Some(len_str) = len_str.strip_suffix("\r\n") {
            if let Ok(n) = len_str.parse::<usize>() {
                for _ in 0..n {
                    bulk_header.clear();
                    bulk_value.clear();
                    // 读取每个成员的 bulk header 和内容
                    reader.read_line(&mut bulk_header).await.unwrap(); // $len\r\n
                    if bulk_header.starts_with("$") {
                        reader.read_line(&mut bulk_value).await.unwrap(); // value\r\n
                    } else {
                        break;
                    }
                }
            }
        }
    }

    // LPUSH myset x -> WRONGTYPE (set vs list)
    let lpush_wrong = "*3\r\n$5\r\nLPUSH\r\n$5\r\nmyset\r\n$1\r\nx\r\n";
    write_half.write_all(lpush_wrong.as_bytes()).await.unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(
        line,
        "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
    );

    // HGET myset f -> WRONGTYPE (set vs hash)
    let hget_wrong = "*3\r\n$4\r\nHGET\r\n$5\r\nmyset\r\n$1\r\nf\r\n";
    write_half.write_all(hget_wrong.as_bytes()).await.unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(
        line,
        "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
    );

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn select_command_isolates_databases() {
    let (addr, shutdown, handle) = spawn_server().await;

    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // 默认在 DB0：SET k v0
    write_half
        .write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$2\r\nv0\r\n")
        .await
        .unwrap();
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "+OK\r\n");

    // 切到 DB1：SELECT 1
    write_half
        .write_all(b"*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "+OK\r\n");

    // 在 DB1 读取 k，应该看不到 DB0 里的值
    write_half
        .write_all(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "$-1\r\n");

    // 在 DB1 写入同名 key：SET k v1
    write_half
        .write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$2\r\nv1\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "+OK\r\n");

    // 再切回 DB0：SELECT 0
    write_half
        .write_all(b"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "+OK\r\n");

    // 在 DB0 读取 k，应仍为 v0
    write_half
        .write_all(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n")
        .await
        .unwrap();
    let mut bulk_header = String::new();
    let mut value = String::new();
    reader.read_line(&mut bulk_header).await.unwrap();
    reader.read_line(&mut value).await.unwrap();
    assert_eq!(bulk_header, "$2\r\n");
    assert_eq!(value, "v0\r\n");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn prometheus_metrics_exporter_basic() {
    // Enable metrics exporter on an available local port to avoid conflicts across tests.
    let metrics_listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind metrics port");
    let metrics_addr = metrics_listener.local_addr().unwrap();
    drop(metrics_listener);
    env::set_var("REDUST_METRICS_ADDR", metrics_addr.to_string());

    let (addr, shutdown, handle) = spawn_server().await;

    // Send a PING to bump total_commands_processed.
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    write_half.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "+PONG\r\n");

    // Now scrape /metrics from the exporter. 由于 metrics 监听与主服务器并行启动，
    // 这里增加一个带最大重试次数的小循环，避免偶发的连接建立时序问题导致测试 flakiness。
    let mut last_err = None;
    let mut metrics_stream = None;
    for _ in 0..20 {
        match TcpStream::connect(metrics_addr).await {
            Ok(s) => {
                metrics_stream = Some(s);
                break;
            }
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
    }

    let mut metrics_stream = metrics_stream
        .unwrap_or_else(|| panic!("failed to connect to metrics exporter: {:?}", last_err));
    metrics_stream
        .write_all(b"GET /metrics HTTP/1.0\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();

    let mut buf = String::new();
    {
        use tokio::io::AsyncReadExt;
        let mut reader = BufReader::new(metrics_stream); // reuse BufReader for HTTP response
        reader
            .read_to_string(&mut buf)
            .await
            .expect("failed to read /metrics response");
    }

    assert!(buf.contains("redust_uptime_seconds"));
    assert!(buf.contains("redust_connected_clients"));
    assert!(buf.contains("redust_total_commands_processed"));
    assert!(buf.contains("redust_keyspace_keys"));

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn list_command_wrongtype_error() {
    let (addr, shutdown, handle) = spawn_server().await;

    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // SET foo bar
    write_half
        .write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
        .await
        .unwrap();
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "+OK\r\n");

    // LTRIM foo 0 -1 should return WRONGTYPE
    write_half
        .write_all(b"*4\r\n$5\r\nLTRIM\r\n$3\r\nfoo\r\n$1\r\n0\r\n$2\r\n-1\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(
        line,
        "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
    );

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn lists_extended_commands() {
    let (addr, shutdown, handle) = spawn_server().await;

    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // RPUSH mylist a b a c -> 4
    write_half
        .write_all(
            b"*6\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\na\r\n$1\r\nc\r\n",
        )
        .await
        .unwrap();
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, ":4\r\n");

    // LLEN mylist -> 4
    write_half
        .write_all(b"*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, ":4\r\n");

    // LINDEX mylist 0 -> a
    write_half
        .write_all(b"*3\r\n$6\r\nLINDEX\r\n$6\r\nmylist\r\n$1\r\n0\r\n")
        .await
        .unwrap();
    let mut bulk_header = String::new();
    let mut value = String::new();
    reader.read_line(&mut bulk_header).await.unwrap();
    reader.read_line(&mut value).await.unwrap();
    assert_eq!(bulk_header, "$1\r\n");
    assert_eq!(value, "a\r\n");

    // LINDEX mylist -1 -> c
    write_half
        .write_all(b"*3\r\n$6\r\nLINDEX\r\n$6\r\nmylist\r\n$2\r\n-1\r\n")
        .await
        .unwrap();
    bulk_header.clear();
    value.clear();
    reader.read_line(&mut bulk_header).await.unwrap();
    reader.read_line(&mut value).await.unwrap();
    assert_eq!(bulk_header, "$1\r\n");
    assert_eq!(value, "c\r\n");

    // LREM mylist 1 a -> remove first a from head, list becomes: b a c
    write_half
        .write_all(b"*4\r\n$4\r\nLREM\r\n$6\r\nmylist\r\n$1\r\n1\r\n$1\r\na\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, ":1\r\n");

    // LLEN mylist -> 3
    write_half
        .write_all(b"*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, ":3\r\n");

    // LTRIM mylist 1 -1 -> keep from index 1 to end (a, c)
    write_half
        .write_all(b"*4\r\n$5\r\nLTRIM\r\n$6\r\nmylist\r\n$1\r\n1\r\n$2\r\n-1\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "+OK\r\n");

    // LRANGE mylist 0 -1 -> [a, c]
    write_half
        .write_all(b"*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$1\r\n0\r\n$2\r\n-1\r\n")
        .await
        .unwrap();

    let mut header = String::new();
    reader.read_line(&mut header).await.unwrap();
    assert_eq!(header, "*2\r\n");

    bulk_header.clear();
    value.clear();
    reader.read_line(&mut bulk_header).await.unwrap();
    reader.read_line(&mut value).await.unwrap();
    assert_eq!(bulk_header, "$1\r\n");
    assert_eq!(value, "a\r\n");

    bulk_header.clear();
    value.clear();
    reader.read_line(&mut bulk_header).await.unwrap();
    reader.read_line(&mut value).await.unwrap();
    assert_eq!(bulk_header, "$1\r\n");
    assert_eq!(value, "c\r\n");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn hashes_basic_behaviour() {
    let (addr, shutdown, handle) = spawn_server().await;

    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // HSET myhash field value -> 1
    write_half
        .write_all(b"*4\r\n$4\r\nHSET\r\n$6\r\nmyhash\r\n$5\r\nfield\r\n$5\r\nvalue\r\n")
        .await
        .unwrap();
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, ":1\r\n");

    // HSET myhash field value2 -> 0 (overwrite)
    write_half
        .write_all(b"*4\r\n$4\r\nHSET\r\n$6\r\nmyhash\r\n$5\r\nfield\r\n$6\r\nvalue2\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, ":0\r\n");

    // HGET myhash field -> value2
    write_half
        .write_all(b"*3\r\n$4\r\nHGET\r\n$6\r\nmyhash\r\n$5\r\nfield\r\n")
        .await
        .unwrap();
    let mut bulk_header = String::new();
    let mut bulk_value = String::new();
    reader.read_line(&mut bulk_header).await.unwrap();
    reader.read_line(&mut bulk_value).await.unwrap();
    assert_eq!(bulk_header, "$6\r\n");
    assert_eq!(bulk_value, "value2\r\n");

    // HKEYS myhash -> [field]
    write_half
        .write_all(b"*2\r\n$5\r\nHKEYS\r\n$6\r\nmyhash\r\n")
        .await
        .unwrap();
    let mut arr_header2 = String::new();
    reader.read_line(&mut arr_header2).await.unwrap();
    assert_eq!(arr_header2, "*1\r\n");
    bulk_header.clear();
    bulk_value.clear();
    reader.read_line(&mut bulk_header).await.unwrap();
    reader.read_line(&mut bulk_value).await.unwrap();
    assert_eq!(bulk_header, "$5\r\n");
    assert_eq!(bulk_value, "field\r\n");

    // HVALS myhash -> [value2]
    write_half
        .write_all(b"*2\r\n$5\r\nHVALS\r\n$6\r\nmyhash\r\n")
        .await
        .unwrap();
    let mut arr_header3 = String::new();
    reader.read_line(&mut arr_header3).await.unwrap();
    assert_eq!(arr_header3, "*1\r\n");
    bulk_header.clear();
    bulk_value.clear();
    reader.read_line(&mut bulk_header).await.unwrap();
    reader.read_line(&mut bulk_value).await.unwrap();
    assert_eq!(bulk_header, "$6\r\n");
    assert_eq!(bulk_value, "value2\r\n");

    // HMGET myhash field missing -> [value2, nil]
    write_half
        .write_all(b"*4\r\n$5\r\nHMGET\r\n$6\r\nmyhash\r\n$5\r\nfield\r\n$7\r\nmissing\r\n")
        .await
        .unwrap();
    let mut arr_header4 = String::new();
    reader.read_line(&mut arr_header4).await.unwrap();
    assert_eq!(arr_header4, "*2\r\n");

    // first bulk: value2
    bulk_header.clear();
    bulk_value.clear();
    reader.read_line(&mut bulk_header).await.unwrap();
    reader.read_line(&mut bulk_value).await.unwrap();
    assert_eq!(bulk_header, "$6\r\n");
    assert_eq!(bulk_value, "value2\r\n");

    // second bulk: nil
    bulk_header.clear();
    bulk_value.clear();
    reader.read_line(&mut bulk_header).await.unwrap();
    assert_eq!(bulk_header, "$-1\r\n");

    // HEXISTS myhash field -> 1
    write_half
        .write_all(b"*3\r\n$7\r\nHEXISTS\r\n$6\r\nmyhash\r\n$5\r\nfield\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, ":1\r\n");

    // HGETALL myhash -> [field, value2]
    write_half
        .write_all(b"*2\r\n$7\r\nHGETALL\r\n$6\r\nmyhash\r\n")
        .await
        .unwrap();
    let mut arr_header = String::new();
    reader.read_line(&mut arr_header).await.unwrap();
    assert_eq!(arr_header, "*2\r\n");

    bulk_header.clear();
    bulk_value.clear();
    reader.read_line(&mut bulk_header).await.unwrap();
    reader.read_line(&mut bulk_value).await.unwrap();
    assert_eq!(bulk_header, "$5\r\n");
    assert_eq!(bulk_value, "field\r\n");

    bulk_header.clear();
    bulk_value.clear();
    reader.read_line(&mut bulk_header).await.unwrap();
    reader.read_line(&mut bulk_value).await.unwrap();
    assert_eq!(bulk_header, "$6\r\n");
    assert_eq!(bulk_value, "value2\r\n");

    // HDEL myhash field -> 1
    write_half
        .write_all(b"*3\r\n$4\r\nHDEL\r\n$6\r\nmyhash\r\n$5\r\nfield\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, ":1\r\n");

    // HEXISTS myhash field -> 0
    write_half
        .write_all(b"*3\r\n$7\r\nHEXISTS\r\n$6\r\nmyhash\r\n$5\r\nfield\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, ":0\r\n");

    // HSET numhash f 10
    write_half
        .write_all(b"*4\r\n$4\r\nHSET\r\n$7\r\nnumhash\r\n$1\r\nf\r\n$2\r\n10\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, ":1\r\n");

    // HINCRBY numhash f 5 -> 15
    write_half
        .write_all(b"*4\r\n$7\r\nHINCRBY\r\n$7\r\nnumhash\r\n$1\r\nf\r\n$1\r\n5\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, ":15\r\n");

    // HLEN numhash -> 1
    write_half
        .write_all(b"*2\r\n$4\r\nHLEN\r\n$7\r\nnumhash\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, ":1\r\n");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn info_basic_fields() {
    let (addr, shutdown, handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    write_half.write_all(b"*1\r\n$4\r\nINFO\r\n").await.unwrap();

    let mut buf = String::new();
    // 读取若干行，直到 EOF 或已经包含我们关心的 Keyspace 行
    for _ in 0..32 {
        let mut line = String::new();
        let n = reader.read_line(&mut line).await.unwrap();
        if n == 0 {
            break;
        }
        buf.push_str(&line);
        if line.starts_with("db0:keys=") {
            break;
        }
    }

    assert!(buf.contains("# Server"));
    assert!(buf.contains("redust_version:"));
    assert!(buf.contains("tcp_port:"));
    assert!(buf.contains("uptime_in_seconds:"));
    assert!(buf.contains("# Clients"));
    assert!(buf.contains("connected_clients:"));
    assert!(buf.contains("# Stats"));
    assert!(buf.contains("total_commands_processed:"));
    assert!(buf.contains("# Keyspace"));
    assert!(buf.contains("db0:keys="));

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn sets_difference_behaviour() {
    let (addr, shutdown, handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // SADD set1 a b c
    write_half
        .write_all(b"*5\r\n$4\r\nSADD\r\n$4\r\nset1\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n")
        .await
        .unwrap();
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, ":3\r\n");

    // SADD set2 b d
    write_half
        .write_all(b"*4\r\n$4\r\nSADD\r\n$4\r\nset2\r\n$1\r\nb\r\n$1\r\nd\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, ":2\r\n");

    // SDIFF set1 set2 -> [a, c]
    write_half
        .write_all(b"*3\r\n$5\r\nSDIFF\r\n$4\r\nset1\r\n$4\r\nset2\r\n")
        .await
        .unwrap();

    let mut header = String::new();
    reader.read_line(&mut header).await.unwrap();
    assert_eq!(header, "*2\r\n");

    let mut bulk_header = String::new();
    let mut value = String::new();
    let mut members = Vec::new();
    for _ in 0..2 {
        bulk_header.clear();
        value.clear();
        reader.read_line(&mut bulk_header).await.unwrap();
        reader.read_line(&mut value).await.unwrap();
        assert_eq!(bulk_header, "$1\r\n");
        members.push(value.trim_end_matches("\r\n").to_string());
    }
    members.sort();
    assert_eq!(members, vec!["a".to_string(), "c".to_string()]);

    // SDIFF set2 set1 -> [d]
    write_half
        .write_all(b"*3\r\n$5\r\nSDIFF\r\n$4\r\nset2\r\n$4\r\nset1\r\n")
        .await
        .unwrap();

    header.clear();
    reader.read_line(&mut header).await.unwrap();
    assert_eq!(header, "*1\r\n");

    bulk_header.clear();
    value.clear();
    reader.read_line(&mut bulk_header).await.unwrap();
    reader.read_line(&mut value).await.unwrap();
    assert_eq!(bulk_header, "$1\r\n");
    assert_eq!(value, "d\r\n");

    // SDIFF set1 missing -> [a, b, c]
    write_half
        .write_all(b"*3\r\n$5\r\nSDIFF\r\n$4\r\nset1\r\n$7\r\nmissing\r\n")
        .await
        .unwrap();

    header.clear();
    reader.read_line(&mut header).await.unwrap();
    assert_eq!(header, "*3\r\n");

    members.clear();
    for _ in 0..3 {
        bulk_header.clear();
        value.clear();
        reader.read_line(&mut bulk_header).await.unwrap();
        reader.read_line(&mut value).await.unwrap();
        assert_eq!(bulk_header, "$1\r\n");
        members.push(value.trim_end_matches("\r\n").to_string());
    }
    members.sort();
    assert_eq!(members, vec!["a", "b", "c"]);

    // SDIFF missing set1 -> empty array
    write_half
        .write_all(b"*3\r\n$5\r\nSDIFF\r\n$7\r\nmissing\r\n$4\r\nset1\r\n")
        .await
        .unwrap();

    header.clear();
    reader.read_line(&mut header).await.unwrap();
    assert_eq!(header, "*0\r\n");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn lists_lrange_boundaries() {
    let (addr, shutdown, handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // RPUSH mylist a b c -> 3
    write_half
        .write_all(b"*5\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n")
        .await
        .unwrap();
    let mut resp = String::new();
    reader.read_line(&mut resp).await.unwrap();
    assert_eq!(resp, ":3\r\n");

    // LRANGE with start > end -> empty array
    write_half
        .write_all(b"*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$1\r\n2\r\n$1\r\n1\r\n")
        .await
        .unwrap();
    let mut header = String::new();
    reader.read_line(&mut header).await.unwrap();
    assert_eq!(header, "*0\r\n");

    // LRANGE far out of range positive indexes -> empty array
    write_half
        .write_all(b"*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$2\r\n10\r\n$2\r\n20\r\n")
        .await
        .unwrap();
    header.clear();
    reader.read_line(&mut header).await.unwrap();
    assert_eq!(header, "*0\r\n");

    // LRANGE with very wide range should still return full list
    write_half
        .write_all(b"*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$4\r\n-100\r\n$3\r\n100\r\n")
        .await
        .unwrap();
    header.clear();
    reader.read_line(&mut header).await.unwrap();
    assert_eq!(header, "*3\r\n");

    let mut bulk_header = String::new();
    let mut value = String::new();

    // a
    reader.read_line(&mut bulk_header).await.unwrap();
    assert_eq!(bulk_header, "$1\r\n");
    reader.read_line(&mut value).await.unwrap();
    assert_eq!(value, "a\r\n");
    bulk_header.clear();
    value.clear();

    // b
    reader.read_line(&mut bulk_header).await.unwrap();
    assert_eq!(bulk_header, "$1\r\n");
    reader.read_line(&mut value).await.unwrap();
    assert_eq!(value, "b\r\n");
    bulk_header.clear();
    value.clear();

    // c
    reader.read_line(&mut bulk_header).await.unwrap();
    assert_eq!(bulk_header, "$1\r\n");
    reader.read_line(&mut value).await.unwrap();
    assert_eq!(value, "c\r\n");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn lists_multi_client_visibility() {
    let (addr, shutdown, handle) = spawn_server().await;

    // client 1: push elements into list
    let stream1 = TcpStream::connect(addr).await.unwrap();
    let (_r1, mut w1) = stream1.into_split();
    w1.write_all(b"*5\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n")
        .await
        .unwrap();

    // client 2: read list contents via LRANGE
    let stream2 = TcpStream::connect(addr).await.unwrap();
    let (r2, mut w2) = stream2.into_split();
    let mut reader2 = BufReader::new(r2);

    w2.write_all(b"*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$1\r\n0\r\n$2\r\n-1\r\n")
        .await
        .unwrap();

    let mut header = String::new();
    reader2.read_line(&mut header).await.unwrap();
    assert_eq!(header, "*3\r\n");

    let mut bulk_header = String::new();
    let mut value = String::new();
    let mut items = Vec::new();
    for _ in 0..3 {
        bulk_header.clear();
        value.clear();
        reader2.read_line(&mut bulk_header).await.unwrap();
        reader2.read_line(&mut value).await.unwrap();
        assert_eq!(bulk_header, "$1\r\n");
        items.push(value.trim_end_matches("\r\n").to_string());
    }

    assert_eq!(items, vec!["a", "b", "c"]);

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn sets_intersection_behaviour() {
    let (addr, shutdown, handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // SINTER on all-missing keys -> empty array
    write_half
        .write_all(b"*3\r\n$6\r\nSINTER\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
        .await
        .unwrap();
    let mut header = String::new();
    reader.read_line(&mut header).await.unwrap();
    assert_eq!(header, "*0\r\n");

    // SADD set1 a b
    write_half
        .write_all(b"*4\r\n$4\r\nSADD\r\n$4\r\nset1\r\n$1\r\na\r\n$1\r\nb\r\n")
        .await
        .unwrap();
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, ":2\r\n");

    // SADD set2 b c
    write_half
        .write_all(b"*4\r\n$4\r\nSADD\r\n$4\r\nset2\r\n$1\r\nb\r\n$1\r\nc\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, ":2\r\n");

    // SADD set3 b d
    write_half
        .write_all(b"*4\r\n$4\r\nSADD\r\n$4\r\nset3\r\n$1\r\nb\r\n$1\r\nd\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, ":2\r\n");

    // SINTER set1 set2 set3 -> [b]
    write_half
        .write_all(b"*4\r\n$6\r\nSINTER\r\n$4\r\nset1\r\n$4\r\nset2\r\n$4\r\nset3\r\n")
        .await
        .unwrap();

    header.clear();
    reader.read_line(&mut header).await.unwrap();
    assert_eq!(header, "*1\r\n");

    let mut bulk_header = String::new();
    let mut value = String::new();
    reader.read_line(&mut bulk_header).await.unwrap();
    reader.read_line(&mut value).await.unwrap();
    assert_eq!(bulk_header, "$1\r\n");
    assert_eq!(value, "b\r\n");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn sets_union_behaviour() {
    let (addr, shutdown, handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // SUNION on all-missing keys -> empty array
    write_half
        .write_all(b"*3\r\n$6\r\nSUNION\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
        .await
        .unwrap();
    let mut header = String::new();
    reader.read_line(&mut header).await.unwrap();
    assert_eq!(header, "*0\r\n");

    // SADD set1 a b
    write_half
        .write_all(b"*4\r\n$4\r\nSADD\r\n$4\r\nset1\r\n$1\r\na\r\n$1\r\nb\r\n")
        .await
        .unwrap();
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, ":2\r\n");

    // SADD set2 b c
    write_half
        .write_all(b"*4\r\n$4\r\nSADD\r\n$4\r\nset2\r\n$1\r\nb\r\n$1\r\nc\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, ":2\r\n");

    // SUNION set1 set2 missing -> [a, b, c] （顺序由存储排序保证）
    write_half
        .write_all(b"*4\r\n$6\r\nSUNION\r\n$4\r\nset1\r\n$4\r\nset2\r\n$7\r\nmissing\r\n")
        .await
        .unwrap();

    header.clear();
    reader.read_line(&mut header).await.unwrap();
    assert_eq!(header, "*3\r\n");

    let mut bulk_header = String::new();
    let mut value = String::new();
    let mut members = Vec::new();
    for _ in 0..3 {
        bulk_header.clear();
        value.clear();
        reader.read_line(&mut bulk_header).await.unwrap();
        reader.read_line(&mut value).await.unwrap();
        assert_eq!(bulk_header, "$1\r\n");
        members.push(value.trim_end_matches("\r\n").to_string());
    }

    members.sort();
    assert_eq!(
        members,
        vec!["a".to_string(), "b".to_string(), "c".to_string()]
    );

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn sets_store_and_random_commands() {
    let (addr, shutdown, handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // Prepare source sets
    send_array(&mut write_half, &["SADD", "set1", "a", "b", "c"]).await;
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, ":3\r\n");

    send_array(&mut write_half, &["SADD", "set2", "b", "c", "d"]).await;
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, ":3\r\n");

    // SUNIONSTORE dest set1 set2 -> 4 members
    send_array(&mut write_half, &["SUNIONSTORE", "dest", "set1", "set2"]).await;
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, ":4\r\n");

    // SMEMBERS dest -> a, b, c, d (order not guaranteed)
    send_array(&mut write_half, &["SMEMBERS", "dest"]).await;
    let mut arr_header = String::new();
    reader.read_line(&mut arr_header).await.unwrap();
    assert_eq!(arr_header, "*4\r\n");
    eprintln!("stage: smembers dest header");
    let mut members = Vec::new();
    for _ in 0..4 {
        let mut bulk_header = String::new();
        let mut value = String::new();
        reader.read_line(&mut bulk_header).await.unwrap();
        reader.read_line(&mut value).await.unwrap();
        assert!(bulk_header.starts_with("$"));
        members.push(value.trim_end_matches("\r\n").to_string());
    }
    members.sort();
    assert_eq!(
        members,
        vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string()
        ]
    );
    eprintln!("stage: union members read");

    // Set a TTL on dest, then SINTERSTORE should drop the expiry
    send_array(&mut write_half, &["PEXPIRE", "dest", "100"]).await;
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, ":1\r\n");

    send_array(&mut write_half, &["SINTERSTORE", "dest", "set1", "set2"]).await;
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, ":2\r\n");

    // PTTL dest should be -1 (no expiry) after store operation
    send_array(&mut write_half, &["PTTL", "dest"]).await;
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, ":-1\r\n");

    // Members now should be intersection: b, c
    send_array(&mut write_half, &["SMEMBERS", "dest"]).await;
    arr_header.clear();
    reader.read_line(&mut arr_header).await.unwrap();
    assert_eq!(arr_header, "*2\r\n");
    members.clear();
    for _ in 0..2 {
        let mut bulk_header = String::new();
        let mut value = String::new();
        reader.read_line(&mut bulk_header).await.unwrap();
        reader.read_line(&mut value).await.unwrap();
        members.push(value.trim_end_matches("\r\n").to_string());
    }
    members.sort();
    assert_eq!(members, vec!["b".to_string(), "c".to_string()]);

    // SDIFFSTORE dest set1 set2 -> a
    send_array(&mut write_half, &["SDIFFSTORE", "dest", "set1", "set2"]).await;
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, ":1\r\n");

    send_array(&mut write_half, &["SMEMBERS", "dest"]).await;
    arr_header.clear();
    reader.read_line(&mut arr_header).await.unwrap();
    assert_eq!(arr_header, "*1\r\n");
    let mut bulk_header = String::new();
    let mut value = String::new();
    reader.read_line(&mut bulk_header).await.unwrap();
    reader.read_line(&mut value).await.unwrap();
    assert_eq!(value, "a\r\n");

    // Wrongtype in source should error and keep dest unchanged
    send_array(&mut write_half, &["SET", "strkey", "x"]).await;
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "+OK\r\n");

    send_array(&mut write_half, &["SUNIONSTORE", "dest", "set1", "strkey"]).await;
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(
        line,
        "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    );

    // dest should still hold the previous SDIFFSTORE result (a)
    write_half
        .write_all(b"*2\r\n$8\r\nSMEMBERS\r\n$4\r\ndest\r\n")
        .await
        .unwrap();
    arr_header.clear();
    reader.read_line(&mut arr_header).await.unwrap();
    assert_eq!(arr_header, "*1\r\n");
    bulk_header.clear();
    value.clear();
    reader.read_line(&mut bulk_header).await.unwrap();
    reader.read_line(&mut value).await.unwrap();
    assert_eq!(value, "a\r\n");

    // SPOP with count should remove that many distinct members
    send_array(&mut write_half, &["SADD", "popset", "x", "y", "z"]).await;
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, ":3\r\n");

    send_array(&mut write_half, &["SPOP", "popset", "2"]).await;
    arr_header.clear();
    reader.read_line(&mut arr_header).await.unwrap();
    assert_eq!(arr_header, "*2\r\n");
    // drain two bulk entries
    for _ in 0..2 {
        bulk_header.clear();
        value.clear();
        reader.read_line(&mut bulk_header).await.unwrap();
        reader.read_line(&mut value).await.unwrap();
        assert!(bulk_header.starts_with("$"));
    }

    send_array(&mut write_half, &["SCARD", "popset"]).await;
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, ":1\r\n");

    // Final SPOP should return the last element and empty the set
    send_array(&mut write_half, &["SPOP", "popset"]).await;
    bulk_header.clear();
    value.clear();
    reader.read_line(&mut bulk_header).await.unwrap();
    reader.read_line(&mut value).await.unwrap();
    assert!(bulk_header.starts_with("$"));

    send_array(&mut write_half, &["SCARD", "popset"]).await;
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, ":0\r\n");

    // SRANDMEMBER with negative count should return duplicates without mutation
    send_array(&mut write_half, &["SADD", "randset", "a", "b", "c"]).await;
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, ":3\r\n");

    send_array(&mut write_half, &["SRANDMEMBER", "randset", "-5"]).await;
    arr_header.clear();
    reader.read_line(&mut arr_header).await.unwrap();
    assert_eq!(arr_header, "*5\r\n");
    for _ in 0..5 {
        bulk_header.clear();
        value.clear();
        reader.read_line(&mut bulk_header).await.unwrap();
        reader.read_line(&mut value).await.unwrap();
        assert!(bulk_header.starts_with("$"));
    }

    send_array(&mut write_half, &["SCARD", "randset"]).await;
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, ":3\r\n");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn lists_pop_behaviour() {
    let (addr, shutdown, handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // LPOP on missing key -> nil
    write_half
        .write_all(b"*2\r\n$4\r\nLPOP\r\n$6\r\nmylist\r\n")
        .await
        .unwrap();
    let mut header = String::new();
    reader.read_line(&mut header).await.unwrap();
    assert_eq!(header, "$-1\r\n");

    // RPUSH mylist a b c -> 3
    write_half
        .write_all(b"*5\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n")
        .await
        .unwrap();
    let mut rpush_resp = String::new();
    reader.read_line(&mut rpush_resp).await.unwrap();
    assert_eq!(rpush_resp, ":3\r\n");

    // LPOP -> a
    write_half
        .write_all(b"*2\r\n$4\r\nLPOP\r\n$6\r\nmylist\r\n")
        .await
        .unwrap();
    let mut bulk_header = String::new();
    let mut value = String::new();
    reader.read_line(&mut bulk_header).await.unwrap();
    reader.read_line(&mut value).await.unwrap();
    assert_eq!(bulk_header, "$1\r\n");
    assert_eq!(value, "a\r\n");

    // RPOP -> c （现在列表只剩 b, c）
    write_half
        .write_all(b"*2\r\n$4\r\nRPOP\r\n$6\r\nmylist\r\n")
        .await
        .unwrap();
    bulk_header.clear();
    value.clear();
    reader.read_line(&mut bulk_header).await.unwrap();
    reader.read_line(&mut value).await.unwrap();
    assert_eq!(bulk_header, "$1\r\n");
    assert_eq!(value, "c\r\n");

    // RPOP -> b （最后一个元素）
    write_half
        .write_all(b"*2\r\n$4\r\nRPOP\r\n$6\r\nmylist\r\n")
        .await
        .unwrap();
    bulk_header.clear();
    value.clear();
    reader.read_line(&mut bulk_header).await.unwrap();
    reader.read_line(&mut value).await.unwrap();
    assert_eq!(bulk_header, "$1\r\n");
    assert_eq!(value, "b\r\n");

    // RPOP on empty list -> nil
    write_half
        .write_all(b"*2\r\n$4\r\nRPOP\r\n$6\r\nmylist\r\n")
        .await
        .unwrap();
    header.clear();
    reader.read_line(&mut header).await.unwrap();
    assert_eq!(header, "$-1\r\n");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn sets_basic_behaviour() {
    let (addr, shutdown, handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // SADD myset a b a -> 2
    write_half
        .write_all(b"*5\r\n$4\r\nSADD\r\n$5\r\nmyset\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\na\r\n")
        .await
        .unwrap();
    let mut sadd_resp = String::new();
    reader.read_line(&mut sadd_resp).await.unwrap();
    assert_eq!(sadd_resp, ":2\r\n");

    // SCARD myset -> 2
    write_half
        .write_all(b"*2\r\n$5\r\nSCARD\r\n$5\r\nmyset\r\n")
        .await
        .unwrap();
    let mut scard_resp = String::new();
    reader.read_line(&mut scard_resp).await.unwrap();
    assert_eq!(scard_resp, ":2\r\n");

    // SISMEMBER myset a -> 1
    write_half
        .write_all(b"*3\r\n$9\r\nSISMEMBER\r\n$5\r\nmyset\r\n$1\r\na\r\n")
        .await
        .unwrap();
    let mut sismem_a = String::new();
    reader.read_line(&mut sismem_a).await.unwrap();
    assert_eq!(sismem_a, ":1\r\n");

    // SISMEMBER myset c -> 0
    write_half
        .write_all(b"*3\r\n$9\r\nSISMEMBER\r\n$5\r\nmyset\r\n$1\r\nc\r\n")
        .await
        .unwrap();
    let mut sismem_c = String::new();
    reader.read_line(&mut sismem_c).await.unwrap();
    assert_eq!(sismem_c, ":0\r\n");

    // SMEMBERS myset -> [a, b] (order not guaranteed, but we sorted in storage)
    write_half
        .write_all(b"*2\r\n$8\r\nSMEMBERS\r\n$5\r\nmyset\r\n")
        .await
        .unwrap();
    let mut arr_header = String::new();
    reader.read_line(&mut arr_header).await.unwrap();
    assert_eq!(arr_header, "*2\r\n");

    let mut bulk_header = String::new();
    let mut value = String::new();
    reader.read_line(&mut bulk_header).await.unwrap();
    reader.read_line(&mut value).await.unwrap();
    assert_eq!(bulk_header, "$1\r\n");
    let first = value.trim_end_matches("\r\n").to_string();

    bulk_header.clear();
    value.clear();
    reader.read_line(&mut bulk_header).await.unwrap();
    reader.read_line(&mut value).await.unwrap();
    assert_eq!(bulk_header, "$1\r\n");
    let second = value.trim_end_matches("\r\n").to_string();

    // Members should be exactly a and b in some order
    let mut members = vec![first, second];
    members.sort();
    assert_eq!(members, vec!["a".to_string(), "b".to_string()]);

    // SREM myset a -> 1
    write_half
        .write_all(b"*3\r\n$4\r\nSREM\r\n$5\r\nmyset\r\n$1\r\na\r\n")
        .await
        .unwrap();
    let mut srem_resp = String::new();
    reader.read_line(&mut srem_resp).await.unwrap();
    assert_eq!(srem_resp, ":1\r\n");

    // SCARD myset -> 1
    write_half
        .write_all(b"*2\r\n$5\r\nSCARD\r\n$5\r\nmyset\r\n")
        .await
        .unwrap();
    scard_resp.clear();
    reader.read_line(&mut scard_resp).await.unwrap();
    assert_eq!(scard_resp, ":1\r\n");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn lists_basic_behaviour() {
    let (addr, shutdown, handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // LRANGE on missing key -> empty array
    write_half
        .write_all(b"*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$1\r\n0\r\n$2\r\n-1\r\n")
        .await
        .unwrap();
    let mut header = String::new();
    reader.read_line(&mut header).await.unwrap();
    assert_eq!(header, "*0\r\n");

    // RPUSH mylist a b c -> 3
    write_half
        .write_all(b"*5\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n")
        .await
        .unwrap();
    let mut rpush_resp = String::new();
    reader.read_line(&mut rpush_resp).await.unwrap();
    assert_eq!(rpush_resp, ":3\r\n");

    // LPUSH mylist x -> 4 (list: x a b c)
    write_half
        .write_all(b"*3\r\n$5\r\nLPUSH\r\n$6\r\nmylist\r\n$1\r\nx\r\n")
        .await
        .unwrap();
    let mut lpush_resp = String::new();
    reader.read_line(&mut lpush_resp).await.unwrap();
    assert_eq!(lpush_resp, ":4\r\n");

    // LRANGE mylist 0 -1 -> [x, a, b, c]
    write_half
        .write_all(b"*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$1\r\n0\r\n$2\r\n-1\r\n")
        .await
        .unwrap();
    let mut arr_header = String::new();
    reader.read_line(&mut arr_header).await.unwrap();
    assert_eq!(arr_header, "*4\r\n");

    let mut bulk_header = String::new();
    let mut value = String::new();

    // x
    reader.read_line(&mut bulk_header).await.unwrap();
    assert_eq!(bulk_header, "$1\r\n");
    reader.read_line(&mut value).await.unwrap();
    assert_eq!(value, "x\r\n");
    bulk_header.clear();
    value.clear();

    // a
    reader.read_line(&mut bulk_header).await.unwrap();
    assert_eq!(bulk_header, "$1\r\n");
    reader.read_line(&mut value).await.unwrap();
    assert_eq!(value, "a\r\n");
    bulk_header.clear();
    value.clear();

    // b
    reader.read_line(&mut bulk_header).await.unwrap();
    assert_eq!(bulk_header, "$1\r\n");
    reader.read_line(&mut value).await.unwrap();
    assert_eq!(value, "b\r\n");
    bulk_header.clear();
    value.clear();

    // c
    reader.read_line(&mut bulk_header).await.unwrap();
    assert_eq!(bulk_header, "$1\r\n");
    reader.read_line(&mut value).await.unwrap();
    assert_eq!(value, "c\r\n");

    // LRANGE mylist 1 2 -> [a, b]
    write_half
        .write_all(b"*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$1\r\n1\r\n$1\r\n2\r\n")
        .await
        .unwrap();
    let mut sub_header = String::new();
    reader.read_line(&mut sub_header).await.unwrap();
    assert_eq!(sub_header, "*2\r\n");

    bulk_header.clear();
    value.clear();
    reader.read_line(&mut bulk_header).await.unwrap();
    assert_eq!(bulk_header, "$1\r\n");
    reader.read_line(&mut value).await.unwrap();
    assert_eq!(value, "a\r\n");

    bulk_header.clear();
    value.clear();
    reader.read_line(&mut bulk_header).await.unwrap();
    assert_eq!(bulk_header, "$1\r\n");
    reader.read_line(&mut value).await.unwrap();
    assert_eq!(value, "b\r\n");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn multiple_clients_share_storage() {
    let (addr, shutdown, handle) = spawn_server().await;

    // client 1: SET foo bar
    let stream1 = TcpStream::connect(addr).await.unwrap();
    let (_r1, mut w1) = stream1.into_split();
    w1.write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
        .await
        .unwrap();

    // client 2: GET foo, should see bar
    let stream2 = TcpStream::connect(addr).await.unwrap();
    let (r2, mut w2) = stream2.into_split();
    let mut reader2 = BufReader::new(r2);
    w2.write_all(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")
        .await
        .unwrap();

    let mut header = String::new();
    let mut value = String::new();
    reader2.read_line(&mut header).await.unwrap();
    reader2.read_line(&mut value).await.unwrap();
    assert_eq!(header, "$3\r\n");
    assert_eq!(value, "bar\r\n");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn continues_after_unknown_command() {
    let (addr, shutdown, handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    write_half
        .write_all(b"*2\r\n$7\r\nCOMMAND\r\n$4\r\nDOCS\r\n")
        .await
        .unwrap();

    let mut error_line = String::new();
    reader.read_line(&mut error_line).await.unwrap();
    assert_eq!(error_line, "-ERR unknown command 'COMMAND DOCS'\r\n");

    write_half.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
    let mut pong = String::new();
    reader.read_line(&mut pong).await.unwrap();
    assert_eq!(pong, "+PONG\r\n");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn handles_quit_and_connection_close() {
    let (addr, shutdown, handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    write_half.write_all(b"*1\r\n$4\r\nQUIT\r\n").await.unwrap();
    let mut ok = String::new();
    reader.read_line(&mut ok).await.unwrap();
    assert_eq!(ok, "+OK\r\n");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn performance_ping_round_trips() {
    let (addr, shutdown, handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    let iterations = 200;
    let start = Instant::now();
    for _ in 0..iterations {
        write_half.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        assert_eq!(line, "+PONG\r\n");
        line.clear();
    }
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(2),
        "Ping loop took too long: {:?}",
        elapsed
    );

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

// ============ Hash 命令测试 ============

async fn read_line_helper(reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>) -> String {
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();
    line
}

#[tokio::test]
async fn test_hsetnx() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // HSETNX 新字段应返回 1
    send_array(&mut write_half, &["HSETNX", "myhash", "field1", "value1"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":1\r\n");

    // HSETNX 已存在字段应返回 0
    send_array(&mut write_half, &["HSETNX", "myhash", "field1", "value2"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":0\r\n");

    // 验证值未被覆盖
    send_array(&mut write_half, &["HGET", "myhash", "field1"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "$6\r\n");
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "value1\r\n");

    // HSETNX 对不存在的 key 应创建 Hash
    send_array(&mut write_half, &["HSETNX", "newhash", "f", "v"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":1\r\n");

    // 验证类型
    send_array(&mut write_half, &["TYPE", "newhash"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "+hash\r\n");

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_hstrlen() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // 设置字段
    send_array(&mut write_half, &["HSET", "myhash", "field1", "hello"]).await;
    let _ = read_line_helper(&mut reader).await;

    // HSTRLEN 返回字段值长度
    send_array(&mut write_half, &["HSTRLEN", "myhash", "field1"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":5\r\n");

    // HSTRLEN 不存在的字段返回 0
    send_array(&mut write_half, &["HSTRLEN", "myhash", "nofield"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":0\r\n");

    // HSTRLEN 不存在的 key 返回 0
    send_array(&mut write_half, &["HSTRLEN", "nokey", "field"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":0\r\n");

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_hmset() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // HMSET 批量设置
    send_array(&mut write_half, &["HMSET", "myhash", "f1", "v1", "f2", "v2", "f3", "v3"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "+OK\r\n");

    // 验证所有字段
    send_array(&mut write_half, &["HGET", "myhash", "f1"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "$2\r\n");
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "v1\r\n");

    send_array(&mut write_half, &["HGET", "myhash", "f2"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "$2\r\n");
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "v2\r\n");

    send_array(&mut write_half, &["HGET", "myhash", "f3"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "$2\r\n");
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "v3\r\n");

    // HMSET 覆盖已有字段
    send_array(&mut write_half, &["HMSET", "myhash", "f1", "new_v1"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "+OK\r\n");

    send_array(&mut write_half, &["HGET", "myhash", "f1"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "$6\r\n");
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "new_v1\r\n");

    // HLEN 验证字段数量
    send_array(&mut write_half, &["HLEN", "myhash"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":3\r\n");

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_hash_wrongtype() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // 创建 String 类型
    send_array(&mut write_half, &["SET", "strkey", "value"]).await;
    let _ = read_line_helper(&mut reader).await;

    // HSETNX 对 String 类型应返回 WRONGTYPE
    send_array(&mut write_half, &["HSETNX", "strkey", "field", "val"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert!(resp.starts_with("-WRONGTYPE"));

    // HSTRLEN 对 String 类型应返回 WRONGTYPE
    send_array(&mut write_half, &["HSTRLEN", "strkey", "field"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert!(resp.starts_with("-WRONGTYPE"));

    // HMSET 对 String 类型应返回 WRONGTYPE
    send_array(&mut write_half, &["HMSET", "strkey", "f", "v"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert!(resp.starts_with("-WRONGTYPE"));

    let _ = shutdown.send(());
}

// ============ List 命令测试 ============

#[tokio::test]
async fn test_lset() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // 创建列表
    send_array(&mut write_half, &["RPUSH", "mylist", "a", "b", "c"]).await;
    let _ = read_line_helper(&mut reader).await;

    // LSET 正常设置
    send_array(&mut write_half, &["LSET", "mylist", "1", "B"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "+OK\r\n");

    // 验证
    send_array(&mut write_half, &["LINDEX", "mylist", "1"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "$1\r\n");
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "B\r\n");

    // LSET 负索引
    send_array(&mut write_half, &["LSET", "mylist", "-1", "C"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "+OK\r\n");

    // LSET 索引越界
    send_array(&mut write_half, &["LSET", "mylist", "10", "x"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert!(resp.starts_with("-ERR index out of range"));

    // LSET 不存在的 key
    send_array(&mut write_half, &["LSET", "nokey", "0", "x"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert!(resp.starts_with("-ERR no such key"));

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_linsert() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // 创建列表
    send_array(&mut write_half, &["RPUSH", "mylist", "a", "c"]).await;
    let _ = read_line_helper(&mut reader).await;

    // LINSERT BEFORE
    send_array(&mut write_half, &["LINSERT", "mylist", "BEFORE", "c", "b"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":3\r\n");

    // LINSERT AFTER
    send_array(&mut write_half, &["LINSERT", "mylist", "AFTER", "c", "d"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":4\r\n");

    // 验证顺序 a, b, c, d
    send_array(&mut write_half, &["LRANGE", "mylist", "0", "-1"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "*4\r\n");
    // 读取 4 个元素（每个元素 2 行：长度 + 值）
    for _ in 0..8 {
        let _ = read_line_helper(&mut reader).await;
    }

    // LINSERT pivot 不存在返回 -1
    send_array(&mut write_half, &["LINSERT", "mylist", "BEFORE", "x", "y"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":-1\r\n");

    // LINSERT key 不存在返回 0
    send_array(&mut write_half, &["LINSERT", "nokey", "BEFORE", "a", "b"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":0\r\n");

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_rpoplpush() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // 创建源列表
    send_array(&mut write_half, &["RPUSH", "src", "a", "b", "c"]).await;
    let _ = read_line_helper(&mut reader).await;

    // RPOPLPUSH
    send_array(&mut write_half, &["RPOPLPUSH", "src", "dst"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "$1\r\n");
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "c\r\n");

    // 验证 src 剩余 a, b
    send_array(&mut write_half, &["LLEN", "src"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":2\r\n");

    // 验证 dst 有 c
    send_array(&mut write_half, &["LRANGE", "dst", "0", "-1"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "*1\r\n");
    // 读取 1 个元素
    let _ = read_line_helper(&mut reader).await; // $1
    let _ = read_line_helper(&mut reader).await; // c

    // RPOPLPUSH 空列表返回 null
    send_array(&mut write_half, &["RPOPLPUSH", "empty", "dst"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "$-1\r\n");

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_lpos() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // 创建列表
    send_array(&mut write_half, &["RPUSH", "mylist", "a", "b", "c", "b", "d", "b"]).await;
    let _ = read_line_helper(&mut reader).await;

    // LPOS 基本查找
    send_array(&mut write_half, &["LPOS", "mylist", "b"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":1\r\n");

    // LPOS 元素不存在返回 null
    send_array(&mut write_half, &["LPOS", "mylist", "x"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "$-1\r\n");

    // LPOS COUNT 返回多个位置
    send_array(&mut write_half, &["LPOS", "mylist", "b", "COUNT", "2"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "*2\r\n");
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":1\r\n");
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":3\r\n");

    // LPOS key 不存在返回 null
    send_array(&mut write_half, &["LPOS", "nokey", "a"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "$-1\r\n");

    let _ = shutdown.send(());
}

// ============ Sorted Set 命令测试 ============

#[tokio::test]
async fn test_zcount() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // 创建 sorted set
    send_array(&mut write_half, &["ZADD", "myzset", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e"]).await;
    let _ = read_line_helper(&mut reader).await;

    // ZCOUNT 全范围
    send_array(&mut write_half, &["ZCOUNT", "myzset", "-inf", "+inf"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":5\r\n");

    // ZCOUNT 指定范围
    send_array(&mut write_half, &["ZCOUNT", "myzset", "2", "4"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":3\r\n");

    // ZCOUNT exclusive
    send_array(&mut write_half, &["ZCOUNT", "myzset", "(2", "4"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":2\r\n");

    // ZCOUNT 不存在的 key
    send_array(&mut write_half, &["ZCOUNT", "nokey", "0", "10"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":0\r\n");

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_zrank_zrevrank() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // 创建 sorted set
    send_array(&mut write_half, &["ZADD", "myzset", "1", "a", "2", "b", "3", "c"]).await;
    let _ = read_line_helper(&mut reader).await;

    // ZRANK
    send_array(&mut write_half, &["ZRANK", "myzset", "a"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":0\r\n");

    send_array(&mut write_half, &["ZRANK", "myzset", "c"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":2\r\n");

    // ZRANK 不存在的成员
    send_array(&mut write_half, &["ZRANK", "myzset", "x"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "$-1\r\n");

    // ZREVRANK
    send_array(&mut write_half, &["ZREVRANK", "myzset", "a"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":2\r\n");

    send_array(&mut write_half, &["ZREVRANK", "myzset", "c"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":0\r\n");

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_zpopmin_zpopmax() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // 创建 sorted set
    send_array(&mut write_half, &["ZADD", "myzset", "1", "a", "2", "b", "3", "c"]).await;
    let _ = read_line_helper(&mut reader).await;

    // ZPOPMIN
    send_array(&mut write_half, &["ZPOPMIN", "myzset"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "*2\r\n");
    let _ = read_line_helper(&mut reader).await; // $1
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "a\r\n");
    let _ = read_line_helper(&mut reader).await; // $1
    let _ = read_line_helper(&mut reader).await; // 1

    // ZPOPMAX
    send_array(&mut write_half, &["ZPOPMAX", "myzset"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "*2\r\n");
    let _ = read_line_helper(&mut reader).await; // $1
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "c\r\n");

    // 验证剩余只有 b
    send_array(&mut write_half, &["ZCARD", "myzset"]).await;
    // 先读取 ZPOPMAX 剩余的响应
    let _ = read_line_helper(&mut reader).await; // $1
    let _ = read_line_helper(&mut reader).await; // 3
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":1\r\n");

    // ZPOPMIN 空 set
    send_array(&mut write_half, &["ZPOPMIN", "empty"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "*0\r\n");

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_zinter_zunion_zdiff() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // 创建两个 sorted set
    send_array(&mut write_half, &["ZADD", "zset1", "1", "a", "2", "b", "3", "c"]).await;
    let _ = read_line_helper(&mut reader).await;
    send_array(&mut write_half, &["ZADD", "zset2", "2", "b", "3", "c", "4", "d"]).await;
    let _ = read_line_helper(&mut reader).await;

    // ZINTER - 交集
    send_array(&mut write_half, &["ZINTER", "2", "zset1", "zset2"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "*2\r\n"); // b, c
    // 读取两个成员
    for _ in 0..4 {
        let _ = read_line_helper(&mut reader).await;
    }

    // ZUNION - 并集
    send_array(&mut write_half, &["ZUNION", "2", "zset1", "zset2"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "*4\r\n"); // a, b, c, d
    for _ in 0..8 {
        let _ = read_line_helper(&mut reader).await;
    }

    // ZDIFF - 差集 (zset1 - zset2)
    send_array(&mut write_half, &["ZDIFF", "2", "zset1", "zset2"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "*1\r\n"); // 只有 a
    let _ = read_line_helper(&mut reader).await; // $1
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "a\r\n");

    // ZINTERSTORE
    send_array(&mut write_half, &["ZINTERSTORE", "out", "2", "zset1", "zset2"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":2\r\n");

    // ZUNIONSTORE
    send_array(&mut write_half, &["ZUNIONSTORE", "out2", "2", "zset1", "zset2"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":4\r\n");

    // ZDIFFSTORE
    send_array(&mut write_half, &["ZDIFFSTORE", "out3", "2", "zset1", "zset2"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":1\r\n");

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_zlexcount() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // 创建 sorted set，所有成员分数相同
    send_array(&mut write_half, &["ZADD", "myzset", "0", "a", "0", "b", "0", "c", "0", "d", "0", "e", "0", "f"]).await;
    let _ = read_line_helper(&mut reader).await;

    // ZLEXCOUNT 全范围
    send_array(&mut write_half, &["ZLEXCOUNT", "myzset", "-", "+"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":6\r\n");

    // ZLEXCOUNT [b, [e] - inclusive
    send_array(&mut write_half, &["ZLEXCOUNT", "myzset", "[b", "[e"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":4\r\n"); // b, c, d, e

    // ZLEXCOUNT (b, (e) - exclusive
    send_array(&mut write_half, &["ZLEXCOUNT", "myzset", "(b", "(e"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":2\r\n"); // c, d

    // ZLEXCOUNT [b, (d) - mixed
    send_array(&mut write_half, &["ZLEXCOUNT", "myzset", "[b", "(d"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":2\r\n"); // b, c

    // ZLEXCOUNT 不存在的 key
    send_array(&mut write_half, &["ZLEXCOUNT", "nokey", "-", "+"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":0\r\n");

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_generic_commands() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // 创建测试数据
    send_array(&mut write_half, &["SET", "key1", "value1"]).await;
    let _ = read_line_helper(&mut reader).await;
    send_array(&mut write_half, &["LPUSH", "list1", "a", "b", "c"]).await;
    let _ = read_line_helper(&mut reader).await;

    // UNLINK - 与 DEL 相同
    send_array(&mut write_half, &["SET", "temp", "val"]).await;
    let _ = read_line_helper(&mut reader).await;
    send_array(&mut write_half, &["UNLINK", "temp"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":1\r\n");
    send_array(&mut write_half, &["EXISTS", "temp"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":0\r\n");

    // COPY - 复制 key
    send_array(&mut write_half, &["COPY", "key1", "key2"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":1\r\n");
    send_array(&mut write_half, &["GET", "key2"]).await;
    let _ = read_line_helper(&mut reader).await; // $6
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "value1\r\n");

    // COPY 不覆盖已存在的 key
    send_array(&mut write_half, &["COPY", "key1", "key2"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":0\r\n");

    // COPY source == destination 应返回错误
    send_array(&mut write_half, &["COPY", "key1", "key1"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert!(resp.starts_with("-ERR"));

    // COPY source == destination with REPLACE 也应返回错误
    send_array(&mut write_half, &["COPY", "key1", "key1", "REPLACE"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert!(resp.starts_with("-ERR"));

    // COPY REPLACE
    send_array(&mut write_half, &["SET", "key1", "newvalue"]).await;
    let _ = read_line_helper(&mut reader).await;
    send_array(&mut write_half, &["COPY", "key1", "key2", "REPLACE"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":1\r\n");
    send_array(&mut write_half, &["GET", "key2"]).await;
    let _ = read_line_helper(&mut reader).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "newvalue\r\n");

    // TOUCH
    send_array(&mut write_half, &["TOUCH", "key1", "key2", "nonexistent"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":2\r\n");

    // OBJECT ENCODING - string
    send_array(&mut write_half, &["OBJECT", "ENCODING", "key1"]).await;
    let _ = read_line_helper(&mut reader).await; // $6
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "embstr\r\n");

    // OBJECT ENCODING - list
    send_array(&mut write_half, &["OBJECT", "ENCODING", "list1"]).await;
    let _ = read_line_helper(&mut reader).await; // $9
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "quicklist\r\n");

    // OBJECT ENCODING - 不存在的 key
    send_array(&mut write_half, &["OBJECT", "ENCODING", "nokey"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "$-1\r\n");

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_expire_commands() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // 创建测试数据
    send_array(&mut write_half, &["SET", "mykey", "value"]).await;
    let _ = read_line_helper(&mut reader).await;

    // EXPIREAT - 设置绝对过期时间（未来 100 秒）
    let future_ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() + 100;
    send_array(&mut write_half, &["EXPIREAT", "mykey", &future_ts.to_string()]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":1\r\n");

    // EXPIRETIME - 获取绝对过期时间
    send_array(&mut write_half, &["EXPIRETIME", "mykey"]).await;
    let resp = read_line_helper(&mut reader).await;
    // 应该返回接近 future_ts 的值
    assert!(resp.starts_with(":"));
    let ts: i64 = resp.trim_start_matches(':').trim().parse().unwrap();
    assert!(ts > 0);

    // PEXPIREAT - 设置绝对过期时间（毫秒）
    send_array(&mut write_half, &["SET", "mykey2", "value2"]).await;
    let _ = read_line_helper(&mut reader).await;
    let future_ts_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64 + 100000;
    send_array(&mut write_half, &["PEXPIREAT", "mykey2", &future_ts_ms.to_string()]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":1\r\n");

    // PEXPIRETIME - 获取绝对过期时间（毫秒）
    send_array(&mut write_half, &["PEXPIRETIME", "mykey2"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert!(resp.starts_with(":"));
    let ts_ms: i64 = resp.trim_start_matches(':').trim().parse().unwrap();
    assert!(ts_ms > 0);

    // EXPIRETIME 不存在的 key
    send_array(&mut write_half, &["EXPIRETIME", "nokey"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":-2\r\n");

    // EXPIRETIME 无过期时间的 key
    send_array(&mut write_half, &["SET", "noexpire", "val"]).await;
    let _ = read_line_helper(&mut reader).await;
    send_array(&mut write_half, &["EXPIRETIME", "noexpire"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":-1\r\n");

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_set_commands_extended() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // 创建测试集合
    send_array(&mut write_half, &["SADD", "set1", "a", "b", "c"]).await;
    let _ = read_line_helper(&mut reader).await;
    send_array(&mut write_half, &["SADD", "set2", "x", "y"]).await;
    let _ = read_line_helper(&mut reader).await;

    // SMOVE - 移动元素
    send_array(&mut write_half, &["SMOVE", "set1", "set2", "a"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":1\r\n");

    // 验证 a 已从 set1 移除
    send_array(&mut write_half, &["SISMEMBER", "set1", "a"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":0\r\n");

    // 验证 a 已添加到 set2
    send_array(&mut write_half, &["SISMEMBER", "set2", "a"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":1\r\n");

    // SMOVE - 元素不存在
    send_array(&mut write_half, &["SMOVE", "set1", "set2", "notexist"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":0\r\n");

    // SMOVE - source 不存在
    send_array(&mut write_half, &["SMOVE", "noset", "set2", "x"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":0\r\n");

    // SMOVE - destination 不存在（应自动创建）
    send_array(&mut write_half, &["SMOVE", "set1", "newset", "b"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":1\r\n");
    send_array(&mut write_half, &["SISMEMBER", "newset", "b"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, ":1\r\n");

    // SPOP - 随机弹出
    send_array(&mut write_half, &["SADD", "popset", "1", "2", "3"]).await;
    let _ = read_line_helper(&mut reader).await;
    send_array(&mut write_half, &["SPOP", "popset"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert!(resp.starts_with("$1\r\n")); // 弹出一个元素
    let _ = read_line_helper(&mut reader).await; // 读取元素值

    // SRANDMEMBER - 随机获取（不删除）
    send_array(&mut write_half, &["SRANDMEMBER", "popset"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert!(resp.starts_with("$1\r\n"));
    let _ = read_line_helper(&mut reader).await;

    // SRANDMEMBER 获取多个
    send_array(&mut write_half, &["SRANDMEMBER", "popset", "2"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert!(resp.starts_with("*")); // 数组响应

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_blocking_list_commands() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // 先创建有数据的列表
    send_array(&mut write_half, &["RPUSH", "mylist", "a", "b", "c"]).await;
    let _ = read_line_helper(&mut reader).await;

    // BLPOP - 立即返回（列表有数据）
    send_array(&mut write_half, &["BLPOP", "mylist", "1"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "*2\r\n"); // 数组响应
    let _ = read_line_helper(&mut reader).await; // $6
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "mylist\r\n");
    let _ = read_line_helper(&mut reader).await; // $1
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "a\r\n");

    // BRPOP - 立即返回
    send_array(&mut write_half, &["BRPOP", "mylist", "1"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "*2\r\n");
    let _ = read_line_helper(&mut reader).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "mylist\r\n");
    let _ = read_line_helper(&mut reader).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "c\r\n"); // 从右边弹出

    // BLPOP - 超时返回 null（列表为空后）
    send_array(&mut write_half, &["LPOP", "mylist"]).await; // 清空列表
    let _ = read_line_helper(&mut reader).await;
    let _ = read_line_helper(&mut reader).await;
    
    send_array(&mut write_half, &["BLPOP", "emptylist", "0.2"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "$-1\r\n"); // null bulk

    // BLPOP - 多个 key，返回第一个有数据的
    send_array(&mut write_half, &["RPUSH", "list2", "x"]).await;
    let _ = read_line_helper(&mut reader).await;
    send_array(&mut write_half, &["BLPOP", "emptylist", "list2", "1"]).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "*2\r\n");
    let _ = read_line_helper(&mut reader).await;
    let resp = read_line_helper(&mut reader).await;
    assert_eq!(resp, "list2\r\n"); // 返回 list2 的数据

    let _ = shutdown.send(());
}
