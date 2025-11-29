use std::net::SocketAddr;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::time::{Duration, Instant};

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

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn info_basic_fields() {
    let (addr, shutdown, handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    write_half
        .write_all(b"*1\r\n$4\r\nINFO\r\n")
        .await
        .unwrap();

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
    assert_eq!(members, vec!["a".to_string(), "b".to_string(), "c".to_string()]);

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

    write_half
        .write_all(b"*1\r\n$4\r\nPING\r\n")
        .await
        .unwrap();
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

    write_half
        .write_all(b"*1\r\n$4\r\nQUIT\r\n")
        .await
        .unwrap();
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
        write_half
            .write_all(b"*1\r\n$4\r\nPING\r\n")
            .await
            .unwrap();
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        assert_eq!(line, "+PONG\r\n");
        line.clear();
    }
    let elapsed = start.elapsed();
    assert!(elapsed < Duration::from_secs(2), "Ping loop took too long: {:?}", elapsed);

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}
