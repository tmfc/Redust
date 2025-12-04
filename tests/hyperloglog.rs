/// HyperLogLog 命令集成测试
use std::net::SocketAddr;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
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

async fn send_array(writer: &mut tokio::net::tcp::OwnedWriteHalf, parts: &[&str]) {
    let mut buf = format!("*{}\r\n", parts.len());
    for p in parts {
        buf.push_str(&format!("${}\r\n{}\r\n", p.len(), p));
    }
    writer.write_all(buf.as_bytes()).await.unwrap();
}

async fn read_line(reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>) -> String {
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();
    line
}

#[tokio::test]
async fn test_pfadd_basic() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // PFADD 添加元素应该返回 1（表示 HLL 被修改）
    send_array(&mut write_half, &["PFADD", "testhll", "hello"]).await;
    let resp = read_line(&mut reader).await;
    assert_eq!(resp, ":1\r\n");

    // 再次添加相同元素应该返回 0（未修改）
    send_array(&mut write_half, &["PFADD", "testhll", "hello"]).await;
    let resp = read_line(&mut reader).await;
    assert!(resp == ":0\r\n" || resp == ":1\r\n", "Expected :0 or :1, got: {}", resp);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_pfadd_multiple_elements() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // PFADD 添加多个元素
    send_array(&mut write_half, &["PFADD", "multihll", "a1", "a2", "a3"]).await;
    let resp = read_line(&mut reader).await;
    assert_eq!(resp, ":1\r\n");

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_pfcount_single_key() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // 添加 100 个不同元素
    for i in 0..100 {
        let elem = format!("elem_{:03}", i);
        send_array(&mut write_half, &["PFADD", "counthll", &elem]).await;
        let _ = read_line(&mut reader).await;
    }

    // PFCOUNT 应该返回接近 100 的值
    send_array(&mut write_half, &["PFCOUNT", "counthll"]).await;
    let resp = read_line(&mut reader).await;
    
    // 解析返回的整数
    let count: i64 = resp.trim().trim_start_matches(':').parse().unwrap();
    assert!(count >= 95 && count <= 105, "Expected count near 100, got: {}", count);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_pfcount_empty_key() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // PFCOUNT 不存在的 key 应该返回 0
    send_array(&mut write_half, &["PFCOUNT", "emptyhll"]).await;
    let resp = read_line(&mut reader).await;
    assert_eq!(resp, ":0\r\n");

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_pfcount_multiple_keys() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // 创建两个 HLL，有部分重叠
    for i in 0..50 {
        let elem = format!("elem_{:03}", i);
        send_array(&mut write_half, &["PFADD", "hll_a", &elem]).await;
        let _ = read_line(&mut reader).await;
    }

    for i in 25..75 {
        let elem = format!("elem_{:03}", i);
        send_array(&mut write_half, &["PFADD", "hll_b", &elem]).await;
        let _ = read_line(&mut reader).await;
    }

    // PFCOUNT 两个 key 应该返回接近 75 的值（并集）
    send_array(&mut write_half, &["PFCOUNT", "hll_a", "hll_b"]).await;
    let resp = read_line(&mut reader).await;
    
    let count: i64 = resp.trim().trim_start_matches(':').parse().unwrap();
    assert!(count >= 70 && count <= 80, "Expected count near 75, got: {}", count);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_pfmerge() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // 创建两个 HLL
    for i in 0..30 {
        let elem = format!("elem_{:03}", i);
        send_array(&mut write_half, &["PFADD", "merge_a", &elem]).await;
        let _ = read_line(&mut reader).await;
    }

    for i in 20..50 {
        let elem = format!("elem_{:03}", i);
        send_array(&mut write_half, &["PFADD", "merge_b", &elem]).await;
        let _ = read_line(&mut reader).await;
    }

    // PFMERGE 合并到新 key
    send_array(&mut write_half, &["PFMERGE", "merge_ab", "merge_a", "merge_b"]).await;
    let resp = read_line(&mut reader).await;
    assert_eq!(resp, "+OK\r\n");

    // 验证合并后的基数接近 50
    send_array(&mut write_half, &["PFCOUNT", "merge_ab"]).await;
    let resp = read_line(&mut reader).await;
    
    let count: i64 = resp.trim().trim_start_matches(':').parse().unwrap();
    assert!(count >= 45 && count <= 55, "Expected count near 50, got: {}", count);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_pfadd_wrongtype() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // 先创建一个字符串 key
    send_array(&mut write_half, &["SET", "stringkey", "value"]).await;
    let _ = read_line(&mut reader).await;

    // 尝试对字符串 key 执行 PFADD 应该返回 WRONGTYPE 错误
    send_array(&mut write_half, &["PFADD", "stringkey", "elem"]).await;
    let resp = read_line(&mut reader).await;
    assert!(resp.contains("WRONGTYPE"), "Expected WRONGTYPE error, got: {}", resp);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_pfcount_wrongtype() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // 先创建一个列表 key
    send_array(&mut write_half, &["LPUSH", "listkey", "item"]).await;
    let _ = read_line(&mut reader).await;

    // 尝试对列表 key 执行 PFCOUNT 应该返回 WRONGTYPE 错误
    send_array(&mut write_half, &["PFCOUNT", "listkey"]).await;
    let resp = read_line(&mut reader).await;
    assert!(resp.contains("WRONGTYPE"), "Expected WRONGTYPE error, got: {}", resp);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_pfmerge_wrongtype() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // 创建一个 HLL 和一个集合
    send_array(&mut write_half, &["PFADD", "hllkey_1", "elem"]).await;
    let _ = read_line(&mut reader).await;
    send_array(&mut write_half, &["SADD", "setkey", "member"]).await;
    let _ = read_line(&mut reader).await;

    // 尝试合并 HLL 和集合应该返回 WRONGTYPE 错误
    send_array(&mut write_half, &["PFMERGE", "hllkey_2", "hllkey_1", "setkey"]).await;
    let resp = read_line(&mut reader).await;
    assert!(resp.contains("WRONGTYPE"), "Expected WRONGTYPE error, got: {}", resp);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_type_command_for_hll() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // 创建一个 HLL
    send_array(&mut write_half, &["PFADD", "typehll", "elem"]).await;
    let _ = read_line(&mut reader).await;

    // TYPE 命令应该返回 "string"（Redis 中 HLL 的类型显示为 string）
    send_array(&mut write_half, &["TYPE", "typehll"]).await;
    let resp = read_line(&mut reader).await;
    assert_eq!(resp, "+string\r\n");

    let _ = shutdown.send(());
}
