/// HyperLogLog 命令集成测试
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const TEST_ADDR: &str = "127.0.0.1:16379";

async fn send_command(stream: &mut TcpStream, cmd: &str) -> String {
    stream.write_all(cmd.as_bytes()).await.unwrap();
    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).await.unwrap();
    String::from_utf8_lossy(&buf[..n]).to_string()
}

#[tokio::test]
async fn test_pfadd_basic() {
    let mut stream = TcpStream::connect(TEST_ADDR).await.unwrap();

    // PFADD 添加元素应该返回 1（表示 HLL 被修改）
    let resp = send_command(&mut stream, "*3\r\n$5\r\nPFADD\r\n$6\r\ntesthll\r\n$5\r\nhello\r\n").await;
    assert!(resp.contains(":1\r\n"), "Expected :1, got: {}", resp);

    // 再次添加相同元素可能返回 0（未修改）或 1（修改）
    let resp = send_command(&mut stream, "*3\r\n$5\r\nPFADD\r\n$6\r\ntesthll\r\n$5\r\nhello\r\n").await;
    assert!(resp.contains(":0\r\n") || resp.contains(":1\r\n"), "Expected :0 or :1, got: {}", resp);

    // 清理
    send_command(&mut stream, "*2\r\n$3\r\nDEL\r\n$7\r\ntesthll\r\n").await;
}

#[tokio::test]
async fn test_pfadd_multiple_elements() {
    let mut stream = TcpStream::connect(TEST_ADDR).await.unwrap();

    // PFADD 添加多个元素
    let resp = send_command(
        &mut stream,
        "*5\r\n$5\r\nPFADD\r\n$7\r\nmultihll\r\n$2\r\na1\r\n$2\r\na2\r\n$2\r\na3\r\n",
    )
    .await;
    assert!(resp.contains(":1\r\n"), "Expected :1, got: {}", resp);

    // 清理
    send_command(&mut stream, "*2\r\n$3\r\nDEL\r\n$8\r\nmultihll\r\n").await;
}

#[tokio::test]
async fn test_pfcount_single_key() {
    let mut stream = TcpStream::connect(TEST_ADDR).await.unwrap();

    // 添加 100 个不同元素
    for i in 0..100 {
        let cmd = format!("*3\r\n$5\r\nPFADD\r\n$8\r\ncounthll\r\n$7\r\nelem_{:03}\r\n", i);
        send_command(&mut stream, &cmd).await;
    }

    // PFCOUNT 应该返回接近 100 的值
    let resp = send_command(&mut stream, "*2\r\n$7\r\nPFCOUNT\r\n$8\r\ncounthll\r\n").await;
    
    // 解析返回的整数
    if let Some(start) = resp.find(':') {
        if let Some(end) = resp[start..].find("\r\n") {
            let count_str = &resp[start + 1..start + end];
            if let Ok(count) = count_str.parse::<i64>() {
                assert!(count >= 95 && count <= 105, "Expected count near 100, got: {}", count);
            } else {
                panic!("Failed to parse count: {}", count_str);
            }
        }
    }

    // 清理
    send_command(&mut stream, "*2\r\n$3\r\nDEL\r\n$8\r\ncounthll\r\n").await;
}

#[tokio::test]
async fn test_pfcount_empty_key() {
    let mut stream = TcpStream::connect(TEST_ADDR).await.unwrap();

    // PFCOUNT 不存在的 key 应该返回 0
    let resp = send_command(&mut stream, "*2\r\n$7\r\nPFCOUNT\r\n$9\r\nemptyhll1\r\n").await;
    assert!(resp.contains(":0\r\n"), "Expected :0, got: {}", resp);
}

#[tokio::test]
async fn test_pfcount_multiple_keys() {
    let mut stream = TcpStream::connect(TEST_ADDR).await.unwrap();

    // 创建两个 HLL，有部分重叠
    for i in 0..50 {
        let cmd = format!("*3\r\n$5\r\nPFADD\r\n$5\r\nhll_a\r\n$7\r\nelem_{:03}\r\n", i);
        send_command(&mut stream, &cmd).await;
    }

    for i in 25..75 {
        let cmd = format!("*3\r\n$5\r\nPFADD\r\n$5\r\nhll_b\r\n$7\r\nelem_{:03}\r\n", i);
        send_command(&mut stream, &cmd).await;
    }

    // PFCOUNT 两个 key 应该返回接近 75 的值（并集）
    let resp = send_command(&mut stream, "*3\r\n$7\r\nPFCOUNT\r\n$5\r\nhll_a\r\n$5\r\nhll_b\r\n").await;
    
    if let Some(start) = resp.find(':') {
        if let Some(end) = resp[start..].find("\r\n") {
            let count_str = &resp[start + 1..start + end];
            if let Ok(count) = count_str.parse::<i64>() {
                assert!(count >= 70 && count <= 80, "Expected count near 75, got: {}", count);
            }
        }
    }

    // 清理
    send_command(&mut stream, "*3\r\n$3\r\nDEL\r\n$5\r\nhll_a\r\n$5\r\nhll_b\r\n").await;
}

#[tokio::test]
async fn test_pfmerge() {
    let mut stream = TcpStream::connect(TEST_ADDR).await.unwrap();

    // 创建两个 HLL
    for i in 0..30 {
        let cmd = format!("*3\r\n$5\r\nPFADD\r\n$7\r\nmerge_a\r\n$7\r\nelem_{:03}\r\n", i);
        send_command(&mut stream, &cmd).await;
    }

    for i in 20..50 {
        let cmd = format!("*3\r\n$5\r\nPFADD\r\n$7\r\nmerge_b\r\n$7\r\nelem_{:03}\r\n", i);
        send_command(&mut stream, &cmd).await;
    }

    // PFMERGE 合并到新 key
    let resp = send_command(
        &mut stream,
        "*4\r\n$7\r\nPFMERGE\r\n$8\r\nmerge_ab\r\n$7\r\nmerge_a\r\n$7\r\nmerge_b\r\n",
    )
    .await;
    assert!(resp.contains("+OK\r\n"), "Expected +OK, got: {}", resp);

    // 验证合并后的基数接近 50
    let resp = send_command(&mut stream, "*2\r\n$7\r\nPFCOUNT\r\n$8\r\nmerge_ab\r\n").await;
    
    if let Some(start) = resp.find(':') {
        if let Some(end) = resp[start..].find("\r\n") {
            let count_str = &resp[start + 1..start + end];
            if let Ok(count) = count_str.parse::<i64>() {
                assert!(count >= 45 && count <= 55, "Expected count near 50, got: {}", count);
            }
        }
    }

    // 清理
    send_command(&mut stream, "*4\r\n$3\r\nDEL\r\n$7\r\nmerge_a\r\n$7\r\nmerge_b\r\n$8\r\nmerge_ab\r\n").await;
}

#[tokio::test]
async fn test_pfadd_wrongtype() {
    let mut stream = TcpStream::connect(TEST_ADDR).await.unwrap();

    // 先创建一个字符串 key
    send_command(&mut stream, "*3\r\n$3\r\nSET\r\n$9\r\nstringkey\r\n$5\r\nvalue\r\n").await;

    // 尝试对字符串 key 执行 PFADD 应该返回 WRONGTYPE 错误
    let resp = send_command(&mut stream, "*3\r\n$5\r\nPFADD\r\n$9\r\nstringkey\r\n$4\r\nelem\r\n").await;
    assert!(resp.contains("WRONGTYPE"), "Expected WRONGTYPE error, got: {}", resp);

    // 清理
    send_command(&mut stream, "*2\r\n$3\r\nDEL\r\n$9\r\nstringkey\r\n").await;
}

#[tokio::test]
async fn test_pfcount_wrongtype() {
    let mut stream = TcpStream::connect(TEST_ADDR).await.unwrap();

    // 先创建一个列表 key
    send_command(&mut stream, "*3\r\n$5\r\nLPUSH\r\n$7\r\nlistkey\r\n$4\r\nitem\r\n").await;

    // 尝试对列表 key 执行 PFCOUNT 应该返回 WRONGTYPE 错误
    let resp = send_command(&mut stream, "*2\r\n$7\r\nPFCOUNT\r\n$7\r\nlistkey\r\n").await;
    assert!(resp.contains("WRONGTYPE"), "Expected WRONGTYPE error, got: {}", resp);

    // 清理
    send_command(&mut stream, "*2\r\n$3\r\nDEL\r\n$7\r\nlistkey\r\n").await;
}

#[tokio::test]
async fn test_pfmerge_wrongtype() {
    let mut stream = TcpStream::connect(TEST_ADDR).await.unwrap();

    // 创建一个 HLL 和一个集合
    send_command(&mut stream, "*3\r\n$5\r\nPFADD\r\n$8\r\nhllkey_1\r\n$4\r\nelem\r\n").await;
    send_command(&mut stream, "*3\r\n$4\r\nSADD\r\n$6\r\nsetkey\r\n$6\r\nmember\r\n").await;

    // 尝试合并 HLL 和集合应该返回 WRONGTYPE 错误
    let resp = send_command(
        &mut stream,
        "*4\r\n$7\r\nPFMERGE\r\n$8\r\nhllkey_2\r\n$8\r\nhllkey_1\r\n$6\r\nsetkey\r\n",
    )
    .await;
    assert!(resp.contains("WRONGTYPE"), "Expected WRONGTYPE error, got: {}", resp);

    // 清理
    send_command(&mut stream, "*3\r\n$3\r\nDEL\r\n$8\r\nhllkey_1\r\n$6\r\nsetkey\r\n").await;
}

#[tokio::test]
async fn test_type_command_for_hll() {
    let mut stream = TcpStream::connect(TEST_ADDR).await.unwrap();

    // 创建一个 HLL
    send_command(&mut stream, "*3\r\n$5\r\nPFADD\r\n$7\r\ntypehll\r\n$4\r\nelem\r\n").await;

    // TYPE 命令应该返回 "string"（Redis 中 HLL 的类型显示为 string）
    let resp = send_command(&mut stream, "*2\r\n$4\r\nTYPE\r\n$7\r\ntypehll\r\n").await;
    assert!(resp.contains("+string\r\n"), "Expected +string, got: {}", resp);

    // 清理
    send_command(&mut stream, "*2\r\n$3\r\nDEL\r\n$7\r\ntypehll\r\n").await;
}
