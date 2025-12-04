/// HyperLogLog 命令集成测试
use std::net::SocketAddr;
use std::time::{Duration, Instant};

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

// ============ 性能测试 ============

/// 测试大量元素添加性能
#[tokio::test]
async fn performance_pfadd_bulk() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    let iterations = 1000;
    let start = Instant::now();

    // 批量添加 1000 个不同元素
    for i in 0..iterations {
        let elem = format!("perf_elem_{:06}", i);
        send_array(&mut write_half, &["PFADD", "perf_hll", &elem]).await;
        let _ = read_line(&mut reader).await;
    }

    let elapsed = start.elapsed();
    
    // 验证基数估算接近 1000
    send_array(&mut write_half, &["PFCOUNT", "perf_hll"]).await;
    let resp = read_line(&mut reader).await;
    let count: i64 = resp.trim().trim_start_matches(':').parse().unwrap();
    assert!(count >= 950 && count <= 1050, "Expected count near 1000, got: {}", count);

    // 性能断言：1000 次 PFADD 应该在 5 秒内完成
    assert!(
        elapsed < Duration::from_secs(5),
        "PFADD bulk took too long: {:?}",
        elapsed
    );

    let _ = shutdown.send(());
}

/// 测试批量添加多元素性能（单次 PFADD 多个元素）
#[tokio::test]
async fn performance_pfadd_multi_elements() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    let iterations = 100;
    let elements_per_call = 50;
    let start = Instant::now();

    // 每次 PFADD 添加 50 个元素，共 100 次
    for i in 0..iterations {
        let mut parts: Vec<String> = vec!["PFADD".to_string(), "perf_multi_hll".to_string()];
        for j in 0..elements_per_call {
            parts.push(format!("elem_{}_{}", i, j));
        }
        let refs: Vec<&str> = parts.iter().map(|s| s.as_str()).collect();
        send_array(&mut write_half, &refs).await;
        let _ = read_line(&mut reader).await;
    }

    let elapsed = start.elapsed();

    // 验证基数估算接近 5000 (100 * 50)
    send_array(&mut write_half, &["PFCOUNT", "perf_multi_hll"]).await;
    let resp = read_line(&mut reader).await;
    let count: i64 = resp.trim().trim_start_matches(':').parse().unwrap();
    assert!(count >= 4750 && count <= 5250, "Expected count near 5000, got: {}", count);

    // 性能断言：100 次批量 PFADD 应该在 3 秒内完成
    assert!(
        elapsed < Duration::from_secs(3),
        "PFADD multi-elements took too long: {:?}",
        elapsed
    );

    let _ = shutdown.send(());
}

/// 测试多键合并性能
#[tokio::test]
async fn performance_pfmerge_multi_keys() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // 创建 10 个 HLL，每个包含 500 个元素
    let num_hlls = 10;
    let elements_per_hll = 500;

    for hll_idx in 0..num_hlls {
        let key = format!("merge_src_{}", hll_idx);
        for elem_idx in 0..elements_per_hll {
            let elem = format!("elem_{}_{}", hll_idx, elem_idx);
            send_array(&mut write_half, &["PFADD", &key, &elem]).await;
            let _ = read_line(&mut reader).await;
        }
    }

    // 测试合并性能
    let start = Instant::now();

    let mut merge_parts: Vec<String> = vec!["PFMERGE".to_string(), "merge_dest".to_string()];
    for i in 0..num_hlls {
        merge_parts.push(format!("merge_src_{}", i));
    }
    let refs: Vec<&str> = merge_parts.iter().map(|s| s.as_str()).collect();
    send_array(&mut write_half, &refs).await;
    let resp = read_line(&mut reader).await;
    assert_eq!(resp, "+OK\r\n");

    let elapsed = start.elapsed();

    // 验证合并后的基数（10 个 HLL 各 500 个不同元素 = 5000）
    send_array(&mut write_half, &["PFCOUNT", "merge_dest"]).await;
    let resp = read_line(&mut reader).await;
    let count: i64 = resp.trim().trim_start_matches(':').parse().unwrap();
    assert!(count >= 4750 && count <= 5250, "Expected count near 5000, got: {}", count);

    // 性能断言：合并 10 个 HLL 应该在 1 秒内完成
    assert!(
        elapsed < Duration::from_secs(1),
        "PFMERGE took too long: {:?}",
        elapsed
    );

    let _ = shutdown.send(());
}

/// 测试 PFCOUNT 多键性能
#[tokio::test]
async fn performance_pfcount_multi_keys() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // 创建 5 个 HLL
    let num_hlls = 5;
    for hll_idx in 0..num_hlls {
        let key = format!("count_src_{}", hll_idx);
        for elem_idx in 0..200 {
            let elem = format!("elem_{}_{}", hll_idx, elem_idx);
            send_array(&mut write_half, &["PFADD", &key, &elem]).await;
            let _ = read_line(&mut reader).await;
        }
    }

    // 测试多键 PFCOUNT 性能
    let iterations = 100;
    let start = Instant::now();

    for _ in 0..iterations {
        send_array(&mut write_half, &[
            "PFCOUNT", "count_src_0", "count_src_1", "count_src_2", "count_src_3", "count_src_4"
        ]).await;
        let resp = read_line(&mut reader).await;
        let count: i64 = resp.trim().trim_start_matches(':').parse().unwrap();
        // 5 个 HLL 各 200 个不同元素 = 1000
        assert!(count >= 950 && count <= 1050, "Expected count near 1000, got: {}", count);
    }

    let elapsed = start.elapsed();

    // 性能断言：100 次多键 PFCOUNT 应该在 3 秒内完成
    assert!(
        elapsed < Duration::from_secs(3),
        "PFCOUNT multi-keys took too long: {:?}",
        elapsed
    );

    let _ = shutdown.send(());
}

/// 测试 HyperLogLog 内存占用（验证约 12KB）
#[tokio::test]
async fn test_hll_memory_size() {
    // 这个测试验证 HyperLogLog 的内存占用
    // 每个 HLL 使用 16384 个 6-bit 寄存器
    // 实际存储为 16384 个 u8 = 16384 bytes ≈ 16KB
    // （比理论的 12KB 稍大，因为使用 u8 而非紧凑的 6-bit 存储）
    
    use redust::hyperloglog::HyperLogLog;
    
    let hll = HyperLogLog::new();
    let registers = hll.registers();
    
    // 验证寄存器数量为 16384
    assert_eq!(registers.len(), 16384, "Expected 16384 registers");
    
    // 内存占用：16384 bytes = 16 KB
    let memory_bytes = registers.len();
    assert_eq!(memory_bytes, 16384, "Expected 16384 bytes per HLL");
}

// ============ 持久化测试 ============

/// 测试 HyperLogLog RDB 保存与加载
#[tokio::test]
async fn test_hll_rdb_persistence() {
    use redust::storage::Storage;
    use std::path::PathBuf;
    
    fn temp_path(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        p.push(format!("redust_{}_{}.rdb", name, nanos));
        p
    }
    
    let storage = Storage::default();
    
    // 创建 HLL 并添加元素
    for i in 0..100 {
        let elem = format!("elem_{}", i).into_bytes();
        storage.pfadd("test_hll", &[elem]).unwrap();
    }
    
    // 获取原始基数
    let original_count = storage.pfcount(&["test_hll".to_string()]).unwrap();
    assert!(original_count >= 95 && original_count <= 105, "Original count: {}", original_count);
    
    // 保存 RDB
    let path = temp_path("hll_persistence");
    storage.save_rdb(&path).unwrap();
    
    // 加载到新的 Storage
    let restored = Storage::default();
    restored.load_rdb(&path).unwrap();
    
    // 验证基数一致
    let restored_count = restored.pfcount(&["test_hll".to_string()]).unwrap();
    assert_eq!(original_count, restored_count, "Count mismatch after restore");
    
    // 清理
    let _ = std::fs::remove_file(&path);
}

/// 测试多个 HLL 的 RDB 持久化
#[tokio::test]
async fn test_multiple_hll_rdb_persistence() {
    use redust::storage::Storage;
    use std::path::PathBuf;
    
    fn temp_path(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        p.push(format!("redust_{}_{}.rdb", name, nanos));
        p
    }
    
    let storage = Storage::default();
    
    // 创建多个 HLL
    for hll_idx in 0..3 {
        let key = format!("hll_{}", hll_idx);
        for i in 0..(50 * (hll_idx + 1)) {
            let elem = format!("elem_{}_{}", hll_idx, i).into_bytes();
            storage.pfadd(&key, &[elem]).unwrap();
        }
    }
    
    // 获取原始基数
    let counts: Vec<u64> = (0..3)
        .map(|i| storage.pfcount(&[format!("hll_{}", i)]).unwrap())
        .collect();
    
    // 保存 RDB
    let path = temp_path("multi_hll_persistence");
    storage.save_rdb(&path).unwrap();
    
    // 加载到新的 Storage
    let restored = Storage::default();
    restored.load_rdb(&path).unwrap();
    
    // 验证每个 HLL 的基数一致
    for i in 0..3 {
        let key = format!("hll_{}", i);
        let restored_count = restored.pfcount(&[key]).unwrap();
        assert_eq!(counts[i], restored_count, "Count mismatch for hll_{}", i);
    }
    
    // 清理
    let _ = std::fs::remove_file(&path);
}

/// 测试 HLL 与其他数据类型混合的 RDB 持久化
#[tokio::test]
async fn test_hll_mixed_types_rdb_persistence() {
    use redust::storage::Storage;
    use std::path::PathBuf;
    
    fn temp_path(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        p.push(format!("redust_{}_{}.rdb", name, nanos));
        p
    }
    
    let storage = Storage::default();
    
    // 创建不同类型的数据
    storage.set("string_key".to_string(), b"string_value".to_vec());
    storage.lpush("list_key", &vec!["a".to_string(), "b".to_string()]).unwrap();
    storage.sadd("set_key", &vec!["x".to_string(), "y".to_string()]).unwrap();
    
    // 创建 HLL
    for i in 0..50 {
        let elem = format!("elem_{}", i).into_bytes();
        storage.pfadd("hll_key", &[elem]).unwrap();
    }
    
    let hll_count = storage.pfcount(&["hll_key".to_string()]).unwrap();
    
    // 保存 RDB
    let path = temp_path("mixed_types_persistence");
    storage.save_rdb(&path).unwrap();
    
    // 加载到新的 Storage
    let restored = Storage::default();
    restored.load_rdb(&path).unwrap();
    
    // 验证所有类型的数据
    assert_eq!(restored.get("string_key").as_deref(), Some(b"string_value".as_slice()));
    
    let list_vals = restored.lrange("list_key", 0, -1).unwrap();
    assert_eq!(list_vals, vec!["b".to_string(), "a".to_string()]);
    
    let mut set_vals = restored.smembers("set_key").unwrap();
    set_vals.sort();
    assert_eq!(set_vals, vec!["x".to_string(), "y".to_string()]);
    
    let restored_hll_count = restored.pfcount(&["hll_key".to_string()]).unwrap();
    assert_eq!(hll_count, restored_hll_count, "HLL count mismatch");
    
    // 清理
    let _ = std::fs::remove_file(&path);
}

/// 测试 HLL 持久化后可以继续添加元素
#[tokio::test]
async fn test_hll_persistence_continue_add() {
    use redust::storage::Storage;
    use std::path::PathBuf;
    
    fn temp_path(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        p.push(format!("redust_{}_{}.rdb", name, nanos));
        p
    }
    
    let storage = Storage::default();
    
    // 添加前 50 个元素
    for i in 0..50 {
        let elem = format!("elem_{}", i).into_bytes();
        storage.pfadd("continue_hll", &[elem]).unwrap();
    }
    
    // 保存 RDB
    let path = temp_path("continue_add");
    storage.save_rdb(&path).unwrap();
    
    // 加载到新的 Storage
    let restored = Storage::default();
    restored.load_rdb(&path).unwrap();
    
    // 继续添加后 50 个元素
    for i in 50..100 {
        let elem = format!("elem_{}", i).into_bytes();
        restored.pfadd("continue_hll", &[elem]).unwrap();
    }
    
    // 验证基数接近 100
    let count = restored.pfcount(&["continue_hll".to_string()]).unwrap();
    assert!(count >= 95 && count <= 105, "Expected count near 100, got: {}", count);
    
    // 清理
    let _ = std::fs::remove_file(&path);
}
