use std::io::Write;
use std::path::PathBuf;

use redust::storage::{RdbLoadMode, Storage};

fn temp_path(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    p.push(format!("redust_{}_{}.rdb", name, nanos));
    p
}

#[tokio::test]
async fn rdb_basic_roundtrip_via_storage() {
    let storage = Storage::default();

    storage
        .set("foo".to_string(), b"bar".to_vec())
        .expect("set foo");
    storage
        .lpush("mylist", &vec!["a".to_string(), "b".to_string()])
        .unwrap();
    storage
        .sadd("myset", &vec!["x".to_string(), "y".to_string()])
        .unwrap();
    storage.hset("myhash", "field", "val".to_string()).unwrap();

    storage
        .set("ttl_key".to_string(), b"tv".to_vec())
        .expect("set ttl_key");
    let _ = storage.expire_seconds("ttl_key", 10);

    let path = temp_path("roundtrip");
    storage.save_rdb(&path).unwrap();

    let restored = Storage::default();
    restored.load_rdb(&path).unwrap();

    assert_eq!(restored.get("foo").as_deref(), Some("bar".as_bytes()));

    let lvals = restored.lrange("mylist", 0, -1).unwrap();
    // Redis 语义：LPUSH mylist a b -> 列表内容为 ["b", "a"]
    assert_eq!(lvals, vec!["b".to_string(), "a".to_string()]);

    let mut svals = restored.smembers("myset").unwrap();
    svals.sort();
    assert_eq!(svals, vec!["x".to_string(), "y".to_string()]);

    let hvals = restored.hgetall("myhash").unwrap();
    assert_eq!(hvals, vec![("field".to_string(), "val".to_string())]);

    let ttl = restored.ttl_seconds("ttl_key");
    assert!(ttl > 0 && ttl <= 10);

    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn rdb_does_not_restore_expired_keys() {
    let storage = Storage::default();
    storage.set("k".to_string(), b"v".to_vec()).expect("set k");
    let _ = storage.expire_millis("k", 1);

    std::thread::sleep(std::time::Duration::from_millis(5));

    let path = temp_path("expired");
    storage.save_rdb(&path).unwrap();

    let restored = Storage::default();
    restored.load_rdb(&path).unwrap();

    assert_eq!(restored.get("k"), None);

    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn rdb_load_from_missing_file_is_noop() {
    let storage = Storage::default();
    let path = temp_path("missing");
    assert!(!path.exists());

    storage.load_rdb(&path).unwrap();

    assert_eq!(storage.keys("*").len(), 0);
}

#[tokio::test]
async fn rdb_strict_mode_rejects_truncated_file() {
    let storage = Storage::default();
    storage.set("key1".to_string(), b"value1".to_vec()).unwrap();
    storage.set("key2".to_string(), b"value2".to_vec()).unwrap();

    let path = temp_path("truncated_strict");
    storage.save_rdb(&path).unwrap();

    // 截断文件（保留 header 但截断数据部分）
    let file_len = std::fs::metadata(&path).unwrap().len();
    let truncate_to = file_len - 10; // 截断最后 10 字节
    let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    file.set_len(truncate_to).unwrap();

    // strict 模式应该拒绝加载，保留原数据
    let restored = Storage::default();
    restored
        .set("existing".to_string(), b"data".to_vec())
        .unwrap();

    let result = restored.load_rdb_with_mode(&path, RdbLoadMode::Strict);
    assert!(result.is_err(), "strict mode should reject truncated file");

    // 原数据应该保留
    assert_eq!(
        restored.get("existing").as_deref(),
        Some(b"data".as_slice())
    );

    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn rdb_tolerant_mode_loads_partial_data() {
    let storage = Storage::default();
    storage.set("key1".to_string(), b"value1".to_vec()).unwrap();
    storage.set("key2".to_string(), b"value2".to_vec()).unwrap();
    storage.set("key3".to_string(), b"value3".to_vec()).unwrap();

    let path = temp_path("truncated_tolerant");
    storage.save_rdb(&path).unwrap();

    // 截断文件
    let file_len = std::fs::metadata(&path).unwrap().len();
    let truncate_to = file_len - 10;
    let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    file.set_len(truncate_to).unwrap();

    // tolerant 模式应该加载部分数据
    let restored = Storage::default();
    let result = restored.load_rdb_with_mode(&path, RdbLoadMode::Tolerant);
    assert!(result.is_ok(), "tolerant mode should accept truncated file");

    // 应该至少加载了一些 key（具体数量取决于截断位置）
    let keys = restored.keys("*");
    assert!(keys.len() < 3, "should have loaded partial data");

    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn rdb_invalid_magic_rejected_in_both_modes() {
    let path = temp_path("invalid_magic");

    // 写入无效的 magic header
    let mut file = std::fs::File::create(&path).unwrap();
    file.write_all(b"INVALID!").unwrap();
    file.write_all(&1u32.to_le_bytes()).unwrap(); // version
    drop(file);

    let storage = Storage::default();
    storage
        .set("existing".to_string(), b"data".to_vec())
        .unwrap();

    // strict 模式应该拒绝
    let result = storage.load_rdb_with_mode(&path, RdbLoadMode::Strict);
    assert!(result.is_err(), "strict mode should reject invalid magic");
    assert_eq!(storage.get("existing").as_deref(), Some(b"data".as_slice()));

    // tolerant 模式也应该拒绝（magic 错误是致命错误）
    let result = storage.load_rdb_with_mode(&path, RdbLoadMode::Tolerant);
    assert!(
        result.is_err(),
        "tolerant mode should also reject invalid magic"
    );
    assert_eq!(storage.get("existing").as_deref(), Some(b"data".as_slice()));

    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn rdb_load_preserves_original_data_on_error() {
    let storage = Storage::default();
    storage
        .set("original".to_string(), b"data".to_vec())
        .unwrap();
    storage.lpush("mylist", &vec!["a".to_string()]).unwrap();

    let path = temp_path("corrupt");

    // 创建一个格式错误的 RDB 文件（有效 header 但数据格式错误）
    let mut file = std::fs::File::create(&path).unwrap();
    file.write_all(b"REDUSTDB").unwrap(); // magic
    file.write_all(&1u32.to_le_bytes()).unwrap(); // version
    file.write_all(&[255u8]).unwrap(); // 无效的类型标记
    drop(file);

    // 加载应该失败，但原数据应该保留
    let result = storage.load_rdb_with_mode(&path, RdbLoadMode::Strict);
    assert!(result.is_err());

    assert_eq!(storage.get("original").as_deref(), Some(b"data".as_slice()));
    assert_eq!(
        storage.lrange("mylist", 0, -1).unwrap(),
        vec!["a".to_string()]
    );

    let _ = std::fs::remove_file(&path);
}
