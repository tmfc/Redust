use std::path::PathBuf;

use redust::storage::Storage;

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
