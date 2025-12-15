use std::net::SocketAddr;

use tokio::net::TcpListener;
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
async fn redis_rs_error_and_null_bulk_compat() {
    use redis::AsyncCommands;

    let (addr, shutdown, handle) = spawn_server().await;

    let url = format!("redis://{}", addr);
    let client = redis::Client::open(url).expect("create redis client");
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("get async multiplexed connection");

    let missing_key = format!("missing:{}", addr.port());
    let v: Option<String> = conn.get(&missing_key).await.expect("GET missing key");
    assert_eq!(v, None);

    let missing_field: Option<String> = redis::cmd("HGET")
        .arg("h:missing")
        .arg("field")
        .query_async(&mut conn)
        .await
        .expect("HGET missing field");
    assert_eq!(missing_field, None);

    let key = format!("wrongtype:{}", addr.port());
    let _: () = conn.set(&key, "v").await.expect("SET key");
    let err = redis::cmd("LPUSH")
        .arg(&key)
        .arg("a")
        .query_async::<_, redis::Value>(&mut conn)
        .await
        .expect_err("LPUSH wrongtype should error");
    assert!(err.to_string().contains("WRONGTYPE"));

    let key2 = format!("increrr:{}", addr.port());
    let _: () = conn.set(&key2, "abc").await.expect("SET key2");
    let err2 = redis::cmd("INCR")
        .arg(&key2)
        .query_async::<_, i64>(&mut conn)
        .await
        .expect_err("INCR non-int should error");
    assert!(err2.to_string().contains("value is not an integer") || err2.to_string().contains("ERR"));

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn redis_rs_basic_commands_roundtrip() {
    use redis::AsyncCommands;

    let (addr, shutdown, handle) = spawn_server().await;

    // 使用 redis-rs 通过 redis:// URL 连接本地 Redust server
    let url = format!("redis://{}", addr);
    let client = redis::Client::open(url).expect("create redis client");
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("get async multiplexed connection");

    // PING
    let pong: String = redis::cmd("PING")
        .query_async(&mut conn)
        .await
        .expect("PING via redis-rs");
    assert_eq!(pong, "PONG");

    // SET / GET
    let _: () = conn.set("foo", "bar").await.expect("SET via redis-rs");
    let v: String = conn.get("foo").await.expect("GET via redis-rs");
    assert_eq!(v, "bar");

    // INCR / GET 数值
    let _: () = conn.set("cnt", 0_i64).await.expect("SET cnt");
    let v1: i64 = conn.incr("cnt", 1_i64).await.expect("INCR cnt");
    assert_eq!(v1, 1);
    let v2: i64 = conn.incr("cnt", 5_i64).await.expect("INCR cnt again");
    assert_eq!(v2, 6);

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn redis_rs_binary_safe_value_roundtrip() {
    use redis::AsyncCommands;

    let (addr, shutdown, handle) = spawn_server().await;

    let url = format!("redis://{}", addr);
    let client = redis::Client::open(url).expect("create redis client");
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("get async multiplexed connection");

    let key = format!("bin:{}", addr.port());
    let bytes: Vec<u8> = vec![0, 1, 2, 0, 255, 10, 13];
    let _: () = conn.set(&key, bytes.clone()).await.expect("SET bytes");
    let got: Vec<u8> = conn.get(&key).await.expect("GET bytes");
    assert_eq!(got, bytes);

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn redis_rs_pipeline_roundtrip() {
    let (addr, shutdown, handle) = spawn_server().await;

    let url = format!("redis://{}", addr);
    let client = redis::Client::open(url).expect("create redis client");
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("get async multiplexed connection");

    let mut pipe = redis::pipe();
    pipe.cmd("PING")
        .cmd("SET")
        .arg("pfoo")
        .arg("pbar")
        .ignore()
        .cmd("INCR")
        .arg("pcnt")
        .cmd("GET")
        .arg("pfoo");

    let (pong, cnt, val): (String, i64, String) = pipe
        .query_async(&mut conn)
        .await
        .expect("pipeline via redis-rs");

    assert_eq!(pong, "PONG");
    assert_eq!(cnt, 1);
    assert_eq!(val, "pbar");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn redis_rs_multi_exec_and_discard_roundtrip() {
    use redis::Value;

    let (addr, shutdown, handle) = spawn_server().await;

    let url = format!("redis://{}", addr);
    let client = redis::Client::open(url).expect("create redis client");
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("get async multiplexed connection");

    // MULTI -> OK
    let ok: String = redis::cmd("MULTI")
        .query_async(&mut conn)
        .await
        .expect("MULTI via redis-rs");
    assert_eq!(ok, "OK");

    // Queue a few commands -> QUEUED
    let queued1: String = redis::cmd("SET")
        .arg("tx:foo")
        .arg("bar")
        .query_async(&mut conn)
        .await
        .expect("queue SET");
    assert_eq!(queued1, "QUEUED");

    let queued2: String = redis::cmd("INCR")
        .arg("tx:cnt")
        .query_async(&mut conn)
        .await
        .expect("queue INCR");
    assert_eq!(queued2, "QUEUED");

    let queued3: String = redis::cmd("GET")
        .arg("tx:foo")
        .query_async(&mut conn)
        .await
        .expect("queue GET");
    assert_eq!(queued3, "QUEUED");

    // EXEC -> array of results: [OK, 1, "bar"]
    let exec_res: Value = redis::cmd("EXEC")
        .query_async(&mut conn)
        .await
        .expect("EXEC via redis-rs");

    match exec_res {
        Value::Bulk(items) => {
            assert_eq!(items.len(), 3);
            assert!(matches!(items[0], Value::Okay));
            assert_eq!(items[1], Value::Int(1));
            match &items[2] {
                Value::Data(bs) => assert_eq!(bs.as_slice(), b"bar"),
                other => panic!("unexpected EXEC result[2]: {:?}", other),
            }
        }
        other => panic!("unexpected EXEC response: {:?}", other),
    }

    // DISCARD should abort queued commands and allow normal commands again
    let ok2: String = redis::cmd("MULTI")
        .query_async(&mut conn)
        .await
        .expect("MULTI #2");
    assert_eq!(ok2, "OK");

    let queued4: String = redis::cmd("SET")
        .arg("tx:discard")
        .arg("1")
        .query_async(&mut conn)
        .await
        .expect("queue SET #2");
    assert_eq!(queued4, "QUEUED");

    let discard_ok: String = redis::cmd("DISCARD")
        .query_async(&mut conn)
        .await
        .expect("DISCARD via redis-rs");
    assert_eq!(discard_ok, "OK");

    // Ensure tx:discard was not set
    let v: Option<String> = redis::cmd("GET")
        .arg("tx:discard")
        .query_async(&mut conn)
        .await
        .expect("GET after DISCARD");
    assert_eq!(v, None);

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn redis_rs_watch_unwatch_roundtrip() {
    use redis::Value;

    let (addr, shutdown, handle) = spawn_server().await;

    let url = format!("redis://{}", addr);
    let client = redis::Client::open(url).expect("create redis client");

    let mut conn1 = client
        .get_multiplexed_async_connection()
        .await
        .expect("get conn1");
    let mut conn2 = client
        .get_multiplexed_async_connection()
        .await
        .expect("get conn2");

    let watched_key = format!("watch:key:{}", addr.port());

    let ok: String = redis::cmd("WATCH")
        .arg(&watched_key)
        .query_async(&mut conn1)
        .await
        .expect("WATCH");
    assert_eq!(ok, "OK");

    let ok: String = redis::cmd("MULTI")
        .query_async(&mut conn1)
        .await
        .expect("MULTI");
    assert_eq!(ok, "OK");

    let queued: String = redis::cmd("SET")
        .arg(&watched_key)
        .arg("from_tx")
        .query_async(&mut conn1)
        .await
        .expect("queue SET");
    assert_eq!(queued, "QUEUED");

    let _: () = redis::cmd("SET")
        .arg(&watched_key)
        .arg("from_other")
        .query_async(&mut conn2)
        .await
        .expect("concurrent SET");

    let exec_res: Value = redis::cmd("EXEC")
        .query_async(&mut conn1)
        .await
        .expect("EXEC after concurrent change");
    assert!(matches!(exec_res, Value::Nil));

    let v: String = redis::cmd("GET")
        .arg(&watched_key)
        .query_async(&mut conn1)
        .await
        .expect("GET watched_key");
    assert_eq!(v, "from_other");

    let watched_key2 = format!("watch:key2:{}", addr.port());

    let ok: String = redis::cmd("WATCH")
        .arg(&watched_key2)
        .query_async(&mut conn1)
        .await
        .expect("WATCH #2");
    assert_eq!(ok, "OK");

    let ok: String = redis::cmd("UNWATCH")
        .query_async(&mut conn1)
        .await
        .expect("UNWATCH");
    assert_eq!(ok, "OK");

    let ok: String = redis::cmd("MULTI")
        .query_async(&mut conn1)
        .await
        .expect("MULTI #2");
    assert_eq!(ok, "OK");

    let queued: String = redis::cmd("SET")
        .arg(&watched_key2)
        .arg("from_tx")
        .query_async(&mut conn1)
        .await
        .expect("queue SET #2");
    assert_eq!(queued, "QUEUED");

    let _: () = redis::cmd("SET")
        .arg(&watched_key2)
        .arg("from_other")
        .query_async(&mut conn2)
        .await
        .expect("concurrent SET #2");

    let exec_res: Value = redis::cmd("EXEC")
        .query_async(&mut conn1)
        .await
        .expect("EXEC after UNWATCH");

    match exec_res {
        Value::Bulk(items) => {
            assert_eq!(items.len(), 1);
            assert!(matches!(items[0], Value::Okay));
        }
        other => panic!("unexpected EXEC response after UNWATCH: {:?}", other),
    }

    let v: String = redis::cmd("GET")
        .arg(&watched_key2)
        .query_async(&mut conn1)
        .await
        .expect("GET watched_key2");
    assert_eq!(v, "from_tx");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn redis_rs_pubsub_roundtrip() {
    use futures_util::StreamExt;

    let (addr, shutdown, handle) = spawn_server().await;

    let url = format!("redis://{}", addr);
    let client = redis::Client::open(url).expect("create redis client");

    let mut pubsub = client
        .get_async_pubsub()
        .await
        .expect("get async pubsub");

    let mut publisher = client
        .get_multiplexed_async_connection()
        .await
        .expect("get publisher connection");

    let ch = format!("rs:ch:{}", addr.port());
    let pat = format!("rs:pat:{}*", addr.port());
    let ch2 = format!("rs:pat:{}-x", addr.port());

    pubsub.subscribe(&ch).await.expect("subscribe");
    pubsub.psubscribe(&pat).await.expect("psubscribe");

    {
        let mut stream = pubsub.on_message();

        let receivers: i64 = redis::cmd("PUBLISH")
            .arg(&ch)
            .arg("hello")
            .query_async(&mut publisher)
            .await
            .expect("PUBLISH ch");
        assert!(receivers >= 1);

        let msg = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
            .await
            .expect("wait msg")
            .expect("msg present");
        assert_eq!(msg.get_channel_name(), ch);
        let payload: String = msg.get_payload().expect("payload");
        assert_eq!(payload, "hello");

        let receivers2: i64 = redis::cmd("PUBLISH")
            .arg(&ch2)
            .arg("world")
            .query_async(&mut publisher)
            .await
            .expect("PUBLISH ch2");
        assert!(receivers2 >= 1);

        let msg2 = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
            .await
            .expect("wait pmessage")
            .expect("pmessage present");
        assert!(msg2.from_pattern());
        let pattern: Option<String> = msg2.get_pattern().expect("pattern");
        assert_eq!(pattern.as_deref(), Some(pat.as_str()));
        assert_eq!(msg2.get_channel_name(), ch2);
        let payload2: String = msg2.get_payload().expect("payload2");
        assert_eq!(payload2, "world");
    }

    pubsub.unsubscribe(&ch).await.expect("unsubscribe");
    pubsub.punsubscribe(&pat).await.expect("punsubscribe");

    let mut stream = pubsub.on_message();

    let receivers3: i64 = redis::cmd("PUBLISH")
        .arg(&ch)
        .arg("after")
        .query_async(&mut publisher)
        .await
        .expect("PUBLISH after unsub");
    assert_eq!(receivers3, 0);

    let no_msg = tokio::time::timeout(std::time::Duration::from_millis(100), stream.next()).await;
    assert!(no_msg.is_err());

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn redis_rs_reconnect_roundtrip() {
    use redis::AsyncCommands;

    let (addr, shutdown, handle) = spawn_server().await;

    let url = format!("redis://{}", addr);
    let client = redis::Client::open(url).expect("create redis client");

    let key = format!("reconnect:{}", addr.port());

    {
        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .expect("get async multiplexed connection #1");
        let _: () = conn.set(&key, "v1").await.expect("SET v1");
        let v: String = conn.get(&key).await.expect("GET v1");
        assert_eq!(v, "v1");
        drop(conn);
    }

    let mut conn2 = client
        .get_multiplexed_async_connection()
        .await
        .expect("get async multiplexed connection #2");
    let v: String = conn2.get(&key).await.expect("GET after reconnect");
    assert_eq!(v, "v1");

    let _: () = conn2.set(&key, "v2").await.expect("SET v2");
    let v2: String = conn2.get(&key).await.expect("GET v2");
    assert_eq!(v2, "v2");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn redis_rs_list_commands_roundtrip() {
    use redis::AsyncCommands;

    let (addr, shutdown, handle) = spawn_server().await;

    let url = format!("redis://{}", addr);
    let client = redis::Client::open(url).expect("create redis client");
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("get async multiplexed connection");

    let list_key = format!("mylist:{}", addr.port());

    // RPUSH mylist a b c
    let len: i64 = conn
        .rpush(&list_key, vec!["a", "b", "c"])
        .await
        .expect("RPUSH via redis-rs");
    assert_eq!(len, 3);

    // LRANGE mylist 0 -1 -> [a, b, c]
    let items: Vec<String> = conn
        .lrange(&list_key, 0, -1)
        .await
        .expect("LRANGE via redis-rs");
    assert_eq!(items, vec!["a", "b", "c"]);

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn redis_rs_hash_commands_roundtrip() {
    use redis::AsyncCommands;

    let (addr, shutdown, handle) = spawn_server().await;

    let url = format!("redis://{}", addr);
    let client = redis::Client::open(url).expect("create redis client");
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("get async multiplexed connection");

    // HSET myhash field value
    let added: i64 = conn
        .hset("myhash", "field", "value")
        .await
        .expect("HSET via redis-rs");
    assert_eq!(added, 1);

    // HGET myhash field -> value
    let v: String = conn
        .hget("myhash", "field")
        .await
        .expect("HGET via redis-rs");
    assert_eq!(v, "value");

    // HGETALL myhash -> map 中包含 field/value
    let map: std::collections::HashMap<String, String> =
        conn.hgetall("myhash").await.expect("HGETALL via redis-rs");
    assert_eq!(map.get("field").map(String::as_str), Some("value"));

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}
