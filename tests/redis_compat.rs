use std::net::SocketAddr;

use tokio::net::TcpListener;
use tokio::sync::oneshot;

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
async fn redis_rs_basic_commands_roundtrip() {
    use redis::AsyncCommands;

    let (addr, shutdown, handle) = spawn_server().await;

    // 使用 redis-rs 通过 redis:// URL 连接本地 Redust server
    let url = format!("redis://{}", addr);
    let client = redis::Client::open(url).expect("create redis client");
    let mut conn = client
        .get_async_connection()
        .await
        .expect("get async connection");

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
async fn redis_rs_list_commands_roundtrip() {
    use redis::AsyncCommands;

    let (addr, shutdown, handle) = spawn_server().await;

    let url = format!("redis://{}", addr);
    let client = redis::Client::open(url).expect("create redis client");
    let mut conn = client
        .get_async_connection()
        .await
        .expect("get async connection");

    // RPUSH mylist a b c
    let len: i64 = conn
        .rpush("mylist", vec!["a", "b", "c"])
        .await
        .expect("RPUSH via redis-rs");
    assert_eq!(len, 3);

    // LRANGE mylist 0 -1 -> [a, b, c]
    let items: Vec<String> = conn
        .lrange("mylist", 0, -1)
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
        .get_async_connection()
        .await
        .expect("get async connection");

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
    let map: std::collections::HashMap<String, String> = conn
        .hgetall("myhash")
        .await
        .expect("HGETALL via redis-rs");
    assert_eq!(map.get("field").map(String::as_str), Some("value"));

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}
