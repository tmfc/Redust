use std::net::SocketAddr;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

use redust::server::serve;
mod env_guard;
use env_guard::{set_env, remove_env, ENV_LOCK};

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
async fn auth_not_enabled_but_command_sent() {
    let _lock = ENV_LOCK.lock().unwrap();
    let _guard = remove_env("REDUST_AUTH_PASSWORD");

    let (addr, shutdown, handle) = spawn_server().await;

    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // AUTH 在未启用密码时应返回错误
    write_half
        .write_all(b"*2\r
$4\r
AUTH\r
$3\r
foo\r
")
        .await
        .unwrap();
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "-ERR AUTH not enabled\r
");

    // 仍然可以正常使用 PING
    write_half
        .write_all(b"*1\r
$4\r
PING\r
")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "+PONG\r
");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn auth_required_blocks_commands_until_authenticated() {
    let _lock = ENV_LOCK.lock().unwrap();
    let _guard = set_env("REDUST_AUTH_PASSWORD", "secret");

    let (addr, shutdown, handle) = spawn_server().await;

    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // 未认证情况下，PING 仍然允许
    write_half
        .write_all(b"*1\r
$4\r
PING\r
")
        .await
        .unwrap();
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "+PONG\r
");

    // 未认证情况下，SET 应被拒绝
    write_half
        .write_all(b"*3\r
$3\r
SET\r
$3\r
foo\r
$3\r
bar\r
")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "-NOAUTH Authentication required\r
");

    // 使用错误密码 AUTH -> WRONGPASS
    write_half
        .write_all(b"*2\r
$4\r
AUTH\r
$5\r
wrong\r
")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "-WRONGPASS invalid username-password pair or user is disabled\r
");

    // 使用正确密码 AUTH -> OK
    write_half
        .write_all(b"*2\r
$4\r
AUTH\r
$6\r
secret\r
")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "+OK\r
");

    // 认证之后，SET/GET 应该可以正常工作
    write_half
        .write_all(b"*3\r
$3\r
SET\r
$3\r
foo\r
$3\r
bar\r
")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "+OK\r
");

    write_half
        .write_all(b"*2\r
$3\r
GET\r
$3\r
foo\r
")
        .await
        .unwrap();

    let mut bulk_header = String::new();
    let mut bulk_value = String::new();
    reader.read_line(&mut bulk_header).await.unwrap();
    reader.read_line(&mut bulk_value).await.unwrap();
    assert_eq!(bulk_header, "$3\r
");
    assert_eq!(bulk_value, "bar\r
");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}
