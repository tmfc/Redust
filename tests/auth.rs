use std::net::SocketAddr;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

use redust::server::serve;
mod env_guard;
use env_guard::{remove_env, set_env, ENV_LOCK};

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

async fn read_mixed_array(reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>) -> Vec<Vec<u8>> {
    let mut header = String::new();
    reader.read_line(&mut header).await.unwrap();
    assert!(header.starts_with('*'));
    let len: usize = header[1..header.len() - 2].parse().unwrap();
    let mut out = Vec::with_capacity(len);
    for _ in 0..len {
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        if line.starts_with('$') {
            let blen: usize = line[1..line.len() - 2].parse().unwrap();
            let mut buf = vec![0u8; blen];
            reader.read_exact(&mut buf).await.unwrap();
            let mut crlf = [0u8; 2];
            reader.read_exact(&mut crlf).await.unwrap();
            assert_eq!(&crlf, b"\r\n");
            out.push(buf);
        } else if line.starts_with(':') {
            out.push(line[1..line.len() - 2].as_bytes().to_vec());
        } else {
            panic!("unexpected element header: {}", line);
        }
    }
    out
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
        .write_all(
            b"*2\r
$4\r
AUTH\r
$3\r
foo\r
",
        )
        .await
        .unwrap();
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(
        line,
        "-ERR AUTH not enabled\r
"
    );

    // 仍然可以正常使用 PING
    write_half
        .write_all(
            b"*1\r
$4\r
PING\r
",
        )
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(
        line,
        "+PONG\r
"
    );

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
        .write_all(
            b"*1\r
$4\r
PING\r
",
        )
        .await
        .unwrap();
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(
        line,
        "+PONG\r
"
    );

    // 未认证情况下，SET 应被拒绝
    write_half
        .write_all(
            b"*3\r
$3\r
SET\r
$3\r
foo\r
$3\r
bar\r
",
        )
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(
        line,
        "-NOAUTH Authentication required\r
"
    );

    // 使用错误密码 AUTH -> WRONGPASS
    write_half
        .write_all(
            b"*2\r
$4\r
AUTH\r
$5\r
wrong\r
",
        )
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(
        line,
        "-WRONGPASS invalid username-password pair or user is disabled\r
"
    );

    // 使用正确密码 AUTH -> OK
    write_half
        .write_all(
            b"*2\r
$4\r
AUTH\r
$6\r
secret\r
",
        )
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(
        line,
        "+OK\r
"
    );

    // 认证之后，SET/GET 应该可以正常工作
    write_half
        .write_all(
            b"*3\r
$3\r
SET\r
$3\r
foo\r
$3\r
bar\r
",
        )
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(
        line,
        "+OK\r
"
    );

    write_half
        .write_all(
            b"*2\r
$3\r
GET\r
$3\r
foo\r
",
        )
        .await
        .unwrap();

    let mut bulk_header = String::new();
    let mut bulk_value = String::new();
    reader.read_line(&mut bulk_header).await.unwrap();
    reader.read_line(&mut bulk_value).await.unwrap();
    assert_eq!(
        bulk_header,
        "$3\r
"
    );
    assert_eq!(
        bulk_value,
        "bar\r
"
    );

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn pubsub_requires_auth_when_enabled() {
    let _lock = ENV_LOCK.lock().unwrap();
    let _guard = set_env("REDUST_AUTH_PASSWORD", "secret");

    let (addr, shutdown, handle) = spawn_server().await;

    // Subscriber connection
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // SUBSCRIBE should be rejected before AUTH
    write_half
        .write_all(b"*2\r\n$9\r\nSUBSCRIBE\r\n$3\r\nfoo\r\n")
        .await
        .unwrap();
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "-NOAUTH Authentication required\r\n");

    // Authenticate
    write_half
        .write_all(b"*2\r\n$4\r\nAUTH\r\n$6\r\nsecret\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "+OK\r\n");

    // Subscribe again, now allowed
    write_half
        .write_all(b"*2\r\n$9\r\nSUBSCRIBE\r\n$3\r\nfoo\r\n")
        .await
        .unwrap();
    let ack = read_mixed_array(&mut reader).await;
    assert_eq!(
        ack,
        vec![b"subscribe".to_vec(), b"foo".to_vec(), b"1".to_vec()]
    );

    // Publisher connection must auth before publishing
    let pub_stream = TcpStream::connect(addr).await.unwrap();
    let (pub_rh, mut pub_wh) = pub_stream.into_split();
    let mut pub_reader = BufReader::new(pub_rh);

    // Publish without auth is rejected
    pub_wh
        .write_all(b"*3\r\n$7\r\nPUBLISH\r\n$3\r\nfoo\r\n$5\r\nhello\r\n")
        .await
        .unwrap();
    let mut pub_line = String::new();
    pub_reader.read_line(&mut pub_line).await.unwrap();
    assert_eq!(pub_line, "-NOAUTH Authentication required\r\n");

    // Authenticate publisher
    pub_wh
        .write_all(b"*2\r\n$4\r\nAUTH\r\n$6\r\nsecret\r\n")
        .await
        .unwrap();
    pub_line.clear();
    pub_reader.read_line(&mut pub_line).await.unwrap();
    assert_eq!(pub_line, "+OK\r\n");

    // Publish should now reach subscriber
    pub_wh
        .write_all(b"*3\r\n$7\r\nPUBLISH\r\n$3\r\nfoo\r\n$5\r\nhello\r\n")
        .await
        .unwrap();
    // Publisher gets integer reply
    pub_line.clear();
    pub_reader.read_line(&mut pub_line).await.unwrap();
    assert_eq!(pub_line, ":1\r\n");

    // Subscriber receives message
    let message = read_mixed_array(&mut reader).await;
    assert_eq!(
        message,
        vec![b"message".to_vec(), b"foo".to_vec(), b"hello".to_vec()]
    );

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}
