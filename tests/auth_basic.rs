use std::net::SocketAddr;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

use redust::server::serve;

mod env_guard;
use env_guard::{ENV_LOCK, remove_env, set_env};

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

struct TestClient {
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: tokio::net::tcp::OwnedWriteHalf,
}

impl TestClient {
    async fn connect(addr: SocketAddr) -> Self {
        let stream = TcpStream::connect(addr).await.unwrap();
        let (read_half, write_half) = stream.into_split();
        let reader = BufReader::new(read_half);
        TestClient { reader, writer: write_half }
    }

    async fn send_array(&mut self, parts: &[&str]) {
        let mut buf = String::new();
        buf.push_str(&format!("*{}\r\n", parts.len()));
        for p in parts {
            buf.push_str(&format!("${}\r\n{}\r\n", p.len(), p));
        }
        self.writer.write_all(buf.as_bytes()).await.unwrap();
    }

    async fn read_line(&mut self) -> String {
        let mut line = String::new();
        self.reader.read_line(&mut line).await.unwrap();
        line
    }
}

#[tokio::test]
async fn auth_not_enabled_behaviour() {
    let _lock = ENV_LOCK.lock().unwrap();

    // 确保未配置密码
    let _auth_guard = remove_env("REDUST_AUTH_PASSWORD");

    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // AUTH 在未启用时应返回 ERR AUTH not enabled
    client.send_array(&["AUTH", "secret"]).await;
    let line = client.read_line().await;
    assert!(line.starts_with("-ERR AUTH not enabled"));

    // 其他命令不受影响
    client.send_array(&["SET", "k", "v"]).await;
    let line = client.read_line().await;
    assert_eq!(line, "+OK\r\n");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn auth_required_for_write_when_enabled() {
    let _lock = ENV_LOCK.lock().unwrap();
    let _auth_guard = set_env("REDUST_AUTH_PASSWORD", "secret");

    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // 未认证时，PING 仍然允许
    client.send_array(&["PING"]).await;
    let line = client.read_line().await;
    assert_eq!(line, "+PONG\r\n");

    // 未认证时，写命令被拦截
    client.send_array(&["SET", "k", "v"]).await;
    let line = client.read_line().await;
    assert!(line.starts_with("-NOAUTH Authentication required"));

    // 错误密码
    client.send_array(&["AUTH", "wrong"]).await;
    let line = client.read_line().await;
    assert!(line.starts_with("-WRONGPASS"));

    // 正确密码
    client.send_array(&["AUTH", "secret"]).await;
    let line = client.read_line().await;
    assert_eq!(line, "+OK\r\n");

    // 认证后写命令成功
    client.send_array(&["SET", "k", "v"]).await;
    let line = client.read_line().await;
    assert_eq!(line, "+OK\r\n");

    // 认证后其他命令正常
    client.send_array(&["GET", "k"]).await;
    let header = client.read_line().await;
    let value = client.read_line().await;
    assert_eq!(header, "$1\r\n");
    assert_eq!(value, "v\r\n");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn auth_is_per_connection() {
    let _lock = ENV_LOCK.lock().unwrap();
    let _auth_guard = set_env("REDUST_AUTH_PASSWORD", "secret");

    let (addr, shutdown, handle) = spawn_server().await;

    // client1: 认证成功
    let mut c1 = TestClient::connect(addr).await;
    c1.send_array(&["AUTH", "secret"]).await;
    let line = c1.read_line().await;
    assert_eq!(line, "+OK\r\n");

    // client2: 未认证直接写，应被拦截
    let mut c2 = TestClient::connect(addr).await;
    c2.send_array(&["SET", "k2", "v2"]).await;
    let line = c2.read_line().await;
    assert!(line.starts_with("-NOAUTH Authentication required"));

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}
