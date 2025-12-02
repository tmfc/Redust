use std::net::SocketAddr;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
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

struct RespClient {
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: tokio::net::tcp::OwnedWriteHalf,
}

impl RespClient {
    async fn connect(addr: SocketAddr) -> Self {
        let stream = TcpStream::connect(addr).await.unwrap();
        let (rh, wh) = stream.into_split();
        RespClient {
            reader: BufReader::new(rh),
            writer: wh,
        }
    }

    async fn send_array(&mut self, parts: &[&[u8]]) {
        let mut buf = Vec::new();
        buf.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
        for p in parts {
            buf.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
            buf.extend_from_slice(p);
            buf.extend_from_slice(b"\r\n");
        }
        self.writer.write_all(&buf).await.unwrap();
    }

    async fn read_integer(&mut self) -> i64 {
        let mut line = String::new();
        self.reader.read_line(&mut line).await.unwrap();
        assert!(line.starts_with(':'));
        line[1..line.len() - 2].parse().unwrap()
    }

    async fn read_array(&mut self) -> Vec<Vec<u8>> {
        let mut header = String::new();
        self.reader.read_line(&mut header).await.unwrap();
        assert!(header.starts_with('*'));
        let len: usize = header[1..header.len() - 2].parse().unwrap();
        let mut out = Vec::with_capacity(len);
        for _ in 0..len {
            let mut line = String::new();
            self.reader.read_line(&mut line).await.unwrap();
            if line.starts_with('$') {
                let blen: usize = line[1..line.len() - 2].parse().unwrap();
                let mut buf = vec![0u8; blen];
                self.reader.read_exact(&mut buf).await.unwrap();
                let mut crlf = [0u8; 2];
                self.reader.read_exact(&mut crlf).await.unwrap();
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
}

#[tokio::test]
async fn subscribe_and_receive_publish() {
    let (addr, shutdown, handle) = spawn_server().await;

    let mut sub = RespClient::connect(addr).await;
    let mut pub_client = RespClient::connect(addr).await;

    sub.send_array(&[b"SUBSCRIBE", b"news"]).await;
    let subscribe_ack = sub.read_array().await;
    assert_eq!(subscribe_ack[0], b"subscribe");
    assert_eq!(subscribe_ack[1], b"news");
    assert_eq!(subscribe_ack[2], b"1");

    pub_client
        .send_array(&[b"PUBLISH", b"news", b"hello world"])
        .await;
    let receivers = pub_client.read_integer().await;
    assert_eq!(receivers, 1);

    let message = sub.read_array().await;
    assert_eq!(message[0], b"message");
    assert_eq!(message[1], b"news");
    assert_eq!(message[2], b"hello world");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn unsubscribe_leaves_sub_mode() {
    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = RespClient::connect(addr).await;

    client.send_array(&[b"SUBSCRIBE", b"chan"]).await;
    let _ = client.read_array().await;

    client.send_array(&[b"UNSUBSCRIBE", b"chan"]).await;
    let unsub = client.read_array().await;
    assert_eq!(unsub[0], b"unsubscribe");
    assert_eq!(unsub[1], b"chan");
    assert_eq!(unsub[2], b"0");

    // After unsubscribing all channels, normal commands should work again
    client.send_array(&[b"PING"]).await;
    let mut line = String::new();
    client.reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "+PONG\r\n");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn subscribe_mode_restricts_commands_and_supports_ping() {
    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = RespClient::connect(addr).await;

    client.send_array(&[b"SUBSCRIBE", b"x"]).await;
    let _ = client.read_array().await;

    // PING in subscribed mode -> array pong
    client.send_array(&[b"PING"]).await;
    let pong = client.read_array().await;
    assert_eq!(pong[0], b"pong");
    assert_eq!(pong[1], b"");

    // Non-allowed command should error
    client.send_array(&[b"SET", b"foo", b"bar"]).await;
    let mut err = String::new();
    client.reader.read_line(&mut err).await.unwrap();
    assert!(err.starts_with("-ERR only (P)SUBSCRIBE"));

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}
