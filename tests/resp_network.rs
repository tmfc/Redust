use std::net::SocketAddr;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
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

async fn connect(addr: SocketAddr) -> TcpStream {
    TcpStream::connect(addr).await.expect("connect")
}

async fn write_in_chunks(stream: &mut TcpStream, data: &[u8], chunk_size: usize) {
    let mut i = 0;
    while i < data.len() {
        let end = (i + chunk_size).min(data.len());
        stream.write_all(&data[i..end]).await.expect("write chunk");
        i = end;
        tokio::task::yield_now().await;
    }
}

async fn read_exact_prefix(stream: &mut TcpStream, prefix: &[u8]) {
    let mut buf = vec![0u8; prefix.len()];
    stream.read_exact(&mut buf).await.expect("read_exact prefix");
    assert_eq!(buf, prefix);
}

async fn read_line(stream: &mut TcpStream) -> Vec<u8> {
    let mut out = Vec::new();
    let mut b = [0u8; 1];
    loop {
        stream.read_exact(&mut b).await.expect("read byte");
        out.push(b[0]);
        if out.len() >= 2 && out[out.len() - 2..] == *b"\r\n" {
            break;
        }
    }
    out
}

#[tokio::test]
async fn resp_multiple_requests_in_single_write_roundtrip() {
    let (addr, shutdown, handle) = spawn_server().await;

    let mut stream = connect(addr).await;

    let req = b"*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n*2\r\n$4\r\nINCR\r\n$3\r\ncnt\r\n";
    stream.write_all(req).await.expect("write_all");

    let pong = read_line(&mut stream).await;
    assert_eq!(pong, b"+PONG\r\n");

    read_exact_prefix(&mut stream, b"$2\r\n").await;
    let mut payload = [0u8; 2];
    stream.read_exact(&mut payload).await.expect("read bulk payload");
    assert_eq!(&payload, b"hi");
    read_exact_prefix(&mut stream, b"\r\n").await;

    let incr = read_line(&mut stream).await;
    assert_eq!(incr, b":1\r\n");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn resp_fragmented_and_coalesced_requests_roundtrip() {
    let (addr, shutdown, handle) = spawn_server().await;

    let mut stream = connect(addr).await;

    // Two requests coalesced in one buffer:
    //   PING
    //   ECHO hi
    let req = b"*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n";
    write_in_chunks(&mut stream, req, 1).await;

    // Expect +PONG\r\n
    let pong = read_line(&mut stream).await;
    assert_eq!(pong, b"+PONG\r\n");

    // Expect bulk string: $2\r\nhi\r\n
    read_exact_prefix(&mut stream, b"$2\r\n").await;
    let mut payload = [0u8; 2];
    stream.read_exact(&mut payload).await.expect("read bulk payload");
    assert_eq!(&payload, b"hi");
    read_exact_prefix(&mut stream, b"\r\n").await;

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn resp_large_bulk_string_fragmented() {
    let (addr, shutdown, handle) = spawn_server().await;

    let mut stream = connect(addr).await;

    let payload = vec![b'a'; 64 * 1024];

    // ECHO <payload>
    let header = format!("*2\r\n$4\r\nECHO\r\n${}\r\n", payload.len());
    let mut req = Vec::with_capacity(header.len() + payload.len() + 2);
    req.extend_from_slice(header.as_bytes());
    req.extend_from_slice(&payload);
    req.extend_from_slice(b"\r\n");

    write_in_chunks(&mut stream, &req, 7).await;

    // Expect the same bulk string back.
    let expected_header = format!("${}\r\n", payload.len());
    read_exact_prefix(&mut stream, expected_header.as_bytes()).await;

    let mut out = vec![0u8; payload.len()];
    stream.read_exact(&mut out).await.expect("read echoed payload");
    assert_eq!(out, payload);

    read_exact_prefix(&mut stream, b"\r\n").await;

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}
