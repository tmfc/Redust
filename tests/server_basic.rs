use std::net::SocketAddr;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::time::{Duration, Instant};

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
async fn responds_to_basic_commands() {
    let (addr, shutdown, handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    write_half.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
    let mut pong = String::new();
    reader.read_line(&mut pong).await.unwrap();
    assert_eq!(pong, "+PONG\r\n");

    write_half
        .write_all(b"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n")
        .await
        .unwrap();
    let mut bulk_header = String::new();
    reader.read_line(&mut bulk_header).await.unwrap();
    assert_eq!(bulk_header, "$5\r\n");
    let mut bulk_value = String::new();
    reader.read_line(&mut bulk_value).await.unwrap();
    assert_eq!(bulk_value, "hello\r\n");

    // SET key
    write_half
        .write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
        .await
        .unwrap();
    let mut ok = String::new();
    reader.read_line(&mut ok).await.unwrap();
    assert_eq!(ok, "+OK\r\n");

    // GET key
    write_half
        .write_all(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")
        .await
        .unwrap();
    let mut get_header = String::new();
    reader.read_line(&mut get_header).await.unwrap();
    assert_eq!(get_header, "$3\r\n");
    let mut get_value = String::new();
    reader.read_line(&mut get_value).await.unwrap();
    assert_eq!(get_value, "bar\r\n");

    // EXISTS key
    write_half
        .write_all(b"*2\r\n$6\r\nEXISTS\r\n$3\r\nfoo\r\n")
        .await
        .unwrap();
    let mut exists_resp = String::new();
    reader.read_line(&mut exists_resp).await.unwrap();
    assert_eq!(exists_resp, ":1\r\n");

    // DEL key
    write_half
        .write_all(b"*2\r\n$3\r\nDEL\r\n$3\r\nfoo\r\n")
        .await
        .unwrap();
    let mut del_resp = String::new();
    reader.read_line(&mut del_resp).await.unwrap();
    assert_eq!(del_resp, ":1\r\n");

    // GET missing key -> nil bulk string
    write_half
        .write_all(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")
        .await
        .unwrap();
    let mut nil_resp = String::new();
    reader.read_line(&mut nil_resp).await.unwrap();
    assert_eq!(nil_resp, "$-1\r\n");

    // INCR on new key -> 1
    write_half
        .write_all(b"*2\r\n$4\r\nINCR\r\n$3\r\ncnt\r\n")
        .await
        .unwrap();
    let mut incr_resp = String::new();
    reader.read_line(&mut incr_resp).await.unwrap();
    assert_eq!(incr_resp, ":1\r\n");

    // INCR again -> 2
    write_half
        .write_all(b"*2\r\n$4\r\nINCR\r\n$3\r\ncnt\r\n")
        .await
        .unwrap();
    let mut incr_resp2 = String::new();
    reader.read_line(&mut incr_resp2).await.unwrap();
    assert_eq!(incr_resp2, ":2\r\n");

    // DECR -> 1
    write_half
        .write_all(b"*2\r\n$4\r\nDECR\r\n$3\r\ncnt\r\n")
        .await
        .unwrap();
    let mut decr_resp = String::new();
    reader.read_line(&mut decr_resp).await.unwrap();
    assert_eq!(decr_resp, ":1\r\n");

    // TYPE existing key -> string
    write_half
        .write_all(b"*2\r\n$4\r\nTYPE\r\n$3\r\ncnt\r\n")
        .await
        .unwrap();
    let mut type_resp = String::new();
    reader.read_line(&mut type_resp).await.unwrap();
    assert_eq!(type_resp, "+string\r\n");

    // TYPE missing key -> none
    write_half
        .write_all(b"*2\r\n$4\r\nTYPE\r\n$7\r\nmissing\r\n")
        .await
        .unwrap();
    let mut type_none = String::new();
    reader.read_line(&mut type_none).await.unwrap();
    assert_eq!(type_none, "+none\r\n");

    // KEYS exact match pattern
    write_half
        .write_all(b"*2\r\n$4\r\nKEYS\r\n$3\r\ncnt\r\n")
        .await
        .unwrap();
    let mut array_header = String::new();
    reader.read_line(&mut array_header).await.unwrap();
    assert_eq!(array_header, "*1\r\n");
    let mut bulk_header2 = String::new();
    reader.read_line(&mut bulk_header2).await.unwrap();
    assert_eq!(bulk_header2, "$3\r\n");
    let mut key_value = String::new();
    reader.read_line(&mut key_value).await.unwrap();
    assert_eq!(key_value, "cnt\r\n");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn multiple_clients_share_storage() {
    let (addr, shutdown, handle) = spawn_server().await;

    // client 1: SET foo bar
    let stream1 = TcpStream::connect(addr).await.unwrap();
    let (_r1, mut w1) = stream1.into_split();
    w1.write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
        .await
        .unwrap();

    // client 2: GET foo, should see bar
    let stream2 = TcpStream::connect(addr).await.unwrap();
    let (r2, mut w2) = stream2.into_split();
    let mut reader2 = BufReader::new(r2);
    w2.write_all(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")
        .await
        .unwrap();

    let mut header = String::new();
    let mut value = String::new();
    reader2.read_line(&mut header).await.unwrap();
    reader2.read_line(&mut value).await.unwrap();
    assert_eq!(header, "$3\r\n");
    assert_eq!(value, "bar\r\n");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn continues_after_unknown_command() {
    let (addr, shutdown, handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    write_half
        .write_all(b"*2\r\n$7\r\nCOMMAND\r\n$4\r\nDOCS\r\n")
        .await
        .unwrap();

    let mut error_line = String::new();
    reader.read_line(&mut error_line).await.unwrap();
    assert_eq!(error_line, "-ERR unknown command 'COMMAND DOCS'\r\n");

    write_half
        .write_all(b"*1\r\n$4\r\nPING\r\n")
        .await
        .unwrap();
    let mut pong = String::new();
    reader.read_line(&mut pong).await.unwrap();
    assert_eq!(pong, "+PONG\r\n");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn handles_quit_and_connection_close() {
    let (addr, shutdown, handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    write_half
        .write_all(b"*1\r\n$4\r\nQUIT\r\n")
        .await
        .unwrap();
    let mut ok = String::new();
    reader.read_line(&mut ok).await.unwrap();
    assert_eq!(ok, "+OK\r\n");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn performance_ping_round_trips() {
    let (addr, shutdown, handle) = spawn_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    let iterations = 200;
    let start = Instant::now();
    for _ in 0..iterations {
        write_half
            .write_all(b"*1\r\n$4\r\nPING\r\n")
            .await
            .unwrap();
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        assert_eq!(line, "+PONG\r\n");
        line.clear();
    }
    let elapsed = start.elapsed();
    assert!(elapsed < Duration::from_secs(2), "Ping loop took too long: {:?}", elapsed);

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}
