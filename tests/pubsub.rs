use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::time::sleep;

use redust::server::serve;

async fn spawn_server() -> (
    SocketAddr,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<tokio::io::Result<()>>,
) {
    std::env::set_var("REDUST_DISABLE_PERSISTENCE", "1");
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
async fn ssubscribe_and_spublish() {
    let (addr, shutdown, handle) = spawn_server().await;

    let mut sub = RespClient::connect(addr).await;
    let mut pub_client = RespClient::connect(addr).await;

    sub.send_array(&[b"SSUBSCRIBE", b"news"]).await;
    let ack = sub.read_array().await;
    assert_eq!(ack[0], b"ssubscribe");
    assert_eq!(ack[1], b"news");
    assert_eq!(ack[2], b"1");

    pub_client
        .send_array(&[b"SPUBLISH", b"news", b"hello shard"])
        .await;
    let receivers = pub_client.read_integer().await;
    assert_eq!(receivers, 1);

    let message = sub.read_array().await;
    assert_eq!(message[0], b"message");
    assert_eq!(message[1], b"news");
    assert_eq!(message[2], b"hello shard");

    sub.send_array(&[b"SUNSUBSCRIBE"]).await;
    let unsub = sub.read_array().await;
    assert_eq!(unsub[0], b"sunsubscribe");
    assert_eq!(unsub[2], b"0");

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

    // PING with payload echoes payload
    client.send_array(&[b"PING", b"hi"]).await;
    let pong2 = client.read_array().await;
    assert_eq!(pong2[0], b"pong");
    assert_eq!(pong2[1], b"hi");

    // Non-allowed command should error
    client.send_array(&[b"SET", b"foo", b"bar"]).await;
    let mut err = String::new();
    client.reader.read_line(&mut err).await.unwrap();
    assert!(err.starts_with("-ERR only (P)SUBSCRIBE"));

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn psubscribe_receives_pmessage() {
    let (addr, shutdown, handle) = spawn_server().await;

    let mut sub = RespClient::connect(addr).await;
    let mut pub_client = RespClient::connect(addr).await;

    sub.send_array(&[b"PSUBSCRIBE", b"news.*"]).await;
    let ack = sub.read_array().await;
    assert_eq!(ack[0], b"psubscribe");
    assert_eq!(ack[1], b"news.*");
    assert_eq!(ack[2], b"1");

    pub_client
        .send_array(&[b"PUBLISH", b"news.tech", b"hello"])
        .await;
    let receivers = pub_client.read_integer().await;
    assert_eq!(receivers, 1);

    let message = sub.read_array().await;
    assert_eq!(message[0], b"pmessage");
    assert_eq!(message[1], b"news.*");
    assert_eq!(message[2], b"news.tech");
    assert_eq!(message[3], b"hello");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn punsubscribe_allows_commands_again() {
    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = RespClient::connect(addr).await;

    client.send_array(&[b"PSUBSCRIBE", b"foo*"]).await;
    let _ = client.read_array().await;

    client.send_array(&[b"PUNSUBSCRIBE"]).await;
    let unsub = client.read_array().await;
    assert_eq!(unsub[0], b"punsubscribe");
    assert_eq!(unsub[1], b"foo*");
    assert_eq!(unsub[2], b"0");

    client.send_array(&[b"PING"]).await;
    let mut line = String::new();
    client.reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "+PONG\r\n");

    client.send_array(&[b"PING", b"hi"]).await;
    let mut bulk = String::new();
    client.reader.read_line(&mut bulk).await.unwrap();
    assert_eq!(bulk, "$2\r\n");
    let mut payload = vec![0u8; 2];
    client.reader.read_exact(&mut payload).await.unwrap();
    let mut crlf = [0u8; 2];
    client.reader.read_exact(&mut crlf).await.unwrap();
    assert_eq!(payload, b"hi");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn pubsub_introspection_counts_channels_and_patterns() {
    let (addr, shutdown, handle) = spawn_server().await;

    let mut sub1 = RespClient::connect(addr).await;
    let mut sub2 = RespClient::connect(addr).await;
    let mut sub3 = RespClient::connect(addr).await;

    sub1.send_array(&[b"SUBSCRIBE", b"alpha"]).await;
    let _ = sub1.read_array().await;

    sub2.send_array(&[b"PSUBSCRIBE", b"a*"]).await;
    let _ = sub2.read_array().await;

    sub3.send_array(&[b"SSUBSCRIBE", b"alpha"]).await;
    let _ = sub3.read_array().await;

    let mut client = RespClient::connect(addr).await;
    client.send_array(&[b"PUBSUB", b"CHANNELS"]).await;
    let channels = client.read_array().await;
    assert_eq!(channels, vec![b"alpha".to_vec()]);

    client.send_array(&[b"PUBSUB", b"CHANNELS", b"a*"]).await;
    let filtered = client.read_array().await;
    assert_eq!(filtered, vec![b"alpha".to_vec()]);

    client.send_array(&[b"PUBSUB", b"CHANNELS", b"b*"]).await;
    let none = client.read_array().await;
    assert!(none.is_empty());

    client
        .send_array(&[b"PUBSUB", b"NUMSUB", b"alpha", b"beta"])
        .await;
    let counts = client.read_array().await;
    assert_eq!(
        counts,
        vec![
            b"alpha".to_vec(),
            b"1".to_vec(),
            b"beta".to_vec(),
            b"0".to_vec()
        ]
    );

    client.send_array(&[b"PUBSUB", b"NUMPAT"]).await;
    let numpat = client.read_integer().await;
    assert_eq!(numpat, 1);

    // Shard introspection currently reuses active channels / zeros
    client
        .send_array(&[b"PUBSUB", b"SHARDCHANNELS", b"a*"])
        .await;
    let shard_channels = client.read_array().await;
    assert_eq!(shard_channels, vec![b"alpha".to_vec()]);

    client
        .send_array(&[b"PUBSUB", b"SHARDNUMSUB", b"alpha", b"beta"])
        .await;
    let shard_counts = client.read_array().await;
    assert_eq!(
        shard_counts,
        vec![
            b"alpha".to_vec(),
            b"1".to_vec(),
            b"beta".to_vec(),
            b"0".to_vec()
        ]
    );

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();

    drop(sub1);
    drop(sub2);
    drop(sub3);
}

#[tokio::test]
async fn channels_cleanup_after_disconnect() {
    let (addr, shutdown, handle) = spawn_server().await;

    {
        let mut sub = RespClient::connect(addr).await;
        sub.send_array(&[b"SUBSCRIBE", b"tmp"]).await;
        let _ = sub.read_array().await;
        // Drop connection without explicit unsubscribe
    }

    // allow server task to observe disconnect and cleanup
    sleep(Duration::from_millis(50)).await;

    let mut client = RespClient::connect(addr).await;
    client.send_array(&[b"PUBSUB", b"CHANNELS"]).await;
    let channels = client.read_array().await;
    assert!(channels.is_empty());

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn patterns_cleanup_after_disconnect() {
    let (addr, shutdown, handle) = spawn_server().await;

    {
        let mut sub = RespClient::connect(addr).await;
        sub.send_array(&[b"PSUBSCRIBE", b"p*"]).await;
        let _ = sub.read_array().await;
    }

    sleep(Duration::from_millis(50)).await;

    let mut client = RespClient::connect(addr).await;
    client.send_array(&[b"PUBSUB", b"NUMPAT"]).await;
    let numpat = client.read_integer().await;
    assert_eq!(numpat, 0);

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn unsubscribe_without_args_sends_one_event_per_channel() {
    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = RespClient::connect(addr).await;

    client.send_array(&[b"SUBSCRIBE", b"a", b"b"]).await;
    let _ = client.read_array().await;
    let _ = client.read_array().await;

    client.send_array(&[b"PSUBSCRIBE", b"p*"]).await;
    let _ = client.read_array().await;

    client.send_array(&[b"UNSUBSCRIBE"]).await;
    let first = client.read_array().await;
    let second = client.read_array().await;

    // Order is stable due to sorting
    assert_eq!(
        first,
        vec![b"unsubscribe".to_vec(), b"a".to_vec(), b"2".to_vec()]
    );
    assert_eq!(
        second,
        vec![b"unsubscribe".to_vec(), b"b".to_vec(), b"1".to_vec()]
    );

    client.send_array(&[b"PUNSUBSCRIBE"]).await;
    let final_evt = client.read_array().await;
    assert_eq!(
        final_evt,
        vec![b"punsubscribe".to_vec(), b"p*".to_vec(), b"0".to_vec()]
    );

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn psubscribe_supports_glob_sets_and_escapes() {
    let (addr, shutdown, handle) = spawn_server().await;

    let mut sub = RespClient::connect(addr).await;
    let mut pub_client = RespClient::connect(addr).await;

    sub.send_array(&[b"PSUBSCRIBE", b"n[ae]ws?"]).await;
    let _ = sub.read_array().await;

    pub_client
        .send_array(&[b"PUBLISH", b"news1", b"hello"])
        .await;
    let _ = pub_client.read_integer().await;

    pub_client.send_array(&[b"PUBLISH", b"naws2", b"hi"]).await;
    let _ = pub_client.read_integer().await;

    // should not match
    pub_client
        .send_array(&[b"PUBLISH", b"nbws2", b"skip"])
        .await;
    let _ = pub_client.read_integer().await;

    let first = sub.read_array().await;
    let second = sub.read_array().await;
    assert_eq!(first[0], b"pmessage");
    assert_eq!(second[0], b"pmessage");
    assert_ne!(first[2], second[2]); // matched two different channels

    // Escape should match literal '*'
    sub.send_array(&[b"PUNSUBSCRIBE", b"n[ae]ws?"]).await;
    let _ = sub.read_array().await;
    sub.send_array(&[b"PSUBSCRIBE", b"literal\\*"]).await;
    let _ = sub.read_array().await;
    pub_client
        .send_array(&[b"PUBLISH", b"literal*", b"ok"])
        .await;
    let _ = pub_client.read_integer().await;
    let msg = sub.read_array().await;
    assert_eq!(msg[2], b"literal*");
    assert_eq!(msg[3], b"ok");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn slow_subscriber_drops_old_messages_but_stays_subscribed() {
    let (addr, shutdown, handle) = spawn_server().await;

    let mut sub = RespClient::connect(addr).await;
    let mut pub_client = RespClient::connect(addr).await;

    sub.send_array(&[b"SUBSCRIBE", b"news"]).await;
    let _ = sub.read_array().await;

    // Send a burst larger than broadcast buffer without reading subscriber messages
    for i in 0..1500 {
        let payload = format!("msg{}", i);
        pub_client
            .send_array(&[b"PUBLISH", b"news", payload.as_bytes()])
            .await;
        let _ = pub_client.read_integer().await;
    }

    // Now publish a final marker and ensure subscriber still receives it
    pub_client
        .send_array(&[b"PUBLISH", b"news", b"final"])
        .await;
    let _ = pub_client.read_integer().await;

    // Drain messages until we see "final" (earlier ones may have been dropped)
    loop {
        let msg = sub.read_array().await;
        if msg[2] == b"final" {
            break;
        }
    }

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();
}
