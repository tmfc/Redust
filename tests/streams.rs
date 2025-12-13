use std::net::SocketAddr;

use redust::server::serve;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::oneshot;

async fn spawn_server() -> (
    SocketAddr,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<tokio::io::Result<()>>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = oneshot::channel();
    let shutdown = async move {
        let _ = rx.await;
    };
    let handle = tokio::spawn(async move { serve(listener, shutdown).await });
    (addr, tx, handle)
}

struct TestClient {
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: tokio::net::tcp::OwnedWriteHalf,
}

impl TestClient {
    async fn connect(addr: SocketAddr) -> Self {
        let stream = TcpStream::connect(addr).await.expect("Failed to connect");
        let (read_half, write_half) = stream.into_split();
        Self {
            reader: BufReader::new(read_half),
            writer: write_half,
        }
    }

    async fn send_command(&mut self, args: &[&str]) {
        let mut cmd = format!("*{}\r\n", args.len());
        for arg in args {
            cmd.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
        }
        self.writer.write_all(cmd.as_bytes()).await.unwrap();
    }

    async fn read_integer(&mut self) -> i64 {
        let mut line = String::new();
        self.reader.read_line(&mut line).await.unwrap();
        assert!(line.starts_with(':'), "Expected integer, got: {}", line);
        line.trim_start_matches(':').trim().parse().unwrap()
    }

    async fn read_simple_string(&mut self) -> String {
        let mut line = String::new();
        self.reader.read_line(&mut line).await.unwrap();
        assert!(line.starts_with('+'), "Expected simple string, got: {}", line);
        line.trim_start_matches('+').trim().to_string()
    }

    async fn read_error(&mut self) -> String {
        let mut line = String::new();
        self.reader.read_line(&mut line).await.unwrap();
        assert!(line.starts_with('-'), "Expected error, got: {}", line);
        line.trim_start_matches('-').trim().to_string()
    }

    async fn read_bulk_string(&mut self) -> Option<String> {
        let mut line = String::new();
        self.reader.read_line(&mut line).await.unwrap();
        if line.starts_with("$-1") {
            return None;
        }
        assert!(line.starts_with('$'), "Expected bulk string, got: {}", line);
        let len: usize = line.trim_start_matches('$').trim().parse().unwrap();
        let mut buf = vec![0u8; len + 2];
        self.reader.read_exact(&mut buf).await.unwrap();
        Some(String::from_utf8_lossy(&buf[..len]).to_string())
    }

    async fn read_array_len(&mut self) -> usize {
        let mut line = String::new();
        self.reader.read_line(&mut line).await.unwrap();
        assert!(line.starts_with('*'), "Expected array, got: {}", line);
        line.trim_start_matches('*').trim().parse().unwrap()
    }

    async fn read_null_array(&mut self) {
        let mut line = String::new();
        self.reader.read_line(&mut line).await.unwrap();
        assert_eq!(line.trim(), "*-1", "Expected null array, got: {}", line);
    }
}

#[tokio::test]
async fn test_xadd_xlen_xrange_basic() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // XLEN on missing key
    client.send_command(&["XLEN", "mystream"]).await;
    assert_eq!(client.read_integer().await, 0);

    // XADD *
    client
        .send_command(&["XADD", "mystream", "*", "f1", "v1", "f2", "v2"])
        .await;
    let id1 = client.read_bulk_string().await.unwrap();
    assert!(id1.contains('-'));

    // XLEN == 1
    client.send_command(&["XLEN", "mystream"]).await;
    assert_eq!(client.read_integer().await, 1);

    // XADD explicit id should succeed if greater
    client
        .send_command(&["XADD", "mystream", "9999999999999-0", "a", "b"])
        .await;
    let id2 = client.read_bulk_string().await.unwrap();
    assert_eq!(id2, "9999999999999-0");

    // XRANGE - +
    client.send_command(&["XRANGE", "mystream", "-", "+"]).await;
    let n = client.read_array_len().await;
    assert_eq!(n, 2);

    // entry 1: [id, [field,value...]]
    let _entry1_len = client.read_array_len().await;
    let _ = client.read_bulk_string().await.unwrap();
    let fv_len = client.read_array_len().await;
    assert_eq!(fv_len, 4);
    let _ = client.read_bulk_string().await.unwrap();
    let _ = client.read_bulk_string().await.unwrap();
    let _ = client.read_bulk_string().await.unwrap();
    let _ = client.read_bulk_string().await.unwrap();

    // entry 2
    let _entry2_len = client.read_array_len().await;
    let _ = client.read_bulk_string().await.unwrap();
    let fv_len = client.read_array_len().await;
    assert_eq!(fv_len, 2);
    let _ = client.read_bulk_string().await.unwrap();
    let _ = client.read_bulk_string().await.unwrap();

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_xadd_id_must_be_increasing() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    client
        .send_command(&["XADD", "mystream", "1-0", "f", "v"])
        .await;
    let id1 = client.read_bulk_string().await.unwrap();
    assert_eq!(id1, "1-0");

    // same id -> error
    client
        .send_command(&["XADD", "mystream", "1-0", "f", "v"])
        .await;
    let err = client.read_error().await;
    assert!(err.contains("ID") || err.contains("smaller"));

    // smaller id -> error
    client
        .send_command(&["XADD", "mystream", "0-1", "f", "v"])
        .await;
    let err = client.read_error().await;
    assert!(err.contains("ID") || err.contains("smaller"));

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_xrange_count() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    for i in 0..3 {
        client
            .send_command(&["XADD", "mystream", "*", "f", &format!("v{}", i)])
            .await;
        let _ = client.read_bulk_string().await.unwrap();
    }

    client
        .send_command(&["XRANGE", "mystream", "-", "+", "COUNT", "2"])
        .await;
    let n = client.read_array_len().await;
    assert_eq!(n, 2);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_xread_non_blocking_empty_returns_empty_array() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    client
        .send_command(&["XREAD", "STREAMS", "mystream", "0-0"])
        .await;
    let n = client.read_array_len().await;
    assert_eq!(n, 0);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_xread_reads_entries() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    client
        .send_command(&["XADD", "mystream", "*", "f", "v1"])
        .await;
    let _ = client.read_bulk_string().await.unwrap();

    client
        .send_command(&["XREAD", "COUNT", "10", "STREAMS", "mystream", "0-0"])
        .await;

    let outer = client.read_array_len().await;
    assert_eq!(outer, 1);

    let pair_len = client.read_array_len().await;
    assert_eq!(pair_len, 2);
    let key = client.read_bulk_string().await.unwrap();
    assert_eq!(key, "mystream");

    let entries_len = client.read_array_len().await;
    assert_eq!(entries_len, 1);
    let entry_len = client.read_array_len().await;
    assert_eq!(entry_len, 2);
    let _id = client.read_bulk_string().await.unwrap();
    let fv_len = client.read_array_len().await;
    assert_eq!(fv_len, 2);
    let field = client.read_bulk_string().await.unwrap();
    let val = client.read_bulk_string().await.unwrap();
    assert_eq!(field, "f");
    assert_eq!(val, "v1");

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_xread_block_timeout_returns_null() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    client
        .send_command(&["XREAD", "BLOCK", "1", "STREAMS", "mystream", "0-0"])
        .await;
    client.read_null_array().await;

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_xinfo_stream_missing_returns_empty_array() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    client.send_command(&["XINFO", "STREAM", "mystream"]).await;
    let n = client.read_array_len().await;
    assert_eq!(n, 0);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_xinfo_stream_basic_fields() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    client
        .send_command(&["XADD", "mystream", "*", "f", "v1"])
        .await;
    let id1 = client.read_bulk_string().await.unwrap();

    client
        .send_command(&["XADD", "mystream", "*", "f", "v2"])
        .await;
    let id2 = client.read_bulk_string().await.unwrap();

    client.send_command(&["XINFO", "STREAM", "mystream"]).await;

    let n = client.read_array_len().await;
    assert_eq!(n, 8);

    let f1 = client.read_bulk_string().await.unwrap();
    assert_eq!(f1, "length");
    assert_eq!(client.read_integer().await, 2);

    let f2 = client.read_bulk_string().await.unwrap();
    assert_eq!(f2, "last-generated-id");
    let last_id = client.read_bulk_string().await.unwrap();
    assert_eq!(last_id, id2);

    let f3 = client.read_bulk_string().await.unwrap();
    assert_eq!(f3, "first-entry");
    let first_pair_len = client.read_array_len().await;
    assert_eq!(first_pair_len, 2);
    let first_id = client.read_bulk_string().await.unwrap();
    assert_eq!(first_id, id1);
    let fv_len = client.read_array_len().await;
    assert_eq!(fv_len, 2);
    let field = client.read_bulk_string().await.unwrap();
    let val = client.read_bulk_string().await.unwrap();
    assert_eq!(field, "f");
    assert_eq!(val, "v1");

    let f4 = client.read_bulk_string().await.unwrap();
    assert_eq!(f4, "last-entry");
    let last_pair_len = client.read_array_len().await;
    assert_eq!(last_pair_len, 2);
    let last_id2 = client.read_bulk_string().await.unwrap();
    assert_eq!(last_id2, id2);
    let fv_len = client.read_array_len().await;
    assert_eq!(fv_len, 2);
    let field = client.read_bulk_string().await.unwrap();
    let val = client.read_bulk_string().await.unwrap();
    assert_eq!(field, "f");
    assert_eq!(val, "v2");

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_xinfo_stream_wrongtype() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    client.send_command(&["SET", "mystream", "hello"]).await;
    let _ = client.read_simple_string().await;

    client.send_command(&["XINFO", "STREAM", "mystream"]).await;
    let err = client.read_error().await;
    assert!(err.contains("WRONGTYPE"));

    let _ = shutdown.send(());
}

// ==================== XGROUP Tests ====================

#[tokio::test]
async fn test_xgroup_create_basic() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // Create a stream first
    client
        .send_command(&["XADD", "mystream", "*", "field", "value"])
        .await;
    let _ = client.read_bulk_string().await.unwrap();

    // Create a consumer group
    client
        .send_command(&["XGROUP", "CREATE", "mystream", "mygroup", "0-0"])
        .await;
    let resp = client.read_simple_string().await;
    assert_eq!(resp, "OK");

    // Creating the same group again should fail with BUSYGROUP
    client
        .send_command(&["XGROUP", "CREATE", "mystream", "mygroup", "0-0"])
        .await;
    let err = client.read_error().await;
    assert!(err.contains("BUSYGROUP"));

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_xgroup_create_mkstream() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // Creating group on non-existent stream without MKSTREAM should fail
    client
        .send_command(&["XGROUP", "CREATE", "newstream", "mygroup", "$"])
        .await;
    let err = client.read_error().await;
    assert!(err.contains("XGROUP subcommand requires the key to exist"));

    // Creating group with MKSTREAM should succeed
    client
        .send_command(&["XGROUP", "CREATE", "newstream", "mygroup", "$", "MKSTREAM"])
        .await;
    let resp = client.read_simple_string().await;
    assert_eq!(resp, "OK");

    // Verify stream was created (XLEN should return 0)
    client.send_command(&["XLEN", "newstream"]).await;
    assert_eq!(client.read_integer().await, 0);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_xgroup_setid() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // Create stream and group
    client
        .send_command(&["XADD", "mystream", "*", "f", "v"])
        .await;
    let _ = client.read_bulk_string().await.unwrap();

    client
        .send_command(&["XGROUP", "CREATE", "mystream", "mygroup", "0-0"])
        .await;
    let _ = client.read_simple_string().await;

    // SETID should succeed
    client
        .send_command(&["XGROUP", "SETID", "mystream", "mygroup", "$"])
        .await;
    let resp = client.read_simple_string().await;
    assert_eq!(resp, "OK");

    // SETID on non-existent group should fail
    client
        .send_command(&["XGROUP", "SETID", "mystream", "nogroup", "0-0"])
        .await;
    let err = client.read_error().await;
    assert!(err.contains("NOGROUP"));

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_xgroup_destroy() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // Create stream and group
    client
        .send_command(&["XADD", "mystream", "*", "f", "v"])
        .await;
    let _ = client.read_bulk_string().await.unwrap();

    client
        .send_command(&["XGROUP", "CREATE", "mystream", "mygroup", "0-0"])
        .await;
    let _ = client.read_simple_string().await;

    // DESTROY should return 1
    client
        .send_command(&["XGROUP", "DESTROY", "mystream", "mygroup"])
        .await;
    assert_eq!(client.read_integer().await, 1);

    // DESTROY again should return 0 (group doesn't exist)
    client
        .send_command(&["XGROUP", "DESTROY", "mystream", "mygroup"])
        .await;
    assert_eq!(client.read_integer().await, 0);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_xgroup_createconsumer_delconsumer() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // Create stream and group
    client
        .send_command(&["XADD", "mystream", "*", "f", "v"])
        .await;
    let _ = client.read_bulk_string().await.unwrap();

    client
        .send_command(&["XGROUP", "CREATE", "mystream", "mygroup", "0-0"])
        .await;
    let _ = client.read_simple_string().await;

    // CREATECONSUMER should return 1 for new consumer
    client
        .send_command(&["XGROUP", "CREATECONSUMER", "mystream", "mygroup", "consumer1"])
        .await;
    assert_eq!(client.read_integer().await, 1);

    // CREATECONSUMER again should return 0 (already exists)
    client
        .send_command(&["XGROUP", "CREATECONSUMER", "mystream", "mygroup", "consumer1"])
        .await;
    assert_eq!(client.read_integer().await, 0);

    // DELCONSUMER should return 0 (no pending messages)
    client
        .send_command(&["XGROUP", "DELCONSUMER", "mystream", "mygroup", "consumer1"])
        .await;
    assert_eq!(client.read_integer().await, 0);

    // DELCONSUMER on non-existent group should fail
    client
        .send_command(&["XGROUP", "DELCONSUMER", "mystream", "nogroup", "consumer1"])
        .await;
    let err = client.read_error().await;
    assert!(err.contains("NOGROUP"));

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_xgroup_wrongtype() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // Create a string key
    client.send_command(&["SET", "mykey", "hello"]).await;
    let _ = client.read_simple_string().await;

    // XGROUP CREATE on string should fail with WRONGTYPE
    client
        .send_command(&["XGROUP", "CREATE", "mykey", "mygroup", "0-0"])
        .await;
    let err = client.read_error().await;
    assert!(err.contains("WRONGTYPE"));

    let _ = shutdown.send(());
}

// ==================== XREADGROUP Tests ====================

#[tokio::test]
async fn test_xreadgroup_basic() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // Create stream with some messages
    client
        .send_command(&["XADD", "mystream", "1-0", "field1", "value1"])
        .await;
    let id1 = client.read_bulk_string().await.unwrap();
    assert_eq!(id1, "1-0");

    client
        .send_command(&["XADD", "mystream", "2-0", "field2", "value2"])
        .await;
    let id2 = client.read_bulk_string().await.unwrap();
    assert_eq!(id2, "2-0");

    // Create consumer group starting from 0
    client
        .send_command(&["XGROUP", "CREATE", "mystream", "mygroup", "0-0"])
        .await;
    let resp = client.read_simple_string().await;
    assert_eq!(resp, "OK");

    // Read with XREADGROUP - should get both messages
    client
        .send_command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer1",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;

    // Parse response: *1 (array of 1 stream)
    let stream_count = client.read_array_len().await;
    assert_eq!(stream_count, 1);

    // Stream entry: *2 (stream name, entries)
    let stream_parts = client.read_array_len().await;
    assert_eq!(stream_parts, 2);

    let stream_name = client.read_bulk_string().await.unwrap();
    assert_eq!(stream_name, "mystream");

    // Entries array
    let entry_count = client.read_array_len().await;
    assert_eq!(entry_count, 2);

    // First entry
    let entry_parts = client.read_array_len().await;
    assert_eq!(entry_parts, 2);
    let entry_id = client.read_bulk_string().await.unwrap();
    assert_eq!(entry_id, "1-0");
    let fields_count = client.read_array_len().await;
    assert_eq!(fields_count, 2);
    let f1 = client.read_bulk_string().await.unwrap();
    let v1 = client.read_bulk_string().await.unwrap();
    assert_eq!(f1, "field1");
    assert_eq!(v1, "value1");

    // Second entry
    let entry_parts = client.read_array_len().await;
    assert_eq!(entry_parts, 2);
    let entry_id = client.read_bulk_string().await.unwrap();
    assert_eq!(entry_id, "2-0");
    let fields_count = client.read_array_len().await;
    assert_eq!(fields_count, 2);
    let f2 = client.read_bulk_string().await.unwrap();
    let v2 = client.read_bulk_string().await.unwrap();
    assert_eq!(f2, "field2");
    assert_eq!(v2, "value2");

    // Reading again with ">" should return null (no new messages)
    client
        .send_command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer1",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;
    client.read_null_array().await;

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_xreadgroup_with_count() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // Create stream with messages
    for i in 1..=5 {
        client
            .send_command(&[
                "XADD",
                "mystream",
                &format!("{}-0", i),
                "n",
                &i.to_string(),
            ])
            .await;
        let _ = client.read_bulk_string().await.unwrap();
    }

    // Create consumer group
    client
        .send_command(&["XGROUP", "CREATE", "mystream", "mygroup", "0-0"])
        .await;
    let _ = client.read_simple_string().await;

    // Read with COUNT 2
    client
        .send_command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer1",
            "COUNT",
            "2",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;

    let stream_count = client.read_array_len().await;
    assert_eq!(stream_count, 1);
    let _ = client.read_array_len().await; // stream parts
    let _ = client.read_bulk_string().await.unwrap(); // stream name
    let entry_count = client.read_array_len().await;
    assert_eq!(entry_count, 2); // Only 2 entries due to COUNT

    // Skip reading the entries
    for _ in 0..2 {
        let _ = client.read_array_len().await;
        let _ = client.read_bulk_string().await;
        let _ = client.read_array_len().await;
        let _ = client.read_bulk_string().await;
        let _ = client.read_bulk_string().await;
    }

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_xreadgroup_nogroup_error() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // Create stream without group
    client
        .send_command(&["XADD", "mystream", "*", "f", "v"])
        .await;
    let _ = client.read_bulk_string().await.unwrap();

    // XREADGROUP on non-existent group should fail
    client
        .send_command(&[
            "XREADGROUP",
            "GROUP",
            "nogroup",
            "consumer1",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;
    let err = client.read_error().await;
    assert!(err.contains("NOGROUP"));

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_xreadgroup_noack() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // Create stream and group
    client
        .send_command(&["XADD", "mystream", "1-0", "f", "v"])
        .await;
    let _ = client.read_bulk_string().await.unwrap();

    client
        .send_command(&["XGROUP", "CREATE", "mystream", "mygroup", "0-0"])
        .await;
    let _ = client.read_simple_string().await;

    // Read with NOACK
    client
        .send_command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer1",
            "NOACK",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;

    let stream_count = client.read_array_len().await;
    assert_eq!(stream_count, 1);

    // Skip the rest of the response
    let _ = client.read_array_len().await;
    let _ = client.read_bulk_string().await;
    let _ = client.read_array_len().await;
    let _ = client.read_array_len().await;
    let _ = client.read_bulk_string().await;
    let _ = client.read_array_len().await;
    let _ = client.read_bulk_string().await;
    let _ = client.read_bulk_string().await;

    // With NOACK, deleting consumer should return 0 pending
    client
        .send_command(&["XGROUP", "DELCONSUMER", "mystream", "mygroup", "consumer1"])
        .await;
    assert_eq!(client.read_integer().await, 0);

    let _ = shutdown.send(());
}

// ==================== XACK Tests ====================

#[tokio::test]
async fn test_xack_basic() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // Create stream and group
    client
        .send_command(&["XADD", "mystream", "1-0", "f", "v"])
        .await;
    let _ = client.read_bulk_string().await.unwrap();

    client
        .send_command(&["XADD", "mystream", "2-0", "f", "v"])
        .await;
    let _ = client.read_bulk_string().await.unwrap();

    client
        .send_command(&["XGROUP", "CREATE", "mystream", "mygroup", "0-0"])
        .await;
    let _ = client.read_simple_string().await;

    // Read messages (adds to PEL)
    client
        .send_command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer1",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;
    // Skip response
    let _ = client.read_array_len().await;
    let _ = client.read_array_len().await;
    let _ = client.read_bulk_string().await;
    let _ = client.read_array_len().await;
    for _ in 0..2 {
        let _ = client.read_array_len().await;
        let _ = client.read_bulk_string().await;
        let _ = client.read_array_len().await;
        let _ = client.read_bulk_string().await;
        let _ = client.read_bulk_string().await;
    }

    // XACK one message
    client
        .send_command(&["XACK", "mystream", "mygroup", "1-0"])
        .await;
    assert_eq!(client.read_integer().await, 1);

    // XACK same message again should return 0
    client
        .send_command(&["XACK", "mystream", "mygroup", "1-0"])
        .await;
    assert_eq!(client.read_integer().await, 0);

    // XACK second message
    client
        .send_command(&["XACK", "mystream", "mygroup", "2-0"])
        .await;
    assert_eq!(client.read_integer().await, 1);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_xack_multiple() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // Create stream with messages
    for i in 1..=3 {
        client
            .send_command(&["XADD", "mystream", &format!("{}-0", i), "f", "v"])
            .await;
        let _ = client.read_bulk_string().await.unwrap();
    }

    client
        .send_command(&["XGROUP", "CREATE", "mystream", "mygroup", "0-0"])
        .await;
    let _ = client.read_simple_string().await;

    // Read all messages
    client
        .send_command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer1",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;
    // Skip response
    let _ = client.read_array_len().await;
    let _ = client.read_array_len().await;
    let _ = client.read_bulk_string().await;
    let _ = client.read_array_len().await;
    for _ in 0..3 {
        let _ = client.read_array_len().await;
        let _ = client.read_bulk_string().await;
        let _ = client.read_array_len().await;
        let _ = client.read_bulk_string().await;
        let _ = client.read_bulk_string().await;
    }

    // XACK multiple messages at once
    client
        .send_command(&["XACK", "mystream", "mygroup", "1-0", "2-0", "3-0"])
        .await;
    assert_eq!(client.read_integer().await, 3);

    let _ = shutdown.send(());
}

// ==================== XPENDING Tests ====================

#[tokio::test]
async fn test_xpending_basic() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // Create stream and group
    client
        .send_command(&["XADD", "mystream", "1-0", "f", "v"])
        .await;
    let _ = client.read_bulk_string().await.unwrap();

    client
        .send_command(&["XADD", "mystream", "2-0", "f", "v"])
        .await;
    let _ = client.read_bulk_string().await.unwrap();

    client
        .send_command(&["XGROUP", "CREATE", "mystream", "mygroup", "0-0"])
        .await;
    let _ = client.read_simple_string().await;

    // XPENDING before any reads should show 0 pending
    client
        .send_command(&["XPENDING", "mystream", "mygroup"])
        .await;
    let arr_len = client.read_array_len().await;
    assert_eq!(arr_len, 4);
    let total = client.read_integer().await;
    assert_eq!(total, 0);
    // Skip null min_id, max_id, and empty consumers array
    let _ = client.read_bulk_string().await; // null
    let _ = client.read_bulk_string().await; // null
    let _ = client.read_array_len().await; // empty consumers

    // Read messages
    client
        .send_command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer1",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;
    // Skip response
    let _ = client.read_array_len().await;
    let _ = client.read_array_len().await;
    let _ = client.read_bulk_string().await;
    let _ = client.read_array_len().await;
    for _ in 0..2 {
        let _ = client.read_array_len().await;
        let _ = client.read_bulk_string().await;
        let _ = client.read_array_len().await;
        let _ = client.read_bulk_string().await;
        let _ = client.read_bulk_string().await;
    }

    // XPENDING should now show 2 pending
    client
        .send_command(&["XPENDING", "mystream", "mygroup"])
        .await;
    let arr_len = client.read_array_len().await;
    assert_eq!(arr_len, 4);
    let total = client.read_integer().await;
    assert_eq!(total, 2);
    let min_id = client.read_bulk_string().await.unwrap();
    assert_eq!(min_id, "1-0");
    let max_id = client.read_bulk_string().await.unwrap();
    assert_eq!(max_id, "2-0");

    // Consumer list
    let consumers_len = client.read_array_len().await;
    assert_eq!(consumers_len, 1);
    let _ = client.read_array_len().await; // consumer entry
    let consumer_name = client.read_bulk_string().await.unwrap();
    assert_eq!(consumer_name, "consumer1");
    let pending_count = client.read_bulk_string().await.unwrap();
    assert_eq!(pending_count, "2");

    // XACK one message
    client
        .send_command(&["XACK", "mystream", "mygroup", "1-0"])
        .await;
    assert_eq!(client.read_integer().await, 1);

    // XPENDING should now show 1 pending
    client
        .send_command(&["XPENDING", "mystream", "mygroup"])
        .await;
    let arr_len = client.read_array_len().await;
    assert_eq!(arr_len, 4);
    let total = client.read_integer().await;
    assert_eq!(total, 1);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_xpending_nogroup_error() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // Create stream without group
    client
        .send_command(&["XADD", "mystream", "*", "f", "v"])
        .await;
    let _ = client.read_bulk_string().await.unwrap();

    // XPENDING on non-existent group should fail
    client
        .send_command(&["XPENDING", "mystream", "nogroup"])
        .await;
    let err = client.read_error().await;
    assert!(err.contains("NOGROUP"));

    let _ = shutdown.send(());
}

// ==================== XCLAIM Tests ====================

#[tokio::test]
async fn test_xclaim_basic() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // Create stream and group
    client
        .send_command(&["XADD", "mystream", "1-0", "f", "v1"])
        .await;
    let _ = client.read_bulk_string().await.unwrap();

    client
        .send_command(&["XADD", "mystream", "2-0", "f", "v2"])
        .await;
    let _ = client.read_bulk_string().await.unwrap();

    client
        .send_command(&["XGROUP", "CREATE", "mystream", "mygroup", "0-0"])
        .await;
    let _ = client.read_simple_string().await;

    // Consumer1 reads messages (adds to PEL)
    client
        .send_command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer1",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;
    // Skip response
    let _ = client.read_array_len().await;
    let _ = client.read_array_len().await;
    let _ = client.read_bulk_string().await;
    let _ = client.read_array_len().await;
    for _ in 0..2 {
        let _ = client.read_array_len().await;
        let _ = client.read_bulk_string().await;
        let _ = client.read_array_len().await;
        let _ = client.read_bulk_string().await;
        let _ = client.read_bulk_string().await;
    }

    // Wait a bit for idle time
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    // XCLAIM message from consumer1 to consumer2 with min-idle-time 0
    client
        .send_command(&["XCLAIM", "mystream", "mygroup", "consumer2", "0", "1-0"])
        .await;
    let arr_len = client.read_array_len().await;
    assert_eq!(arr_len, 1);
    // Read entry: [id, [fields...]]
    let _ = client.read_array_len().await;
    let id = client.read_bulk_string().await.unwrap();
    assert_eq!(id, "1-0");
    let fields_len = client.read_array_len().await;
    assert_eq!(fields_len, 2);
    let _ = client.read_bulk_string().await; // field
    let _ = client.read_bulk_string().await; // value

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_xclaim_min_idle_time() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // Create stream and group
    client
        .send_command(&["XADD", "mystream", "1-0", "f", "v"])
        .await;
    let _ = client.read_bulk_string().await.unwrap();

    client
        .send_command(&["XGROUP", "CREATE", "mystream", "mygroup", "0-0"])
        .await;
    let _ = client.read_simple_string().await;

    // Consumer1 reads message
    client
        .send_command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer1",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;
    // Skip response
    let _ = client.read_array_len().await;
    let _ = client.read_array_len().await;
    let _ = client.read_bulk_string().await;
    let _ = client.read_array_len().await;
    let _ = client.read_array_len().await;
    let _ = client.read_bulk_string().await;
    let _ = client.read_array_len().await;
    let _ = client.read_bulk_string().await;
    let _ = client.read_bulk_string().await;

    // XCLAIM with very high min-idle-time should return empty
    client
        .send_command(&["XCLAIM", "mystream", "mygroup", "consumer2", "999999999", "1-0"])
        .await;
    let arr_len = client.read_array_len().await;
    assert_eq!(arr_len, 0);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_xclaim_justid() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // Create stream and group
    client
        .send_command(&["XADD", "mystream", "1-0", "f", "v"])
        .await;
    let _ = client.read_bulk_string().await.unwrap();

    client
        .send_command(&["XGROUP", "CREATE", "mystream", "mygroup", "0-0"])
        .await;
    let _ = client.read_simple_string().await;

    // Consumer1 reads message
    client
        .send_command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer1",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;
    // Skip response
    let _ = client.read_array_len().await;
    let _ = client.read_array_len().await;
    let _ = client.read_bulk_string().await;
    let _ = client.read_array_len().await;
    let _ = client.read_array_len().await;
    let _ = client.read_bulk_string().await;
    let _ = client.read_array_len().await;
    let _ = client.read_bulk_string().await;
    let _ = client.read_bulk_string().await;

    // XCLAIM with JUSTID should return only IDs
    client
        .send_command(&["XCLAIM", "mystream", "mygroup", "consumer2", "0", "1-0", "JUSTID"])
        .await;
    let arr_len = client.read_array_len().await;
    assert_eq!(arr_len, 1);
    let id = client.read_bulk_string().await.unwrap();
    assert_eq!(id, "1-0");

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_xclaim_force() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // Create stream and group
    client
        .send_command(&["XADD", "mystream", "1-0", "f", "v"])
        .await;
    let _ = client.read_bulk_string().await.unwrap();

    client
        .send_command(&["XGROUP", "CREATE", "mystream", "mygroup", "0-0"])
        .await;
    let _ = client.read_simple_string().await;

    // XCLAIM without FORCE on message not in PEL should return empty
    client
        .send_command(&["XCLAIM", "mystream", "mygroup", "consumer1", "0", "1-0"])
        .await;
    let arr_len = client.read_array_len().await;
    assert_eq!(arr_len, 0);

    // XCLAIM with FORCE should create PEL entry
    client
        .send_command(&["XCLAIM", "mystream", "mygroup", "consumer1", "0", "1-0", "FORCE"])
        .await;
    let arr_len = client.read_array_len().await;
    assert_eq!(arr_len, 1);
    // Skip entry content
    let _ = client.read_array_len().await;
    let id = client.read_bulk_string().await.unwrap();
    assert_eq!(id, "1-0");
    let _ = client.read_array_len().await;
    let _ = client.read_bulk_string().await;
    let _ = client.read_bulk_string().await;

    // Verify message is now in PEL via XPENDING
    client
        .send_command(&["XPENDING", "mystream", "mygroup"])
        .await;
    let _ = client.read_array_len().await;
    let total = client.read_integer().await;
    assert_eq!(total, 1);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_xclaim_nogroup_error() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // Create stream without group
    client
        .send_command(&["XADD", "mystream", "*", "f", "v"])
        .await;
    let _ = client.read_bulk_string().await.unwrap();

    // XCLAIM on non-existent group should fail
    client
        .send_command(&["XCLAIM", "mystream", "nogroup", "consumer", "0", "1-0"])
        .await;
    let err = client.read_error().await;
    assert!(err.contains("NOGROUP"));

    let _ = shutdown.send(());
}
