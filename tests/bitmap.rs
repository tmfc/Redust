//! Integration tests for Bitmap commands (SETBIT, GETBIT, BITCOUNT, BITPOS, BITOP)

use std::net::SocketAddr;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

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

    async fn send_command_bytes(&mut self, args: &[&[u8]]) {
        let mut cmd = Vec::new();
        cmd.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
        for arg in args {
            cmd.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            cmd.extend_from_slice(arg);
            cmd.extend_from_slice(b"\r\n");
        }
        self.writer.write_all(&cmd).await.unwrap();
    }

    async fn read_integer(&mut self) -> i64 {
        let mut line = String::new();
        self.reader.read_line(&mut line).await.unwrap();
        assert!(line.starts_with(':'), "Expected integer, got: {}", line);
        line.trim_start_matches(':').trim().parse().unwrap()
    }

    async fn read_array_len(&mut self) -> usize {
        let mut line = String::new();
        self.reader.read_line(&mut line).await.unwrap();
        assert!(line.starts_with('*'), "Expected array, got: {}", line);
        line.trim_start_matches('*').trim().parse().unwrap()
    }

    async fn read_bulk_bytes(&mut self) -> Option<Vec<u8>> {
        let mut line = String::new();
        self.reader.read_line(&mut line).await.unwrap();
        if line.starts_with("$-1") {
            return None;
        }
        assert!(line.starts_with('$'), "Expected bulk string, got: {}", line);
        let len: usize = line.trim_start_matches('$').trim().parse().unwrap();
        let mut buf = vec![0u8; len + 2];
        self.reader.read_exact(&mut buf).await.unwrap();
        Some(buf[..len].to_vec())
    }

    async fn read_bulk_string(&mut self) -> Option<String> {
        self.read_bulk_bytes()
            .await
            .map(|b| String::from_utf8_lossy(&b).to_string())
    }

    async fn read_error(&mut self) -> String {
        let mut line = String::new();
        self.reader.read_line(&mut line).await.unwrap();
        assert!(line.starts_with('-'), "Expected error, got: {}", line);
        line.trim_start_matches('-').trim().to_string()
    }

    async fn read_simple_string(&mut self) -> String {
        let mut line = String::new();
        self.reader.read_line(&mut line).await.unwrap();
        assert!(line.starts_with('+'), "Expected simple string, got: {}", line);
        line.trim_start_matches('+').trim().to_string()
    }
}

#[tokio::test]
async fn test_setbit_getbit_basic() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // SETBIT on non-existent key
    client.send_command(&["SETBIT", "mykey", "7", "1"]).await;
    let old_bit = client.read_integer().await;
    assert_eq!(old_bit, 0);

    // GETBIT
    client.send_command(&["GETBIT", "mykey", "7"]).await;
    let bit = client.read_integer().await;
    assert_eq!(bit, 1);

    // GETBIT on unset bit
    client.send_command(&["GETBIT", "mykey", "0"]).await;
    let bit = client.read_integer().await;
    assert_eq!(bit, 0);

    // SETBIT to 0
    client.send_command(&["SETBIT", "mykey", "7", "0"]).await;
    let old_bit = client.read_integer().await;
    assert_eq!(old_bit, 1);

    client.send_command(&["GETBIT", "mykey", "7"]).await;
    let bit = client.read_integer().await;
    assert_eq!(bit, 0);

    // GETBIT on non-existent key
    client.send_command(&["GETBIT", "nonexistent", "100"]).await;
    let bit = client.read_integer().await;
    assert_eq!(bit, 0);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_bitfield_get_set_basic() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // SET u8 at bit offset 0 to 100, expect old value 0
    client.send_command(&["BITFIELD", "mykey", "SET", "u8", "0", "100"]).await;
    let n = client.read_array_len().await;
    assert_eq!(n, 1);
    let old = client.read_integer().await;
    assert_eq!(old, 0);

    // GET u8 at bit offset 0, expect 100
    client.send_command(&["BITFIELD", "mykey", "GET", "u8", "0"]).await;
    let n = client.read_array_len().await;
    assert_eq!(n, 1);
    let v = client.read_integer().await;
    assert_eq!(v, 100);

    // SET i8 at #1 (bit offset 8) to -1
    client.send_command(&["BITFIELD", "mykey", "SET", "i8", "#1", "-1"]).await;
    let n = client.read_array_len().await;
    assert_eq!(n, 1);
    let old = client.read_integer().await;
    assert_eq!(old, 0);

    // GET i8 at #1, expect -1
    client.send_command(&["BITFIELD", "mykey", "GET", "i8", "#1"]).await;
    let n = client.read_array_len().await;
    assert_eq!(n, 1);
    let v = client.read_integer().await;
    assert_eq!(v, -1);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_bitfield_incrby_and_overflow_sat() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // u2 can hold 0..3, use SAT so it clamps at 3
    client
        .send_command(&[
            "BITFIELD",
            "mykey",
            "OVERFLOW",
            "SAT",
            "INCRBY",
            "u2",
            "0",
            "1",
            "INCRBY",
            "u2",
            "0",
            "10",
        ])
        .await;

    let n = client.read_array_len().await;
    assert_eq!(n, 2);
    let a = client.read_integer().await;
    let b = client.read_integer().await;
    assert_eq!(a, 1);
    assert_eq!(b, 3);

    // read back
    client.send_command(&["BITFIELD", "mykey", "GET", "u2", "0"]).await;
    let n = client.read_array_len().await;
    assert_eq!(n, 1);
    let v = client.read_integer().await;
    assert_eq!(v, 3);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_bitfield_overflow_fail_returns_nil() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // u2 overflow with FAIL -> nil
    client
        .send_command(&[
            "BITFIELD",
            "mykey",
            "SET",
            "u2",
            "0",
            "3",
            "OVERFLOW",
            "FAIL",
            "INCRBY",
            "u2",
            "0",
            "1",
        ])
        .await;

    let n = client.read_array_len().await;
    assert_eq!(n, 2);

    // First reply is old value from SET
    let old = client.read_integer().await;
    assert_eq!(old, 0);

    // Second reply should be nil
    let bs = client.read_bulk_bytes().await;
    assert!(bs.is_none());

    // value should remain 3
    client.send_command(&["BITFIELD", "mykey", "GET", "u2", "0"]).await;
    let n = client.read_array_len().await;
    assert_eq!(n, 1);
    let v = client.read_integer().await;
    assert_eq!(v, 3);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_bitfield_ro_only_get() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // BITFIELD_RO GET is ok
    client.send_command(&["BITFIELD_RO", "mykey", "GET", "u8", "0"]).await;
    let n = client.read_array_len().await;
    assert_eq!(n, 1);
    let v = client.read_integer().await;
    assert_eq!(v, 0);

    // BITFIELD_RO SET should error
    client.send_command(&["BITFIELD_RO", "mykey", "SET", "u8", "0", "1"]).await;
    let err = client.read_error().await;
    assert!(err.contains("BITFIELD_RO") || err.contains("GET"));

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_setbit_extends_string() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // Set bit at offset 100 (byte 12)
    client.send_command(&["SETBIT", "mykey", "100", "1"]).await;
    let _ = client.read_integer().await;

    // Check string length
    client.send_command(&["STRLEN", "mykey"]).await;
    let len = client.read_integer().await;
    assert_eq!(len, 13); // 100/8 + 1 = 13 bytes

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_bitcount_basic() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // Set string "foobar"
    client.send_command(&["SET", "mykey", "foobar"]).await;
    let _ = client.read_simple_string().await;

    // BITCOUNT entire string
    client.send_command(&["BITCOUNT", "mykey"]).await;
    let count = client.read_integer().await;
    assert_eq!(count, 26); // "foobar" has 26 bits set

    // BITCOUNT with byte range
    client.send_command(&["BITCOUNT", "mykey", "0", "0"]).await;
    let count = client.read_integer().await;
    assert_eq!(count, 4); // 'f' = 0x66 = 01100110, 4 bits set

    // BITCOUNT with negative index
    client.send_command(&["BITCOUNT", "mykey", "-1", "-1"]).await;
    let count = client.read_integer().await;
    assert_eq!(count, 4);

    // BITCOUNT on non-existent key
    client.send_command(&["BITCOUNT", "nonexistent"]).await;
    let count = client.read_integer().await;
    assert_eq!(count, 0);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_bitcount_bit_mode() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // Set some bits
    client.send_command(&["SETBIT", "mykey", "0", "1"]).await;
    let _ = client.read_integer().await;
    client.send_command(&["SETBIT", "mykey", "1", "1"]).await;
    let _ = client.read_integer().await;
    client.send_command(&["SETBIT", "mykey", "2", "1"]).await;
    let _ = client.read_integer().await;

    // BITCOUNT with BIT mode
    client.send_command(&["BITCOUNT", "mykey", "0", "7", "BIT"]).await;
    let count = client.read_integer().await;
    assert_eq!(count, 3);

    // BITCOUNT with BIT mode, partial range
    client.send_command(&["BITCOUNT", "mykey", "0", "1", "BIT"]).await;
    let count = client.read_integer().await;
    assert_eq!(count, 2);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_bitpos_basic() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // Set string with known bit pattern
    // 0xFF = 11111111, 0xF0 = 11110000, 0x00 = 00000000
    client
        .send_command_bytes(&[b"SET", b"mykey", b"\xff\xf0\x00"])
        .await;
    let _ = client.read_simple_string().await;

    // Find first 0 bit
    client.send_command(&["BITPOS", "mykey", "0"]).await;
    let pos = client.read_integer().await;
    assert_eq!(pos, 12); // First 0 is at bit 12 (in 0xF0)

    // Find first 1 bit
    client.send_command(&["BITPOS", "mykey", "1"]).await;
    let pos = client.read_integer().await;
    assert_eq!(pos, 0); // First 1 is at bit 0

    // BITPOS on all-ones string looking for 0
    client
        .send_command_bytes(&[b"SET", b"allones", b"\xff\xff\xff"])
        .await;
    let _ = client.read_simple_string().await;
    client.send_command(&["BITPOS", "allones", "0"]).await;
    let pos = client.read_integer().await;
    assert_eq!(pos, 24); // No 0 found in string, returns position after string

    // BITPOS on all-zeros string looking for 1
    client
        .send_command_bytes(&[b"SET", b"allzeros", b"\x00\x00\x00"])
        .await;
    let _ = client.read_simple_string().await;
    client.send_command(&["BITPOS", "allzeros", "1"]).await;
    let pos = client.read_integer().await;
    assert_eq!(pos, -1); // No 1 found

    // BITPOS on non-existent key
    client.send_command(&["BITPOS", "nonexistent", "1"]).await;
    let pos = client.read_integer().await;
    assert_eq!(pos, -1);

    client.send_command(&["BITPOS", "nonexistent", "0"]).await;
    let pos = client.read_integer().await;
    assert_eq!(pos, 0);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_bitop_and() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    client
        .send_command_bytes(&[b"SET", b"key1", b"\xff\x0f"])
        .await;
    let _ = client.read_simple_string().await;
    client
        .send_command_bytes(&[b"SET", b"key2", b"\x0f\xff"])
        .await;
    let _ = client.read_simple_string().await;

    client.send_command(&["BITOP", "AND", "destkey", "key1", "key2"]).await;
    let len = client.read_integer().await;
    assert_eq!(len, 2);

    client.send_command(&["GET", "destkey"]).await;
    let val = client.read_bulk_bytes().await.unwrap();
    assert_eq!(val.as_slice(), &[0x0f, 0x0f]);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_bitop_or() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    client
        .send_command_bytes(&[b"SET", b"key1", b"\xf0\x00"])
        .await;
    let _ = client.read_simple_string().await;
    client
        .send_command_bytes(&[b"SET", b"key2", b"\x0f\x00"])
        .await;
    let _ = client.read_simple_string().await;

    client.send_command(&["BITOP", "OR", "destkey", "key1", "key2"]).await;
    let len = client.read_integer().await;
    assert_eq!(len, 2);

    client.send_command(&["GET", "destkey"]).await;
    let val = client.read_bulk_bytes().await.unwrap();
    assert_eq!(val.as_slice(), &[0xff, 0x00]);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_bitop_xor() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    client
        .send_command_bytes(&[b"SET", b"key1", b"\xff\xff"])
        .await;
    let _ = client.read_simple_string().await;
    client
        .send_command_bytes(&[b"SET", b"key2", b"\x0f\xf0"])
        .await;
    let _ = client.read_simple_string().await;

    client.send_command(&["BITOP", "XOR", "destkey", "key1", "key2"]).await;
    let len = client.read_integer().await;
    assert_eq!(len, 2);

    client.send_command(&["GET", "destkey"]).await;
    let val = client.read_bulk_bytes().await.unwrap();
    assert_eq!(val.as_slice(), &[0xf0, 0x0f]);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_bitop_not() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    client
        .send_command_bytes(&[b"SET", b"key1", b"\x00\xff"])
        .await;
    let _ = client.read_simple_string().await;

    client.send_command(&["BITOP", "NOT", "destkey", "key1"]).await;
    let len = client.read_integer().await;
    assert_eq!(len, 2);

    client.send_command(&["GET", "destkey"]).await;
    let val = client.read_bulk_bytes().await.unwrap();
    assert_eq!(val.as_slice(), &[0xff, 0x00]);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_bitop_not_requires_one_key() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    client
        .send_command_bytes(&[b"SET", b"key1", b"\x00"])
        .await;
    let _ = client.read_simple_string().await;
    client
        .send_command_bytes(&[b"SET", b"key2", b"\xff"])
        .await;
    let _ = client.read_simple_string().await;

    // BITOP NOT with multiple keys should error
    client.send_command(&["BITOP", "NOT", "destkey", "key1", "key2"]).await;
    let err = client.read_error().await;
    assert!(err.contains("NOT") || err.contains("one"));

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_bitop_different_lengths() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    client
        .send_command_bytes(&[b"SET", b"key1", b"\xff\xff\xff"])
        .await;
    let _ = client.read_simple_string().await;
    client
        .send_command_bytes(&[b"SET", b"key2", b"\x0f"])
        .await;
    let _ = client.read_simple_string().await;

    // AND with different lengths - shorter key is zero-padded
    client.send_command(&["BITOP", "AND", "destkey", "key1", "key2"]).await;
    let len = client.read_integer().await;
    assert_eq!(len, 3);

    client.send_command(&["GET", "destkey"]).await;
    let val = client.read_bulk_bytes().await.unwrap();
    assert_eq!(val.as_slice(), &[0x0f, 0x00, 0x00]);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_bitmap_wrongtype() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // Create a list
    client.send_command(&["LPUSH", "mylist", "value"]).await;
    let _ = client.read_integer().await;

    // SETBIT on list should error
    client.send_command(&["SETBIT", "mylist", "0", "1"]).await;
    let err = client.read_error().await;
    assert!(err.contains("WRONGTYPE"));

    // GETBIT on list should error
    client.send_command(&["GETBIT", "mylist", "0"]).await;
    let err = client.read_error().await;
    assert!(err.contains("WRONGTYPE"));

    // BITCOUNT on list should error
    client.send_command(&["BITCOUNT", "mylist"]).await;
    let err = client.read_error().await;
    assert!(err.contains("WRONGTYPE"));

    // BITPOS on list should error
    client.send_command(&["BITPOS", "mylist", "1"]).await;
    let err = client.read_error().await;
    assert!(err.contains("WRONGTYPE"));

    // BITOP on list should error
    client.send_command(&["BITOP", "AND", "dest", "mylist"]).await;
    let err = client.read_error().await;
    assert!(err.contains("WRONGTYPE"));

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_setbit_invalid_bit_value() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // SETBIT with value other than 0 or 1
    client.send_command(&["SETBIT", "mykey", "0", "2"]).await;
    let err = client.read_error().await;
    assert!(err.contains("bit") || err.contains("out of range"));

    client.send_command(&["SETBIT", "mykey", "0", "-1"]).await;
    let err = client.read_error().await;
    assert!(err.contains("bit") || err.contains("out of range"));

    let _ = shutdown.send(());
}
