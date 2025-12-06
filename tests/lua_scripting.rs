//! Integration tests for Lua scripting (EVAL, EVALSHA, SCRIPT commands)

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

    async fn read_line(&mut self) -> String {
        let mut line = String::new();
        self.reader.read_line(&mut line).await.unwrap();
        line
    }

    async fn read_bulk_string(&mut self) -> Option<String> {
        let line = self.read_line().await;
        if line.starts_with("$-1") {
            return None;
        }
        if !line.starts_with('$') {
            panic!("Expected bulk string, got: {}", line);
        }
        let len: usize = line[1..].trim().parse().unwrap();
        let mut buf = vec![0u8; len];
        self.reader.read_exact(&mut buf).await.unwrap();
        // Read trailing \r\n
        let mut crlf = [0u8; 2];
        self.reader.read_exact(&mut crlf).await.unwrap();
        Some(String::from_utf8_lossy(&buf).to_string())
    }

    async fn read_integer(&mut self) -> i64 {
        let line = self.read_line().await;
        if !line.starts_with(':') {
            panic!("Expected integer, got: {}", line);
        }
        line[1..].trim().parse().unwrap()
    }

    async fn read_error(&mut self) -> String {
        let line = self.read_line().await;
        if !line.starts_with('-') {
            panic!("Expected error, got: {}", line);
        }
        line[1..].trim().to_string()
    }

    async fn read_simple_string(&mut self) -> String {
        let line = self.read_line().await;
        if !line.starts_with('+') {
            panic!("Expected simple string, got: {}", line);
        }
        line[1..].trim().to_string()
    }

    async fn read_array_len(&mut self) -> i64 {
        let line = self.read_line().await;
        if !line.starts_with('*') {
            panic!("Expected array, got: {}", line);
        }
        line[1..].trim().parse().unwrap()
    }
}

#[tokio::test]
async fn test_eval_return_integer() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // EVAL "return 42" 0
    client.send_command(&["EVAL", "return 42", "0"]).await;
    let result = client.read_integer().await;
    assert_eq!(result, 42);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_eval_return_string() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // EVAL "return 'hello'" 0
    client.send_command(&["EVAL", "return 'hello'", "0"]).await;
    let result = client.read_bulk_string().await;
    assert_eq!(result, Some("hello".to_string()));

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_eval_return_nil() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // EVAL "return nil" 0
    client.send_command(&["EVAL", "return nil", "0"]).await;
    let result = client.read_bulk_string().await;
    assert_eq!(result, None);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_eval_with_keys() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // EVAL "return KEYS[1]" 1 mykey
    client
        .send_command(&["EVAL", "return KEYS[1]", "1", "mykey"])
        .await;
    let result = client.read_bulk_string().await;
    assert_eq!(result, Some("mykey".to_string()));

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_eval_with_argv() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // EVAL "return ARGV[1]" 0 myarg
    client
        .send_command(&["EVAL", "return ARGV[1]", "0", "myarg"])
        .await;
    let result = client.read_bulk_string().await;
    assert_eq!(result, Some("myarg".to_string()));

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_eval_return_array() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // EVAL "return {1, 2, 3}" 0
    client
        .send_command(&["EVAL", "return {1, 2, 3}", "0"])
        .await;
    let len = client.read_array_len().await;
    assert_eq!(len, 3);
    assert_eq!(client.read_integer().await, 1);
    assert_eq!(client.read_integer().await, 2);
    assert_eq!(client.read_integer().await, 3);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_eval_syntax_error() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // EVAL with syntax error
    client
        .send_command(&["EVAL", "return invalid syntax here", "0"])
        .await;
    let err = client.read_error().await;
    assert!(err.contains("ERR"));

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_script_load_and_evalsha() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // SCRIPT LOAD "return 123"
    client.send_command(&["SCRIPT", "LOAD", "return 123"]).await;
    let sha1 = client.read_bulk_string().await.unwrap();
    assert_eq!(sha1.len(), 40); // SHA1 is 40 hex chars

    // EVALSHA <sha1> 0
    client.send_command(&["EVALSHA", &sha1, "0"]).await;
    let result = client.read_integer().await;
    assert_eq!(result, 123);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_evalsha_noscript() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // EVALSHA with non-existent script
    client
        .send_command(&["EVALSHA", "0000000000000000000000000000000000000000", "0"])
        .await;
    let err = client.read_error().await;
    assert!(err.contains("NOSCRIPT"));

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_script_exists() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // Load a script
    client.send_command(&["SCRIPT", "LOAD", "return 1"]).await;
    let sha1 = client.read_bulk_string().await.unwrap();

    // SCRIPT EXISTS <sha1> <nonexistent>
    client
        .send_command(&[
            "SCRIPT",
            "EXISTS",
            &sha1,
            "0000000000000000000000000000000000000000",
        ])
        .await;
    let len = client.read_array_len().await;
    assert_eq!(len, 2);
    assert_eq!(client.read_integer().await, 1); // exists
    assert_eq!(client.read_integer().await, 0); // doesn't exist

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_script_flush() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // Load a script
    client.send_command(&["SCRIPT", "LOAD", "return 1"]).await;
    let sha1 = client.read_bulk_string().await.unwrap();

    // Verify it exists
    client.send_command(&["SCRIPT", "EXISTS", &sha1]).await;
    let _ = client.read_array_len().await;
    assert_eq!(client.read_integer().await, 1);

    // SCRIPT FLUSH
    client.send_command(&["SCRIPT", "FLUSH"]).await;
    let ok = client.read_simple_string().await;
    assert_eq!(ok, "OK");

    // Verify it no longer exists
    client.send_command(&["SCRIPT", "EXISTS", &sha1]).await;
    let _ = client.read_array_len().await;
    assert_eq!(client.read_integer().await, 0);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_eval_boolean_conversion() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // Redis Lua: true -> 1
    client.send_command(&["EVAL", "return true", "0"]).await;
    let result = client.read_integer().await;
    assert_eq!(result, 1);

    // Redis Lua: false -> nil
    client.send_command(&["EVAL", "return false", "0"]).await;
    let result = client.read_bulk_string().await;
    assert_eq!(result, None);

    let _ = shutdown.send(());
}
