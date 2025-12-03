//! Integration tests for Lua scripting (EVAL, EVALSHA, SCRIPT commands)

use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::thread;
use std::time::Duration;

use std::sync::atomic::{AtomicU16, Ordering};

static PORT_COUNTER: AtomicU16 = AtomicU16::new(16500);

fn spawn_server() -> (Child, String) {
    let port = PORT_COUNTER.fetch_add(1, Ordering::SeqCst);
    let addr = format!("127.0.0.1:{}", port);
    
    let child = Command::new("cargo")
        .args(["run", "--bin", "redust"])
        .env("REDUST_ADDR", &addr)
        .env("REDUST_AUTH_PASSWORD", "")
        .spawn()
        .expect("Failed to start server");
    
    // Wait for server to start
    thread::sleep(Duration::from_millis(1000));
    
    (child, addr)
}

struct TestClient {
    stream: BufReader<TcpStream>,
}

impl TestClient {
    fn connect(addr: &str) -> Self {
        let stream = TcpStream::connect(addr).expect("Failed to connect");
        stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        Self {
            stream: BufReader::new(stream),
        }
    }

    fn send_command(&mut self, args: &[&str]) {
        let mut cmd = format!("*{}\r\n", args.len());
        for arg in args {
            cmd.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
        }
        self.stream.get_mut().write_all(cmd.as_bytes()).unwrap();
    }

    fn read_line(&mut self) -> String {
        let mut line = String::new();
        self.stream.read_line(&mut line).unwrap();
        line
    }

    fn read_bulk_string(&mut self) -> Option<String> {
        let line = self.read_line();
        if line.starts_with("$-1") {
            return None;
        }
        if !line.starts_with('$') {
            panic!("Expected bulk string, got: {}", line);
        }
        let len: usize = line[1..].trim().parse().unwrap();
        let mut buf = vec![0u8; len + 2]; // +2 for \r\n
        std::io::Read::read_exact(&mut self.stream, &mut buf).unwrap();
        Some(String::from_utf8_lossy(&buf[..len]).to_string())
    }

    fn read_integer(&mut self) -> i64 {
        let line = self.read_line();
        if !line.starts_with(':') {
            panic!("Expected integer, got: {}", line);
        }
        line[1..].trim().parse().unwrap()
    }

    fn read_error(&mut self) -> String {
        let line = self.read_line();
        if !line.starts_with('-') {
            panic!("Expected error, got: {}", line);
        }
        line[1..].trim().to_string()
    }

    fn read_simple_string(&mut self) -> String {
        let line = self.read_line();
        if !line.starts_with('+') {
            panic!("Expected simple string, got: {}", line);
        }
        line[1..].trim().to_string()
    }

    fn read_array_len(&mut self) -> i64 {
        let line = self.read_line();
        if !line.starts_with('*') {
            panic!("Expected array, got: {}", line);
        }
        line[1..].trim().parse().unwrap()
    }
}

#[test]
fn test_eval_return_integer() {
    let (mut child, addr) = spawn_server();
    let mut client = TestClient::connect(&addr);

    // EVAL "return 42" 0
    client.send_command(&["EVAL", "return 42", "0"]);
    let result = client.read_integer();
    assert_eq!(result, 42);

    child.kill().unwrap();
}

#[test]
fn test_eval_return_string() {
    let (mut child, addr) = spawn_server();
    let mut client = TestClient::connect(&addr);

    // EVAL "return 'hello'" 0
    client.send_command(&["EVAL", "return 'hello'", "0"]);
    let result = client.read_bulk_string();
    assert_eq!(result, Some("hello".to_string()));

    child.kill().unwrap();
}

#[test]
fn test_eval_return_nil() {
    let (mut child, addr) = spawn_server();
    let mut client = TestClient::connect(&addr);

    // EVAL "return nil" 0
    client.send_command(&["EVAL", "return nil", "0"]);
    let result = client.read_bulk_string();
    assert_eq!(result, None);

    child.kill().unwrap();
}

#[test]
fn test_eval_with_keys() {
    let (mut child, addr) = spawn_server();
    let mut client = TestClient::connect(&addr);

    // EVAL "return KEYS[1]" 1 mykey
    client.send_command(&["EVAL", "return KEYS[1]", "1", "mykey"]);
    let result = client.read_bulk_string();
    assert_eq!(result, Some("mykey".to_string()));

    child.kill().unwrap();
}

#[test]
fn test_eval_with_argv() {
    let (mut child, addr) = spawn_server();
    let mut client = TestClient::connect(&addr);

    // EVAL "return ARGV[1]" 0 myarg
    client.send_command(&["EVAL", "return ARGV[1]", "0", "myarg"]);
    let result = client.read_bulk_string();
    assert_eq!(result, Some("myarg".to_string()));

    child.kill().unwrap();
}

#[test]
fn test_eval_return_array() {
    let (mut child, addr) = spawn_server();
    let mut client = TestClient::connect(&addr);

    // EVAL "return {1, 2, 3}" 0
    client.send_command(&["EVAL", "return {1, 2, 3}", "0"]);
    let len = client.read_array_len();
    assert_eq!(len, 3);
    assert_eq!(client.read_integer(), 1);
    assert_eq!(client.read_integer(), 2);
    assert_eq!(client.read_integer(), 3);

    child.kill().unwrap();
}

#[test]
fn test_eval_syntax_error() {
    let (mut child, addr) = spawn_server();
    let mut client = TestClient::connect(&addr);

    // EVAL with syntax error
    client.send_command(&["EVAL", "return invalid syntax here", "0"]);
    let err = client.read_error();
    assert!(err.contains("ERR"));

    child.kill().unwrap();
}

#[test]
fn test_script_load_and_evalsha() {
    let (mut child, addr) = spawn_server();
    let mut client = TestClient::connect(&addr);

    // SCRIPT LOAD "return 123"
    client.send_command(&["SCRIPT", "LOAD", "return 123"]);
    let sha1 = client.read_bulk_string().unwrap();
    assert_eq!(sha1.len(), 40); // SHA1 is 40 hex chars

    // EVALSHA <sha1> 0
    client.send_command(&["EVALSHA", &sha1, "0"]);
    let result = client.read_integer();
    assert_eq!(result, 123);

    child.kill().unwrap();
}

#[test]
fn test_evalsha_noscript() {
    let (mut child, addr) = spawn_server();
    let mut client = TestClient::connect(&addr);

    // EVALSHA with non-existent script
    client.send_command(&["EVALSHA", "0000000000000000000000000000000000000000", "0"]);
    let err = client.read_error();
    assert!(err.contains("NOSCRIPT"));

    child.kill().unwrap();
}

#[test]
fn test_script_exists() {
    let (mut child, addr) = spawn_server();
    let mut client = TestClient::connect(&addr);

    // Load a script
    client.send_command(&["SCRIPT", "LOAD", "return 1"]);
    let sha1 = client.read_bulk_string().unwrap();

    // SCRIPT EXISTS <sha1> <nonexistent>
    client.send_command(&["SCRIPT", "EXISTS", &sha1, "0000000000000000000000000000000000000000"]);
    let len = client.read_array_len();
    assert_eq!(len, 2);
    assert_eq!(client.read_integer(), 1); // exists
    assert_eq!(client.read_integer(), 0); // doesn't exist

    child.kill().unwrap();
}

#[test]
fn test_script_flush() {
    let (mut child, addr) = spawn_server();
    let mut client = TestClient::connect(&addr);

    // Load a script
    client.send_command(&["SCRIPT", "LOAD", "return 1"]);
    let sha1 = client.read_bulk_string().unwrap();

    // Verify it exists
    client.send_command(&["SCRIPT", "EXISTS", &sha1]);
    let _ = client.read_array_len();
    assert_eq!(client.read_integer(), 1);

    // SCRIPT FLUSH
    client.send_command(&["SCRIPT", "FLUSH"]);
    let ok = client.read_simple_string();
    assert_eq!(ok, "OK");

    // Verify it no longer exists
    client.send_command(&["SCRIPT", "EXISTS", &sha1]);
    let _ = client.read_array_len();
    assert_eq!(client.read_integer(), 0);

    child.kill().unwrap();
}

#[test]
fn test_eval_boolean_conversion() {
    let (mut child, addr) = spawn_server();
    let mut client = TestClient::connect(&addr);

    // Redis Lua: true -> 1
    client.send_command(&["EVAL", "return true", "0"]);
    let result = client.read_integer();
    assert_eq!(result, 1);

    // Redis Lua: false -> nil
    client.send_command(&["EVAL", "return false", "0"]);
    let result = client.read_bulk_string();
    assert_eq!(result, None);

    child.kill().unwrap();
}
