use std::env;
use std::time::Instant;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

struct RespClient {
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: tokio::net::tcp::OwnedWriteHalf,
}

impl RespClient {
    async fn connect(addr: &str) -> Self {
        let stream = TcpStream::connect(addr)
            .await
            .unwrap_or_else(|e| panic!("failed to connect to {}: {}", addr, e));
        let (read_half, write_half) = stream.into_split();
        let reader = BufReader::new(read_half);
        RespClient { reader, writer: write_half }
    }

    async fn send_array(&mut self, parts: &[&str]) {
        let mut buf = String::new();
        buf.push_str(&format!("*{}\r\n", parts.len()));
        for p in parts {
            buf.push_str(&format!("${}\r\n{}\r\n", p.len(), p));
        }
        self.writer.write_all(buf.as_bytes()).await.unwrap();
    }

    async fn read_simple_line(&mut self) -> String {
        let mut line = String::new();
        self.reader.read_line(&mut line).await.unwrap();
        line
    }

    async fn auth(&mut self, password: &str) {
        self.send_array(&["AUTH", password]).await;
        let line = self.read_simple_line().await;
        if line != "+OK\r\n" {
            panic!("unexpected AUTH response: {:?}", line);
        }
    }

    async fn read_bulk(&mut self) -> Option<String> {
        let mut header = String::new();
        self.reader.read_line(&mut header).await.unwrap();

        if header == "$-1\r\n" {
            return None;
        }

        if !header.starts_with('$') {
            panic!("unexpected bulk header: {:?}", header);
        }

        let len_str = &header[1..header.len() - 2];
        let len: usize = len_str
            .parse()
            .unwrap_or_else(|e| panic!("invalid bulk length in {:?}: {}", header, e));

        let mut value = String::new();
        self.reader.read_line(&mut value).await.unwrap();

        if value.len() != len + 2 {
            panic!(
                "bulk length mismatch: expected {} bytes (plus CRLF), got {:?}",
                len, value
            );
        }

        Some(value.trim_end_matches("\r\n").to_string())
    }

    async fn set(&mut self, key: &str, value: &str) {
        self.send_array(&["SET", key, value]).await;
        let line = self.read_simple_line().await;
        if line != "+OK\r\n" {
            panic!("unexpected SET response: {:?}", line);
        }
    }

    async fn get(&mut self, key: &str) -> Option<String> {
        self.send_array(&["GET", key]).await;
        self.read_bulk().await
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().skip(1).collect();

    let addr = args
        .iter()
        .find(|a| a.starts_with("--addr="))
        .map(|a| a[7..].to_string())
        .unwrap_or_else(|| "127.0.0.1:6379".to_string());

    let iterations: usize = args
        .iter()
        .find(|a| a.starts_with("--iterations="))
        .map(|a| a[13..].to_string())
        .as_deref()
        .unwrap_or("1000")
        .parse()
        .unwrap_or(1000);

    let auth_password = args
        .iter()
        .find(|a| a.starts_with("--auth="))
        .map(|a| a[7..].to_string());

    println!("[bench] target addr = {}", addr);
    println!("[bench] iterations = {}", iterations);
    if let Some(_) = auth_password.as_ref() {
        println!("[bench] using AUTH");
    }

    let mut client = RespClient::connect(&addr).await;

    if let Some(password) = auth_password.as_ref() {
        client.auth(password).await;
    }

    // 先做一次功能校验，确保两端语义一致
    client.set("bench_foo", "bar").await;
    let v = client.get("bench_foo").await;
    assert_eq!(v.as_deref(), Some("bar"));

    // SET/GET 回环性能测试
    let start = Instant::now();
    for i in 0..iterations {
        let key = "bench_key"; // 固定 key，更接近常见热点场景
        let value = format!("value-{}", i);
        client.set(key, &value).await;
        let got = client.get(key).await;
        if got.as_deref() != Some(&value) {
            panic!(
                "mismatched value at iter {}: expected {:?}, got {:?}",
                i, value, got
            );
        }
    }
    let elapsed = start.elapsed();

    let total_ops = (iterations * 2) as f64; // 每轮一个 SET + 一个 GET
    let secs = elapsed.as_secs_f64();
    let qps = if secs > 0.0 { total_ops / secs } else { 0.0 };

    println!("[bench] total time: {:?}", elapsed);
    println!("[bench] total ops: {}", total_ops as u64);
    println!("[bench] ops/sec: {:.2}", qps);
}
