use std::env;

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

    async fn incr(&mut self, key: &str) -> i64 {
        self.send_array(&["INCR", key]).await;
        let line = self.read_simple_line().await;
        if !line.starts_with(':') || !line.ends_with("\r\n") {
            panic!("unexpected INCR response: {:?}", line);
        }
        let num_str = &line[1..line.len() - 2];
        num_str
            .parse::<i64>()
            .unwrap_or_else(|e| panic!("invalid INCR integer {:?}: {}", line, e))
    }

    async fn decr(&mut self, key: &str) -> i64 {
        self.send_array(&["DECR", key]).await;
        let line = self.read_simple_line().await;
        if !line.starts_with(':') || !line.ends_with("\r\n") {
            panic!("unexpected DECR response: {:?}", line);
        }
        let num_str = &line[1..line.len() - 2];
        num_str
            .parse::<i64>()
            .unwrap_or_else(|e| panic!("invalid DECR integer {:?}: {}", line, e))
    }

    async fn exists(&mut self, keys: &[&str]) -> i64 {
        let mut parts = Vec::with_capacity(1 + keys.len());
        parts.push("EXISTS");
        parts.extend_from_slice(keys);
        self.send_array(&parts).await;
        let line = self.read_simple_line().await;
        if !line.starts_with(':') || !line.ends_with("\r\n") {
            panic!("unexpected EXISTS response: {:?}", line);
        }
        let num_str = &line[1..line.len() - 2];
        num_str
            .parse::<i64>()
            .unwrap_or_else(|e| panic!("invalid EXISTS integer {:?}: {}", line, e))
    }

    async fn del(&mut self, keys: &[&str]) -> i64 {
        let mut parts = Vec::with_capacity(1 + keys.len());
        parts.push("DEL");
        parts.extend_from_slice(keys);
        self.send_array(&parts).await;
        let line = self.read_simple_line().await;
        if !line.starts_with(':') || !line.ends_with("\r\n") {
            panic!("unexpected DEL response: {:?}", line);
        }
        let num_str = &line[1..line.len() - 2];
        num_str
            .parse::<i64>()
            .unwrap_or_else(|e| panic!("invalid DEL integer {:?}: {}", line, e))
    }

    async fn lpush(&mut self, key: &str, values: &[&str]) -> i64 {
        let mut parts = Vec::with_capacity(2 + values.len());
        parts.push("LPUSH");
        parts.push(key);
        parts.extend_from_slice(values);
        self.send_array(&parts).await;
        let line = self.read_simple_line().await;
        if !line.starts_with(':') || !line.ends_with("\r\n") {
            panic!("unexpected LPUSH response: {:?}", line);
        }
        let num_str = &line[1..line.len() - 2];
        num_str
            .parse::<i64>()
            .unwrap_or_else(|e| panic!("invalid LPUSH integer {:?}: {}", line, e))
    }

    async fn rpush(&mut self, key: &str, values: &[&str]) -> i64 {
        let mut parts = Vec::with_capacity(2 + values.len());
        parts.push("RPUSH");
        parts.push(key);
        parts.extend_from_slice(values);
        self.send_array(&parts).await;
        let line = self.read_simple_line().await;
        if !line.starts_with(':') || !line.ends_with("\r\n") {
            panic!("unexpected RPUSH response: {:?}", line);
        }
        let num_str = &line[1..line.len() - 2];
        num_str
            .parse::<i64>()
            .unwrap_or_else(|e| panic!("invalid RPUSH integer {:?}: {}", line, e))
    }

    async fn lrange(&mut self, key: &str, start: isize, stop: isize) -> Vec<String> {
        self.send_array(&["LRANGE", key, &start.to_string(), &stop.to_string()])
            .await;

        // 读取 array header
        let mut header = String::new();
        self.reader.read_line(&mut header).await.unwrap();
        if !header.starts_with('*') || !header.ends_with("\r\n") {
            panic!("unexpected LRANGE header: {:?}", header);
        }
        let len_str = &header[1..header.len() - 2];
        let len: usize = len_str
            .parse()
            .unwrap_or_else(|e| panic!("invalid LRANGE array len {:?}: {}", header, e));

        let mut items = Vec::with_capacity(len);
        for _ in 0..len {
            if let Some(v) = self.read_bulk().await {
                items.push(v);
            } else {
                panic!("LRANGE returned nil bulk inside array");
            }
        }
        items
    }

    async fn lpop(&mut self, key: &str) -> Option<String> {
        self.send_array(&["LPOP", key]).await;
        self.read_bulk().await
    }

    async fn rpop(&mut self, key: &str) -> Option<String> {
        self.send_array(&["RPOP", key]).await;
        self.read_bulk().await
    }
}

async fn run_group(client: &mut RespClient, group: &str, iterations: usize) {
    match group {
        "set_get" => {
            // 先做一次功能校验，确保两端语义一致
            client.set("bench_foo", "bar").await;
            let v = client.get("bench_foo").await;
            assert_eq!(v.as_deref(), Some("bar"));

            // SET/GET 回环性能测试
            let start = std::time::Instant::now();
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
        "incr_decr" => {
            // 功能校验：INCR/DECR 在两端行为一致
            let key = "bench_counter";
            let v1 = client.incr(key).await;
            let v2 = client.incr(key).await;
            let v3 = client.decr(key).await;
            if v2 != v1 + 1 || v3 != v2 - 1 {
                panic!(
                    "unexpected INCR/DECR sequence: v1={}, v2={}, v3={}",
                    v1, v2, v3
                );
            }

            // INCR/DECR 回环性能测试
            let start = std::time::Instant::now();
            for _ in 0..iterations {
                let _ = client.incr(key).await;
                let _ = client.decr(key).await;
            }
            let elapsed = start.elapsed();

            let total_ops = (iterations * 2) as f64; // 每轮一个 INCR + 一个 DECR
            let secs = elapsed.as_secs_f64();
            let qps = if secs > 0.0 { total_ops / secs } else { 0.0 };

            println!("[bench] total time: {:?}", elapsed);
            println!("[bench] total ops: {}", total_ops as u64);
            println!("[bench] ops/sec: {:.2}", qps);
        }
        "exists_del" => {
            // 功能校验：EXISTS/DEL 在两端行为一致
            let k1 = "bench_exists_1";
            let k2 = "bench_exists_2";
            client.set(k1, "v1").await;
            client.set(k2, "v2").await;
            let e = client.exists(&[k1, k2, "bench_missing"]).await;
            if e != 2 {
                panic!("unexpected EXISTS count: {}", e);
            }
            let removed = client.del(&[k1, k2, "bench_missing"]).await;
            if removed != 2 {
                panic!("unexpected DEL removed: {}", removed);
            }

            // EXISTS/DEL 回环性能测试：不断创建和删除一批 key
            let base = "bench_exists_key";
            let start = std::time::Instant::now();
            for i in 0..iterations {
                let k = format!("{}:{}", base, i);
                client.set(&k, "x").await;
                let c = client.exists(&[&k]).await;
                if c != 1 {
                    panic!("EXISTS should be 1 for key {} but was {}", k, c);
                }
                let d = client.del(&[&k]).await;
                if d != 1 {
                    panic!("DEL should remove 1 for key {} but was {}", k, d);
                }
            }
            let elapsed = start.elapsed();

            let total_ops = (iterations * 3) as f64; // 每轮 SET + EXISTS + DEL
            let secs = elapsed.as_secs_f64();
            let qps = if secs > 0.0 { total_ops / secs } else { 0.0 };

            println!("[bench] total time: {:?}", elapsed);
            println!("[bench] total ops: {}", total_ops as u64);
            println!("[bench] ops/sec: {:.2}", qps);
        }
        "list_ops" => {
            // 功能校验：LPUSH/RPUSH/LRANGE
            let key = "bench_list";
            client.del(&[key]).await;
            let len = client.rpush(key, &["a", "b", "c"]).await;
            if len != 3 {
                panic!("unexpected RPUSH length: {}", len);
            }
            let len2 = client.lpush(key, &["x"]).await;
            if len2 != 4 {
                panic!("unexpected LPUSH length: {}", len2);
            }
            let items = client.lrange(key, 0, -1).await;
            if items != vec!["x", "a", "b", "c"] {
                panic!("unexpected LRANGE items: {:?}", items);
            }

            // 列表操作回环性能测试：交替 LPUSH/RPUSH + LRANGE
            let start = std::time::Instant::now();
            for i in 0..iterations {
                let v = format!("v{}", i);
                let _ = client.lpush(key, &[&v]).await;
                let _ = client.rpush(key, &[&v]).await;
                let _ = client.lrange(key, 0, 10).await;
            }
            let elapsed = start.elapsed();

            let total_ops = (iterations * 3) as f64; // 每轮 LPUSH + RPUSH + LRANGE
            let secs = elapsed.as_secs_f64();
            let qps = if secs > 0.0 { total_ops / secs } else { 0.0 };

            println!("[bench] total time: {:?}", elapsed);
            println!("[bench] total ops: {}", total_ops as u64);
            println!("[bench] ops/sec: {:.2}", qps);
        }
        "list_pops" => {
            // 功能校验：LPOP/RPOP 行为
            let key = "bench_list_pop";
            client.del(&[key]).await;
            let len = client.rpush(key, &["a", "b", "c"]).await;
            if len != 3 {
                panic!("unexpected RPUSH length in list_pops: {}", len);
            }
            let v1 = client.lpop(key).await;
            let v2 = client.rpop(key).await;
            let v3 = client.rpop(key).await;
            let v4 = client.rpop(key).await;
            if v1.as_deref() != Some("a")
                || v2.as_deref() != Some("c")
                || v3.as_deref() != Some("b")
                || v4.is_some()
            {
                panic!(
                    "unexpected LPOP/RPOP sequence: v1={:?}, v2={:?}, v3={:?}, v4={:?}",
                    v1, v2, v3, v4
                );
            }

            // LPOP/RPOP 回环性能测试：在一个有限长度的列表上进行弹入+弹出组合
            let key = "bench_list_pop_loop";
            client.del(&[key]).await;
            client.rpush(key, &["x"]).await; // 保证列表非空

            let start = std::time::Instant::now();
            for i in 0..iterations {
                let v = format!("v{}", i);
                let _ = client.lpush(key, &[&v]).await;
                let _ = client.rpop(key).await;
            }
            let elapsed = start.elapsed();

            let total_ops = (iterations * 2) as f64; // 每轮 LPUSH + RPOP
            let secs = elapsed.as_secs_f64();
            let qps = if secs > 0.0 { total_ops / secs } else { 0.0 };

            println!("[bench] total time: {:?}", elapsed);
            println!("[bench] total ops: {}", total_ops as u64);
            println!("[bench] ops/sec: {:.2}", qps);
        }
        other => {
            panic!("unknown bench group: {}", other);
        }
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

    let group = args
        .iter()
        .find(|a| a.starts_with("--group="))
        .map(|a| a[8..].to_string())
        .unwrap_or_else(|| "all".to_string());

    let auth_password = args
        .iter()
        .find(|a| a.starts_with("--auth="))
        .map(|a| a[7..].to_string());

    println!("[bench] target addr = {}", addr);
    println!("[bench] iterations = {}", iterations);
    println!("[bench] group = {}", group);
    if let Some(_) = auth_password.as_ref() {
        println!("[bench] using AUTH");
    }

    let mut client = RespClient::connect(&addr).await;

    if let Some(password) = auth_password.as_ref() {
        client.auth(password).await;
    }

    if group == "all" {
        for g in ["set_get", "incr_decr", "exists_del", "list_ops", "list_pops"] {
            println!("[bench] ==== group: {} ====", g);
            run_group(&mut client, g, iterations).await;
        }
    } else {
        run_group(&mut client, &group, iterations).await;
    }
}
