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

    async fn ping(&mut self) {
        self.send_array(&["PING"]).await;
        let line = self.read_simple_line().await;
        if line != "+PONG\r\n" {
            panic!("unexpected PING response: {:?}", line);
        }
    }

    async fn mset(&mut self, pairs: &[(&str, &str)]) {
        let mut parts = Vec::with_capacity(1 + pairs.len() * 2);
        parts.push("MSET");
        for (k, v) in pairs {
            parts.push(k);
            parts.push(v);
        }
        self.send_array(&parts).await;
        let line = self.read_simple_line().await;
        if line != "+OK\r\n" {
            panic!("unexpected MSET response: {:?}", line);
        }
    }

    async fn mget(&mut self, keys: &[&str]) -> Vec<Option<String>> {
        let mut parts = Vec::with_capacity(1 + keys.len());
        parts.push("MGET");
        parts.extend_from_slice(keys);
        self.send_array(&parts).await;

        let mut header = String::new();
        self.reader.read_line(&mut header).await.unwrap();
        if !header.starts_with('*') || !header.ends_with("\r\n") {
            panic!("unexpected MGET header: {:?}", header);
        }
        let len_str = &header[1..header.len() - 2];
        let len: usize = len_str
            .parse()
            .unwrap_or_else(|e| panic!("invalid MGET array len {:?}: {}", header, e));

        let mut items = Vec::with_capacity(len);
        for _ in 0..len {
            let mut bulk_header = String::new();
            self.reader.read_line(&mut bulk_header).await.unwrap();
            if bulk_header == "$-1\r\n" {
                items.push(None);
                continue;
            }
            if !bulk_header.starts_with('$') || !bulk_header.ends_with("\r\n") {
                panic!("unexpected MGET bulk header: {:?}", bulk_header);
            }
            let len_str = &bulk_header[1..bulk_header.len() - 2];
            let bulk_len: usize = len_str
                .parse()
                .unwrap_or_else(|e| panic!("invalid MGET bulk len {:?}: {}", bulk_header, e));

            let mut value = String::new();
            self.reader.read_line(&mut value).await.unwrap();
            if value.len() != bulk_len + 2 {
                panic!(
                    "MGET bulk length mismatch: expected {}+CRLF, got {:?}",
                    bulk_len, value
                );
            }
            items.push(Some(value.trim_end_matches("\r\n").to_string()));
        }

        items
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

    async fn sunion(&mut self, keys: &[&str]) -> Vec<String> {
        let mut parts = Vec::with_capacity(1 + keys.len());
        parts.push("SUNION");
        parts.extend_from_slice(keys);
        self.send_array(&parts).await;

        // 读取 array header
        let mut header = String::new();
        self.reader.read_line(&mut header).await.unwrap();
        if !header.starts_with('*') || !header.ends_with("\r\n") {
            panic!("unexpected SUNION header: {:?}", header);
        }
        let len_str = &header[1..header.len() - 2];
        let len: usize = len_str
            .parse()
            .unwrap_or_else(|e| panic!("invalid SUNION array len {:?}: {}", header, e));

        let mut items = Vec::with_capacity(len);
        for _ in 0..len {
            if let Some(v) = self.read_bulk().await {
                items.push(v);
            } else {
                panic!("SUNION returned nil bulk inside array");
            }
        }
        items
    }

    async fn sinter(&mut self, keys: &[&str]) -> Vec<String> {
        let mut parts = Vec::with_capacity(1 + keys.len());
        parts.push("SINTER");
        parts.extend_from_slice(keys);
        self.send_array(&parts).await;

        // 读取 array header
        let mut header = String::new();
        self.reader.read_line(&mut header).await.unwrap();
        if !header.starts_with('*') || !header.ends_with("\r\n") {
            panic!("unexpected SINTER header: {:?}", header);
        }
        let len_str = &header[1..header.len() - 2];
        let len: usize = len_str
            .parse()
            .unwrap_or_else(|e| panic!("invalid SINTER array len {:?}: {}", header, e));

        let mut items = Vec::with_capacity(len);
        for _ in 0..len {
            if let Some(v) = self.read_bulk().await {
                items.push(v);
            } else {
                panic!("SINTER returned nil bulk inside array");
            }
        }
        items
    }

    async fn sdiff(&mut self, keys: &[&str]) -> Vec<String> {
        let mut parts = Vec::with_capacity(1 + keys.len());
        parts.push("SDIFF");
        parts.extend_from_slice(keys);
        self.send_array(&parts).await;

        // 读取 array header
        let mut header = String::new();
        self.reader.read_line(&mut header).await.unwrap();
        if !header.starts_with('*') || !header.ends_with("\r\n") {
            panic!("unexpected SDIFF header: {:?}", header);
        }
        let len_str = &header[1..header.len() - 2];
        let len: usize = len_str
            .parse()
            .unwrap_or_else(|e| panic!("invalid SDIFF array len {:?}: {}", header, e));

        let mut items = Vec::with_capacity(len);
        for _ in 0..len {
            if let Some(v) = self.read_bulk().await {
                items.push(v);
            } else {
                panic!("SDIFF returned nil bulk inside array");
            }
        }
        items
    }
}

async fn run_group(client: &mut RespClient, group: &str, iterations: usize) {
    match group {
        "ping" => {
            // PING 语义校验
            client.ping().await;

            let start = std::time::Instant::now();
            for _ in 0..iterations {
                client.ping().await;
            }
            let elapsed = start.elapsed();
            let total_ops = iterations as f64;
            let secs = elapsed.as_secs_f64();
            let qps = if secs > 0.0 { total_ops / secs } else { 0.0 };

            println!("[bench][ping] total time: {:?}", elapsed);
            println!("[bench][ping] total ops: {}", total_ops as u64);
            println!("[bench][ping] ops/sec: {:.2}", qps);
        }
        "mset_mget" => {
            // 功能校验：MSET/MGET
            let pairs = [("bench_m1", "v1"), ("bench_m2", "v2")];
            client.mset(&pairs).await;
            let values = client.mget(&["bench_m1", "bench_m2", "bench_missing"]).await;
            if values.len() != 3
                || values[0].as_deref() != Some("v1")
                || values[1].as_deref() != Some("v2")
                || values[2].is_some()
            {
                panic!("unexpected MSET/MGET values: {:?}", values);
            }

            // 性能测试：固定大小批量 MSET/MGET
            let keys: Vec<String> = (0..10)
                .map(|i| format!("bench_mk:{}", i))
                .collect();
            let key_refs: Vec<&str> = keys.iter().map(|k| k.as_str()).collect();

            let start = std::time::Instant::now();
            for i in 0..iterations {
                // 构造一次性 MSET 命令：MSET k0 v0 k1 v1 ...
                let mut raw: Vec<String> = Vec::with_capacity(1 + key_refs.len() * 2);
                raw.push("MSET".to_string());
                for (idx, k) in key_refs.iter().enumerate() {
                    let v = format!("v{}:{}", i, idx);
                    raw.push((*k).to_string());
                    raw.push(v);
                }
                let parts: Vec<&str> = raw.iter().map(|s| s.as_str()).collect();
                client.send_array(&parts).await;
                let line = client.read_simple_line().await;
                if line != "+OK\r\n" {
                    panic!("unexpected MSET response in loop: {:?}", line);
                }

                let _ = client.mget(&key_refs).await;
            }
            let elapsed = start.elapsed();

            let total_ops = (iterations * 2) as f64; // 每轮一 MSET + 一 MGET
            let secs = elapsed.as_secs_f64();
            let qps = if secs > 0.0 { total_ops / secs } else { 0.0 };

            println!("[bench][mset_mget] total time: {:?}", elapsed);
            println!("[bench][mset_mget] total ops: {}", total_ops as u64);
            println!("[bench][mset_mget] ops/sec: {:.2}", qps);
        }
        "hash_ops" => {
            // 功能校验：HSET/HGET/HGETALL/HDEL
            let key = "bench_hash";
            client.del(&[key]).await;
            client
                .send_array(&["HSET", key, "field", "value"])
                .await;
            let _ = client.read_simple_line().await; // :1

            client
                .send_array(&["HSET", key, "field", "value2"])
                .await;
            let _ = client.read_simple_line().await; // :0

            client
                .send_array(&["HGET", key, "field"])
                .await;
            let v = client.read_bulk().await;
            if v.as_deref() != Some("value2") {
                panic!("unexpected HGET value: {:?}", v);
            }

            client
                .send_array(&["HGETALL", key])
                .await;
            let mut header = String::new();
            client.reader.read_line(&mut header).await.unwrap();
            if header.trim_end() != "*2" {
                panic!("unexpected HGETALL header: {:?}", header);
            }
            let f = client.read_bulk().await;
            let v2 = client.read_bulk().await;
            if f.as_deref() != Some("field") || v2.as_deref() != Some("value2") {
                panic!("unexpected HGETALL pair: {:?} {:?}", f, v2);
            }

            client
                .send_array(&["HDEL", key, "field"])
                .await;
            let del_line = client.read_simple_line().await;
            if del_line.trim_end() != ":1" {
                panic!("unexpected HDEL response: {:?}", del_line);
            }

            // 性能测试：同一个 Hash 上循环 HSET/HGET
            let key = "bench_hash_loop";
            client.del(&[key]).await;

            let start = std::time::Instant::now();
            for i in 0..iterations {
                let field = format!("f{}", i % 16);
                let value = format!("v{}", i);
                client
                    .send_array(&["HSET", key, &field, &value])
                    .await;
                let _ = client.read_simple_line().await;

                client
                    .send_array(&["HGET", key, &field])
                    .await;
                let got = client.read_bulk().await;
                if got.as_deref() != Some(value.as_str()) {
                    panic!(
                        "unexpected HGET in loop: expected {:?}, got {:?}",
                        value, got
                    );
                }
            }
            let elapsed = start.elapsed();

            let total_ops = (iterations * 2) as f64; // 每轮 HSET + HGET
            let secs = elapsed.as_secs_f64();
            let qps = if secs > 0.0 { total_ops / secs } else { 0.0 };

            println!("[bench][hash_ops] total time: {:?}", elapsed);
            println!("[bench][hash_ops] total ops: {}", total_ops as u64);
            println!("[bench][hash_ops] ops/sec: {:.2}", qps);
        }
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
        "set_union" => {
            // 功能校验：SUNION 行为
            client.del(&["bench_su_1", "bench_su_2"]).await;
            let added1 = client
                .send_array(&["SADD", "bench_su_1", "a", "b"])
                .await;
            let _ = added1; // 我们不检查返回值，这里只依赖服务器自身行为
            // 读取 :2
            let _ = client.read_simple_line().await;

            client
                .send_array(&["SADD", "bench_su_2", "b", "c"])
                .await;
            let _ = client.read_simple_line().await; // :2

            let mut items = client.sunion(&["bench_su_1", "bench_su_2", "missing"]).await;
            items.sort();
            if items != vec!["a".to_string(), "b".to_string(), "c".to_string()] {
                panic!("unexpected SUNION items: {:?}", items);
            }

            // SUNION 回环性能测试：在两个集合上交替添加元素并做 SUNION
            let base1 = "bench_su_loop_1";
            let base2 = "bench_su_loop_2";
            client.del(&[base1, base2]).await;
            client
                .send_array(&["SADD", base1, "a"])
                .await;
            let _ = client.read_simple_line().await;
            client
                .send_array(&["SADD", base2, "b"])
                .await;
            let _ = client.read_simple_line().await;

            let start = std::time::Instant::now();
            for i in 0..iterations {
                let v1 = format!("x{}", i);
                let v2 = format!("y{}", i);
                client
                    .send_array(&["SADD", base1, &v1])
                    .await;
                let _ = client.read_simple_line().await;
                client
                    .send_array(&["SADD", base2, &v2])
                    .await;
                let _ = client.read_simple_line().await;
                let _ = client.sunion(&[base1, base2]).await;
            }
            let elapsed = start.elapsed();

            let total_ops = (iterations * 3) as f64; // 每轮 两次 SADD + 一次 SUNION
            let secs = elapsed.as_secs_f64();
            let qps = if secs > 0.0 { total_ops / secs } else { 0.0 };

            println!("[bench] total time: {:?}", elapsed);
            println!("[bench] total ops: {}", total_ops as u64);
            println!("[bench] ops/sec: {:.2}", qps);
        }
        "set_intersection" => {
            // 功能校验：SINTER 行为
            client.del(&["bench_si_1", "bench_si_2", "bench_si_3"]).await;

            client
                .send_array(&["SADD", "bench_si_1", "a", "b"])
                .await;
            let _ = client.read_simple_line().await; // :2

            client
                .send_array(&["SADD", "bench_si_2", "b", "c"])
                .await;
            let _ = client.read_simple_line().await; // :2

            client
                .send_array(&["SADD", "bench_si_3", "b", "d"])
                .await;
            let _ = client.read_simple_line().await; // :2

            let mut items = client.sinter(&["bench_si_1", "bench_si_2", "bench_si_3"]).await;
            items.sort();
            if items != vec!["b".to_string()] {
                panic!("unexpected SINTER items: {:?}", items);
            }

            // SINTER 回环性能测试：在三个集合上交替添加元素并做 SINTER
            let base1 = "bench_si_loop_1";
            let base2 = "bench_si_loop_2";
            let base3 = "bench_si_loop_3";
            client.del(&[base1, base2, base3]).await;

            client
                .send_array(&["SADD", base1, "x"])
                .await;
            let _ = client.read_simple_line().await;
            client
                .send_array(&["SADD", base2, "x"])
                .await;
            let _ = client.read_simple_line().await;
            client
                .send_array(&["SADD", base3, "x"])
                .await;
            let _ = client.read_simple_line().await;

            let start = std::time::Instant::now();
            for i in 0..iterations {
                let v1 = format!("i{}", i);
                let v2 = format!("j{}", i);
                let v3 = format!("k{}", i);

                client
                    .send_array(&["SADD", base1, &v1])
                    .await;
                let _ = client.read_simple_line().await;
                client
                    .send_array(&["SADD", base2, &v2])
                    .await;
                let _ = client.read_simple_line().await;
                client
                    .send_array(&["SADD", base3, &v3])
                    .await;
                let _ = client.read_simple_line().await;

                let _ = client.sinter(&[base1, base2, base3]).await;
            }
            let elapsed = start.elapsed();

            let total_ops = (iterations * 4) as f64; // 每轮三次 SADD + 一次 SINTER
            let secs = elapsed.as_secs_f64();
            let qps = if secs > 0.0 { total_ops / secs } else { 0.0 };

            println!("[bench] total time: {:?}", elapsed);
            println!("[bench] total ops: {}", total_ops as u64);
            println!("[bench] ops/sec: {:.2}", qps);
        }
        "set_difference" => {
            // 功能校验：SDIFF 行为
            client.del(&["bench_sd_1", "bench_sd_2"]).await;

            client
                .send_array(&["SADD", "bench_sd_1", "a", "b", "c"])
                .await;
            let _ = client.read_simple_line().await; // :3

            client
                .send_array(&["SADD", "bench_sd_2", "b", "d"])
                .await;
            let _ = client.read_simple_line().await; // :2

            let mut items = client.sdiff(&["bench_sd_1", "bench_sd_2"]).await;
            items.sort();
            if items != vec!["a".to_string(), "c".to_string()] {
                panic!("unexpected SDIFF items: {:?}", items);
            }

            // SDIFF 回环性能测试：不断往第一个集合追加元素，并与第二个集合做差集
            let base1 = "bench_sd_loop_1";
            let base2 = "bench_sd_loop_2";
            client.del(&[base1, base2]).await;

            client
                .send_array(&["SADD", base1, "x", "y", "z"])
                .await;
            let _ = client.read_simple_line().await;
            client
                .send_array(&["SADD", base2, "y"])
                .await;
            let _ = client.read_simple_line().await;

            let start = std::time::Instant::now();
            for i in 0..iterations {
                let v = format!("v{}", i);

                client
                    .send_array(&["SADD", base1, &v])
                    .await;
                let _ = client.read_simple_line().await;

                let _ = client.sdiff(&[base1, base2]).await;
            }
            let elapsed = start.elapsed();

            let total_ops = (iterations * 2) as f64; // 每轮一次 SADD + 一次 SDIFF
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
        for g in [
            "ping",
            "mset_mget",
            "hash_ops",
            "set_get",
            "incr_decr",
            "exists_del",
            "list_ops",
            "list_pops",
            "set_union",
            "set_intersection",
            "set_difference",
        ] {
            println!("[bench] ==== group: {} ====", g);
            run_group(&mut client, g, iterations).await;
        }
    } else {
        run_group(&mut client, &group, iterations).await;
    }
}
