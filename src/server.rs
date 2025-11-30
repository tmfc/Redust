use std::future::Future;
use std::sync::{Arc, atomic::{AtomicU64, AtomicUsize, Ordering}};
use std::time::Instant;
use std::env;

use tokio::io::{self, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::sleep;

use crate::command::{read_command, Command, CommandError}; // Import CommandError
use crate::resp::{
    respond_bulk_string,
    respond_simple_string,
    respond_error,
    respond_integer,
    respond_null_bulk,
};
use crate::storage::Storage;

struct Metrics {
    start_time: Instant,
    connected_clients: AtomicUsize,
    total_commands: AtomicU64,
    tcp_port: u16,
}

fn parse_maxmemory_bytes(input: &str) -> Option<u64> {
    let s = input.trim().to_lowercase();
    if s.is_empty() {
        return None;
    }

    // 纯数字：按字节解析
    if let Ok(v) = s.parse::<u64>() {
        return Some(v);
    }

    // 支持带单位：MB / GB（大小写不敏感）
    let (num_part, multiplier) = if let Some(stripped) = s.strip_suffix("mb") {
        (stripped.trim(), 1024u64 * 1024)
    } else if let Some(stripped) = s.strip_suffix("gb") {
        (stripped.trim(), 1024u64 * 1024 * 1024)
    } else {
        return None;
    };

    let base = num_part.parse::<u64>().ok()?;
    base.checked_mul(multiplier)
}

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;

    if bytes >= GB {
        let v = bytes as f64 / GB as f64;
        return format!("{:.2}GB", v);
    }
    if bytes >= MB {
        let v = bytes as f64 / MB as f64;
        return format!("{:.2}MB", v);
    }
    if bytes >= KB {
        let v = bytes as f64 / KB as f64;
        return format!("{:.2}KB", v);
    }
    format!("{}B", bytes)
}

fn build_prometheus_metrics(storage: &Storage, metrics: &Metrics) -> String {
    let uptime = Instant::now().duration_since(metrics.start_time).as_secs();
    let connected = metrics.connected_clients.load(Ordering::Relaxed);
    let total_cmds = metrics.total_commands.load(Ordering::Relaxed);
    let keys = storage.keys("*").len();

    let mut buf = String::new();

    buf.push_str("# TYPE redust_uptime_seconds counter\n");
    buf.push_str(&format!("redust_uptime_seconds {}\n", uptime));

    buf.push_str("# TYPE redust_connected_clients gauge\n");
    buf.push_str(&format!("redust_connected_clients {}\n", connected));

    buf.push_str("# TYPE redust_total_commands_processed counter\n");
    buf.push_str(&format!("redust_total_commands_processed {}\n", total_cmds));

    buf.push_str("# TYPE redust_keyspace_keys gauge\n");
    buf.push_str(&format!("redust_keyspace_keys{{db=\"0\"}} {}\n", keys));

    buf
}

fn prefix_key(db: u8, key: &str) -> String {
    format!("{}:{}", db, key)
}

async fn handle_string_command(
    cmd: Command,
    storage: &Storage,
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    current_db: u8,
) -> io::Result<()> {
    match cmd {
        Command::Ping => {
            respond_simple_string(writer, "PONG").await?;
        }
        Command::Echo(value) => {
            respond_bulk_string(writer, &value).await?;
        }
        Command::Quit => {
            respond_simple_string(writer, "OK").await?;
        }
        Command::Set { key, value, expire_millis } => {
            let physical = prefix_key(current_db, &key);
            storage.set(physical.clone(), value);
            if let Some(ms) = expire_millis {
                storage.expire_millis(&physical, ms);
            }
            respond_simple_string(writer, "OK").await?;
        }
        Command::Get { key } => {
            let physical = prefix_key(current_db, &key);
            if let Some(value) = storage.get(&physical) {
                respond_bulk_string(writer, &value).await?;
            } else {
                respond_null_bulk(writer).await?;
            }
        }
        Command::Incr { key } => {
            let physical = prefix_key(current_db, &key);
            match storage.incr(&physical) {
                Ok(value) => {
                    respond_integer(writer, value).await?;
                }
                Err(_) => {
                    respond_error(writer, "ERR value is not an integer or out of range").await?;
                }
            }
        }
        Command::Decr { key } => {
            let physical = prefix_key(current_db, &key);
            match storage.decr(&physical) {
                Ok(value) => {
                    respond_integer(writer, value).await?;
                }
                Err(_) => {
                    respond_error(writer, "ERR value is not an integer or out of range").await?;
                }
            }
        }
        Command::Incrby { key, delta } => {
            let physical = prefix_key(current_db, &key);
            match storage.incr_by(&physical, delta) {
                Ok(value) => {
                    respond_integer(writer, value).await?;
                }
                Err(_) => {
                    respond_error(writer, "ERR value is not an integer or out of range").await?;
                }
            }
        }
        Command::Decrby { key, delta } => {
            let physical = prefix_key(current_db, &key);
            match storage.incr_by(&physical, -delta) {
                Ok(value) => {
                    respond_integer(writer, value).await?;
                }
                Err(_) => {
                    respond_error(writer, "ERR value is not an integer or out of range").await?;
                }
            }
        }
        Command::Del { keys } => {
            let physical: Vec<String> = keys.into_iter().map(|k| prefix_key(current_db, &k)).collect();
            let removed = storage.del(&physical);
            respond_integer(writer, removed as i64).await?;
        }
        Command::Exists { keys } => {
            let physical: Vec<String> = keys.into_iter().map(|k| prefix_key(current_db, &k)).collect();
            let count = storage.exists(&physical);
            respond_integer(writer, count as i64).await?;
        }
        Command::Mget { keys } => {
            let physical: Vec<String> = keys.into_iter().map(|k| prefix_key(current_db, &k)).collect();
            let values = storage.mget(&physical);
            let mut response = format!("*{}\r\n", values.len());
            for v in values {
                match v {
                    Some(s) => {
                        response.push_str(&format!("${}\r\n{}\r\n", s.len(), s));
                    }
                    None => {
                        response.push_str("$-1\r\n");
                    }
                }
            }
            writer.write_all(response.as_bytes()).await?;
        }
        Command::Mset { pairs } => {
            let mapped: Vec<(String, String)> = pairs
                .into_iter()
                .map(|(k, v)| (prefix_key(current_db, &k), v))
                .collect();
            storage.mset(&mapped);
            respond_simple_string(writer, "OK").await?;
        }
        Command::Setnx { key, value } => {
            let physical = prefix_key(current_db, &key);
            let inserted = storage.setnx(&physical, value);
            let v = if inserted { 1 } else { 0 };
            respond_integer(writer, v).await?;
        }
        Command::Setex { key, seconds, value } => {
            let physical = prefix_key(current_db, &key);
            storage.set_with_expire_seconds(physical, value, seconds);
            respond_simple_string(writer, "OK").await?;
        }
        Command::Psetex { key, millis, value } => {
            let physical = prefix_key(current_db, &key);
            storage.set_with_expire_millis(physical, value, millis);
            respond_simple_string(writer, "OK").await?;
        }
        _ => {}
    }

    Ok(())
}

async fn handle_list_command(
    cmd: Command,
    storage: &Storage,
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    current_db: u8,
) -> io::Result<()> {
    match cmd {
        Command::Lpush { key, values } => {
            let physical = prefix_key(current_db, &key);
            match storage.lpush(&physical, &values) {
                Ok(len) => {
                    respond_integer(writer, len as i64).await?;
                }
                Err(()) => {
                    respond_error(writer, "WRONGTYPE Operation against a key holding the wrong kind of value").await?;
                }
            }
        }
        Command::Rpush { key, values } => {
            let physical = prefix_key(current_db, &key);
            match storage.rpush(&physical, &values) {
                Ok(len) => {
                    respond_integer(writer, len as i64).await?;
                }
                Err(()) => {
                    respond_error(writer, "WRONGTYPE Operation against a key holding the wrong kind of value").await?;
                }
            }
        }
        Command::Lrange { key, start, stop } => {
            let physical = prefix_key(current_db, &key);
            match storage.lrange(&physical, start, stop) {
                Ok(items) => {
                    let mut response = format!("*{}\r\n", items.len());
                    for item in items {
                        response.push_str(&format!("${}\r\n{}\r\n", item.len(), item));
                    }
                    writer.write_all(response.as_bytes()).await?;
                }
                Err(()) => {
                    respond_error(writer, "WRONGTYPE Operation against a key holding the wrong kind of value").await?;
                }
            }
        }
        Command::Lpop { key } => {
            let physical = prefix_key(current_db, &key);
            match storage.lpop(&physical) {
                Ok(Some(value)) => {
                    respond_bulk_string(writer, &value).await?;
                }
                Ok(None) => {
                    respond_null_bulk(writer).await?;
                }
                Err(()) => {
                    respond_error(writer, "WRONGTYPE Operation against a key holding the wrong kind of value").await?;
                }
            }
        }
        Command::Rpop { key } => {
            let physical = prefix_key(current_db, &key);
            match storage.rpop(&physical) {
                Ok(Some(value)) => {
                    respond_bulk_string(writer, &value).await?;
                }
                Ok(None) => {
                    respond_null_bulk(writer).await?;
                }
                Err(()) => {
                    respond_error(writer, "WRONGTYPE Operation against a key holding the wrong kind of value").await?;
                }
            }
        }
        Command::Llen { key } => {
            let physical = prefix_key(current_db, &key);
            match storage.llen(&physical) {
                Ok(len) => {
                    respond_integer(writer, len as i64).await?;
                }
                Err(()) => {
                    respond_error(writer, "WRONGTYPE Operation against a key holding the wrong kind of value").await?;
                }
            }
        }
        Command::Lindex { key, index } => {
            let physical = prefix_key(current_db, &key);
            match storage.lindex(&physical, index) {
                Ok(Some(value)) => {
                    respond_bulk_string(writer, &value).await?;
                }
                Ok(None) => {
                    respond_null_bulk(writer).await?;
                }
                Err(()) => {
                    respond_error(writer, "WRONGTYPE Operation against a key holding the wrong kind of value").await?;
                }
            }
        }
        Command::Lrem { key, count, value } => {
            let physical = prefix_key(current_db, &key);
            match storage.lrem(&physical, count, &value) {
                Ok(removed) => {
                    respond_integer(writer, removed as i64).await?;
                }
                Err(()) => {
                    respond_error(writer, "WRONGTYPE Operation against a key holding the wrong kind of value").await?;
                }
            }
        }
        Command::Ltrim { key, start, stop } => {
            let physical = prefix_key(current_db, &key);
            match storage.ltrim(&physical, start, stop) {
                Ok(()) => {
                    respond_simple_string(writer, "OK").await?;
                }
                Err(()) => {
                    respond_error(writer, "WRONGTYPE Operation against a key holding the wrong kind of value").await?;
                }
            }
        }
        _ => {}
    }

    Ok(())
}

async fn handle_set_command(
    cmd: Command,
    storage: &Storage,
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    current_db: u8,
) -> io::Result<()> {
    match cmd {
        Command::Sadd { key, members } => {
            let physical = prefix_key(current_db, &key);
            match storage.sadd(&physical, &members) {
                Ok(added) => {
                    respond_integer(writer, added as i64).await?;
                }
                Err(()) => {
                    respond_error(writer, "WRONGTYPE Operation against a key holding the wrong kind of value").await?;
                }
            }
        }
        Command::Srem { key, members } => {
            let physical = prefix_key(current_db, &key);
            match storage.srem(&physical, &members) {
                Ok(removed) => {
                    respond_integer(writer, removed as i64).await?;
                }
                Err(()) => {
                    respond_error(writer, "WRONGTYPE Operation against a key holding the wrong kind of value").await?;
                }
            }
        }
        Command::Smembers { key } => {
            let physical = prefix_key(current_db, &key);
            match storage.smembers(&physical) {
                Ok(members) => {
                    let mut response = format!("*{}\r\n", members.len());
                    for m in members {
                        response.push_str(&format!("${}\r\n{}\r\n", m.len(), m));
                    }
                    writer.write_all(response.as_bytes()).await?;
                }
                Err(()) => {
                    respond_error(writer, "WRONGTYPE Operation against a key holding the wrong kind of value").await?;
                }
            }
        }
        Command::Scard { key } => {
            let physical = prefix_key(current_db, &key);
            match storage.scard(&physical) {
                Ok(card) => {
                    respond_integer(writer, card as i64).await?;
                }
                Err(()) => {
                    respond_error(writer, "WRONGTYPE Operation against a key holding the wrong kind of value").await?;
                }
            }
        }
        Command::Sismember { key, member } => {
            let physical = prefix_key(current_db, &key);
            match storage.sismember(&physical, &member) {
                Ok(is_member) => {
                    let v = if is_member { 1 } else { 0 };
                    respond_integer(writer, v).await?;
                }
                Err(()) => {
                    respond_error(writer, "WRONGTYPE Operation against a key holding the wrong kind of value").await?;
                }
            }
        }
        Command::Sunion { keys } => {
            let physical: Vec<String> = keys.into_iter().map(|k| prefix_key(current_db, &k)).collect();
            match storage.sunion(&physical) {
                Ok(members) => {
                    let mut response = format!("*{}\r\n", members.len());
                    for m in members {
                        response.push_str(&format!("${}\r\n{}\r\n", m.len(), m));
                    }
                    writer.write_all(response.as_bytes()).await?;
                }
                Err(()) => {
                    respond_error(writer, "WRONGTYPE Operation against a key holding the wrong kind of value").await?;
                }
            }
        }
        Command::Sinter { keys } => {
            let physical: Vec<String> = keys.into_iter().map(|k| prefix_key(current_db, &k)).collect();
            match storage.sinter(&physical) {
                Ok(members) => {
                    let mut response = format!("*{}\r\n", members.len());
                    for m in members {
                        response.push_str(&format!("${}\r\n{}\r\n", m.len(), m));
                    }
                    writer.write_all(response.as_bytes()).await?;
                }
                Err(()) => {
                    respond_error(writer, "WRONGTYPE Operation against a key holding the wrong kind of value").await?;
                }
            }
        }
        Command::Sdiff { keys } => {
            let physical: Vec<String> = keys.into_iter().map(|k| prefix_key(current_db, &k)).collect();
            match storage.sdiff(&physical) {
                Ok(members) => {
                    let mut response = format!("*{}\r\n", members.len());
                    for m in members {
                        response.push_str(&format!("${}\r\n{}\r\n", m.len(), m));
                    }
                    writer.write_all(response.as_bytes()).await?;
                }
                Err(()) => {
                    respond_error(writer, "WRONGTYPE Operation against a key holding the wrong kind of value").await?;
                }
            }
        }
        _ => {}
    }

    Ok(())
}

async fn handle_hash_command(
    cmd: Command,
    storage: &Storage,
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    current_db: u8,
) -> io::Result<()> {
    match cmd {
        Command::Hset { key, field, value } => {
            let physical = prefix_key(current_db, &key);
            match storage.hset(&physical, &field, value) {
                Ok(added) => {
                    respond_integer(writer, added as i64).await?;
                }
                Err(()) => {
                    respond_error(writer, "WRONGTYPE Operation against a key holding the wrong kind of value").await?;
                }
            }
        }
        Command::Hget { key, field } => {
            let physical = prefix_key(current_db, &key);
            match storage.hget(&physical, &field) {
                Ok(Some(value)) => {
                    respond_bulk_string(writer, &value).await?;
                }
                Ok(None) => {
                    respond_null_bulk(writer).await?;
                }
                Err(()) => {
                    respond_error(writer, "WRONGTYPE Operation against a key holding the wrong kind of value").await?;
                }
            }
        }
        Command::Hdel { key, fields } => {
            let physical = prefix_key(current_db, &key);
            match storage.hdel(&physical, &fields) {
                Ok(removed) => {
                    respond_integer(writer, removed as i64).await?;
                }
                Err(()) => {
                    respond_error(writer, "WRONGTYPE Operation against a key holding the wrong kind of value").await?;
                }
            }
        }
        Command::Hexists { key, field } => {
            let physical = prefix_key(current_db, &key);
            match storage.hexists(&physical, &field) {
                Ok(exists) => {
                    let v = if exists { 1 } else { 0 };
                    respond_integer(writer, v).await?;
                }
                Err(()) => {
                    respond_error(writer, "WRONGTYPE Operation against a key holding the wrong kind of value").await?;
                }
            }
        }
        Command::Hgetall { key } => {
            let physical = prefix_key(current_db, &key);
            match storage.hgetall(&physical) {
                Ok(entries) => {
                    let mut response = format!("*{}\r\n", entries.len() * 2);
                    for (field, value) in entries {
                        response.push_str(&format!("${}\r\n{}\r\n", field.len(), field));
                        response.push_str(&format!("${}\r\n{}\r\n", value.len(), value));
                    }
                    writer.write_all(response.as_bytes()).await?;
                }
                Err(()) => {
                    respond_error(writer, "WRONGTYPE Operation against a key holding the wrong kind of value").await?;
                }
            }
        }
        _ => {}
    }

    Ok(())
}

async fn handle_key_meta_command(
    cmd: Command,
    storage: &Storage,
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    current_db: u8,
) -> io::Result<()> {
    match cmd {
        Command::Expire { key, seconds } => {
            let physical = prefix_key(current_db, &key);
            let res = storage.expire_seconds(&physical, seconds);
            let v = if res { 1 } else { 0 };
            respond_integer(writer, v).await?;
        }
        Command::Pexpire { key, millis } => {
            let physical = prefix_key(current_db, &key);
            let res = storage.expire_millis(&physical, millis);
            let v = if res { 1 } else { 0 };
            respond_integer(writer, v).await?;
        }
        Command::Ttl { key } => {
            let physical = prefix_key(current_db, &key);
            let ttl = storage.ttl_seconds(&physical);
            respond_integer(writer, ttl).await?;
        }
        Command::Pttl { key } => {
            let physical = prefix_key(current_db, &key);
            let ttl = storage.pttl_millis(&physical);
            respond_integer(writer, ttl).await?;
        }
        Command::Persist { key } => {
            let physical = prefix_key(current_db, &key);
            let changed = storage.persist(&physical);
            let v = if changed { 1 } else { 0 };
            respond_integer(writer, v).await?;
        }
        Command::Type { key } => {
            let physical = prefix_key(current_db, &key);
            let t = storage.type_of(&physical);
            respond_simple_string(writer, &t).await?;
        }
        Command::Keys { pattern } => {
            let mut result: Vec<String> = Vec::new();

            if pattern == "*" {
                let all = storage.keys("*");
                let prefix = format!("{}:", current_db);
                for k in all {
                    if let Some(rest) = k.strip_prefix(&prefix) {
                        result.push(rest.to_string());
                    }
                }
            } else {
                let physical_pattern = prefix_key(current_db, &pattern);
                let physical_keys = storage.keys(&physical_pattern);
                let prefix = format!("{}:", current_db);
                for k in physical_keys {
                    if let Some(rest) = k.strip_prefix(&prefix) {
                        result.push(rest.to_string());
                    }
                }
            }

            let mut response = format!("*{}\r\n", result.len());
            for k in result {
                response.push_str(&format!("${}\r\n{}\r\n", k.len(), k));
            }
            writer.write_all(response.as_bytes()).await?;
        }
        Command::Dbsize => {
            let all = storage.keys("*");
            let prefix = format!("{}:", current_db);
            let count = all.into_iter().filter(|k| k.starts_with(&prefix)).count();
            respond_integer(writer, count as i64).await?;
        }
        _ => {}
    }

    Ok(())
}

async fn handle_info_command(
    storage: &Storage,
    metrics: &Metrics,
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
) -> io::Result<()> {
    let uptime = Instant::now().duration_since(metrics.start_time).as_secs();
    let connected = metrics.connected_clients.load(Ordering::Relaxed);
    let total_cmds = metrics.total_commands.load(Ordering::Relaxed);
    let maxmemory = storage.maxmemory_bytes().unwrap_or(0);
    let maxmemory_human = format_bytes(maxmemory);
    let used_memory = storage.approximate_used_memory();
    let used_memory_human = format_bytes(used_memory);

    let mut info = String::new();
    info.push_str("# Server\r\n");
    info.push_str(&format!("redust_version:0.1.0\r\n"));
    info.push_str(&format!("tcp_port:{}\r\n", metrics.tcp_port));
    info.push_str(&format!("uptime_in_seconds:{}\r\n", uptime));
    info.push_str(&format!("maxmemory:{}\r\n", maxmemory));
    info.push_str(&format!("maxmemory_human:{}\r\n", maxmemory_human));
    info.push_str(&format!("used_memory:{}\r\n", used_memory));
    info.push_str(&format!("used_memory_human:{}\r\n", used_memory_human));
    info.push_str("\r\n# Clients\r\n");
    info.push_str(&format!("connected_clients:{}\r\n", connected));
    info.push_str("\r\n# Stats\r\n");
    info.push_str(&format!("total_commands_processed:{}\r\n", total_cmds));
    info.push_str("\r\n# Keyspace\r\n");

    let all_keys = storage.keys("*");
    let mut db_counts = [0usize; 16];
    for k in all_keys {
        if let Some((db_part, _rest)) = k.split_once(':') {
            if let Ok(idx) = db_part.parse::<usize>() {
                if idx < db_counts.len() {
                    db_counts[idx] += 1;
                }
            }
        }
    }

    // db0 始终输出（即便为 0），保证 INFO 总有一行 db0:keys=...
    info.push_str(&format!("db0:keys={}\r\n", db_counts[0]));

    // 其他 DB 仅在有 key 时输出
    for idx in 1..db_counts.len() {
        let count = db_counts[idx];
        if count > 0 {
            info.push_str(&format!("db{}:keys={}\r\n", idx, count));
        }
    }
    info.push_str("\r\n");

    respond_bulk_string(writer, &info).await
}

async fn handle_metrics_connection(
    mut stream: TcpStream,
    storage: Storage,
    metrics: Arc<Metrics>,
) -> io::Result<()> {
    use tokio::io::AsyncReadExt;

    let mut buf = [0u8; 1024];
    let n = stream.read(&mut buf).await?;
    if n == 0 {
        return Ok(());
    }

    let req = String::from_utf8_lossy(&buf[..n]);
    let first_line = req.lines().next().unwrap_or("");
    let ok = first_line.starts_with("GET /metrics ") || first_line.starts_with("GET /metrics\r");

    if !ok {
        let resp = "HTTP/1.0 404 Not Found\r\nContent-Length: 0\r\n\r\n";
        stream.write_all(resp.as_bytes()).await?;
        return Ok(());
    }

    let body = build_prometheus_metrics(&storage, &metrics);
    let resp = format!(
        "HTTP/1.0 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    stream.write_all(resp.as_bytes()).await?;
    Ok(())
}

async fn run_metrics_exporter(bind_addr: &str, storage: Storage, metrics: Arc<Metrics>) -> io::Result<()> {
    let listener = TcpListener::bind(bind_addr).await?;
    println!("[metrics] exporter listening on {}", listener.local_addr()?);

    loop {
        let (stream, addr) = listener.accept().await?;
        let storage = storage.clone();
        let metrics = metrics.clone();
        println!("[metrics] connection from {}", addr);
        tokio::spawn(async move {
            if let Err(e) = handle_metrics_connection(stream, storage, metrics).await {
                eprintln!("[metrics] connection error: {}", e);
            }
        });
    }
}

async fn handle_connection(stream: TcpStream, storage: Storage, metrics: Arc<Metrics>) -> io::Result<()> {
    let peer_addr = stream.peer_addr().ok();
    println!("[conn] new connection from {:?}", peer_addr);

    metrics.connected_clients.fetch_add(1, Ordering::Relaxed);

    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut current_db: u8 = 0;

    loop {
        let cmd_result = read_command(&mut reader).await;
        let cmd = match cmd_result {
            Ok(Some(command)) => command,
            Ok(None) => break, // Connection closed or no more data
            Err(CommandError::Io(e)) => {
                // Log the IO error and break
                eprintln!("[conn] IO error reading command: {}", e);
                break;
            }
            Err(CommandError::RedisError(msg)) => {
                // Send the Redis-like error message to the client
                let response = format!("-{}\r\n", msg);
                write_half.write_all(response.as_bytes()).await?;
                continue; // Continue to read next command
            }
        };

        metrics.total_commands.fetch_add(1, Ordering::Relaxed);

        println!("[conn] received command: {:?}", cmd);

        match cmd {
            // string / generic key-value 命令
            Command::Ping
            | Command::Echo(_)
            | Command::Set { .. }
            | Command::Get { .. }
            | Command::Incr { .. }
            | Command::Decr { .. }
            | Command::Incrby { .. }
            | Command::Decrby { .. }
            | Command::Del { .. }
            | Command::Exists { .. }
            | Command::Mget { .. }
            | Command::Mset { .. }
            | Command::Setnx { .. }
            | Command::Setex { .. }
            | Command::Psetex { .. } => {
                // Quit 需要在外面单独处理连接关闭语义
                handle_string_command(cmd, &storage, &mut write_half, current_db).await?;
            }
            Command::Quit => {
                handle_string_command(cmd, &storage, &mut write_half, current_db).await?;
                break;
            }

            // list 命令
            Command::Lpush { .. }
            | Command::Rpush { .. }
            | Command::Lrange { .. }
            | Command::Lpop { .. }
            | Command::Rpop { .. }
            | Command::Llen { .. }
            | Command::Lindex { .. }
            | Command::Lrem { .. }
            | Command::Ltrim { .. } => {
                handle_list_command(cmd, &storage, &mut write_half, current_db).await?;
            }

            // set 命令
            Command::Sadd { .. }
            | Command::Srem { .. }
            | Command::Smembers { .. }
            | Command::Scard { .. }
            | Command::Sismember { .. }
            | Command::Sunion { .. }
            | Command::Sinter { .. }
            | Command::Sdiff { .. } => {
                handle_set_command(cmd, &storage, &mut write_half, current_db).await?;
            }

            // hash 命令
            Command::Hset { .. }
            | Command::Hget { .. }
            | Command::Hdel { .. }
            | Command::Hexists { .. }
            | Command::Hgetall { .. } => {
                handle_hash_command(cmd, &storage, &mut write_half, current_db).await?;
            }

            // key 元信息、过期相关命令
            Command::Expire { .. }
            | Command::Pexpire { .. }
            | Command::Ttl { .. }
            | Command::Pttl { .. }
            | Command::Persist { .. }
            | Command::Type { .. }
            | Command::Keys { .. }
            | Command::Dbsize => {
                handle_key_meta_command(cmd, &storage, &mut write_half, current_db).await?;
            }

            // info
            Command::Info => {
                handle_info_command(&storage, &metrics, &mut write_half).await?;
            }

            // 多 DB：SELECT
            Command::Select { db } => {
                current_db = db;
                respond_simple_string(&mut write_half, "OK").await?;
            }

            // 解析阶段构造的错误命令
            Command::Error(msg) => {
                respond_error(&mut write_half, &msg).await?;
            }

            // 未知命令
            Command::Unknown(parts) => {
                let joined = parts.join(" ");
                let msg = format!("ERR unknown command '{}'", joined);
                respond_error(&mut write_half, &msg).await?;
            }
        }
    }

    metrics.connected_clients.fetch_sub(1, Ordering::Relaxed);

    Ok(())
}

pub async fn serve(
    listener: TcpListener,
    shutdown: impl Future<Output = ()> + Send,
) -> io::Result<()> {
    let local_addr = listener.local_addr()?;
    let port = local_addr.port();
    let maxmemory_bytes = env::var("REDUST_MAXMEMORY_BYTES")
        .ok()
        .and_then(|s| parse_maxmemory_bytes(&s))
        .filter(|v| *v > 0);

    let storage = Storage::new(maxmemory_bytes);

    let rdb_path = env::var("REDUST_RDB_PATH").unwrap_or_else(|_| "redust.rdb".to_string());
    if let Err(e) = storage.load_rdb(&rdb_path) {
        eprintln!("[rdb] failed to load RDB from {}: {}", rdb_path, e);
    } else {
        println!("[rdb] loaded RDB from {} (if existing and valid)", rdb_path);
    }

    storage.spawn_expiration_task();

    if let Ok(interval_str) = env::var("REDUST_RDB_AUTO_SAVE_SECS") {
        if let Ok(secs) = interval_str.parse::<u64>() {
            if secs > 0 {
                let storage_clone = storage.clone();
                let path_clone = rdb_path.clone();
                println!(
                    "[rdb] auto-save enabled: every {} seconds to {}",
                    secs, path_clone
                );
                tokio::spawn(async move {
                    let interval = std::time::Duration::from_secs(secs);
                    loop {
                        sleep(interval).await;
                        let storage_for_blocking = storage_clone.clone();
                        let path_for_blocking = path_clone.clone();
                        let result = tokio::task::spawn_blocking(move || {
                            storage_for_blocking.save_rdb(&path_for_blocking)
                        })
                        .await;

                        match result {
                            Ok(Ok(())) => {
                                // saved successfully
                            }
                            Ok(Err(e)) => {
                                eprintln!("[rdb] auto-save failed: {}", e);
                            }
                            Err(e) => {
                                eprintln!("[rdb] auto-save task panicked: {}", e);
                            }
                        }
                    }
                });
            }
        }
    }

    let metrics = Arc::new(Metrics {
        start_time: Instant::now(),
        connected_clients: AtomicUsize::new(0),
        total_commands: AtomicU64::new(0),
        tcp_port: port,
    });

    if let Ok(metrics_addr) = env::var("REDUST_METRICS_ADDR") {
        if !metrics_addr.is_empty() {
            let storage_clone = storage.clone();
            let metrics_clone = metrics.clone();
            tokio::spawn(async move {
                if let Err(e) = run_metrics_exporter(&metrics_addr, storage_clone, metrics_clone).await {
                    eprintln!("[metrics] exporter failed: {}", e);
                }
            });
        }
    }
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            res = listener.accept() => {
                let (stream, addr) = res?;
                let storage = storage.clone();
                let metrics = metrics.clone();
                println!("Accepted connection from {}", addr);
                tokio::spawn(async move {
                    if let Err(err) = handle_connection(stream, storage, metrics).await {
                        eprintln!("Connection error: {}", err);
                    }
                });
            }
            _ = &mut shutdown => {
                break;
            }
        }
    }

    Ok(())
}

pub async fn run_server(
    bind_addr: &str,
    shutdown: impl Future<Output = ()> + Send,
) -> io::Result<()> {
    let listener = TcpListener::bind(bind_addr).await?;
    let local_addr = listener.local_addr()?;

    println!("Redust listening on {}", local_addr);

    serve(listener, shutdown).await
}
