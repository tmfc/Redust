use std::collections::HashMap;
use std::env;
use std::future::Future;
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc,
};
use std::time::Instant;

use dashmap::DashMap;
use tokio::io::{self, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc};
use tokio::time::sleep;

use log::{error, info};

use crate::command::{read_command, Command, CommandError}; // Import CommandError
use crate::resp::{
    respond_bulk_bytes, respond_bulk_string, respond_error, respond_integer, respond_null_bulk,
    respond_simple_string,
};
use crate::storage::Storage;

struct Metrics {
    start_time: Instant,
    connected_clients: AtomicUsize,
    total_commands: AtomicU64,
    tcp_port: u16,
    pubsub_channel_subs: AtomicU64,
    pubsub_pattern_subs: AtomicU64,
    pubsub_shard_subs: AtomicU64,
    pubsub_messages_delivered: AtomicU64,
    pubsub_messages_dropped: AtomicU64,
}

#[derive(Clone)]
struct PubSubHub {
    channels: Arc<DashMap<String, broadcast::Sender<PubMessage>>>,
    patterns: Arc<DashMap<String, broadcast::Sender<PubMessage>>>,
    shard_channels: Arc<DashMap<String, broadcast::Sender<PubMessage>>>,
}

const PUBSUB_BUFFER: usize = 128;

impl PubSubHub {
    fn new() -> Self {
        PubSubHub {
            channels: Arc::new(DashMap::new()),
            patterns: Arc::new(DashMap::new()),
            shard_channels: Arc::new(DashMap::new()),
        }
    }

    fn subscribe_channel(&self, channel: &str) -> broadcast::Receiver<PubMessage> {
        let tx = self
            .channels
            .entry(channel.to_string())
            .or_insert_with(|| broadcast::channel(PUBSUB_BUFFER).0)
            .clone();
        tx.subscribe()
    }

    fn subscribe_pattern(&self, pattern: &str) -> broadcast::Receiver<PubMessage> {
        let tx = self
            .patterns
            .entry(pattern.to_string())
            .or_insert_with(|| broadcast::channel(PUBSUB_BUFFER).0)
            .clone();
        tx.subscribe()
    }

    fn subscribe_shard_channel(&self, channel: &str) -> broadcast::Receiver<PubMessage> {
        let tx = self
            .shard_channels
            .entry(channel.to_string())
            .or_insert_with(|| broadcast::channel(PUBSUB_BUFFER).0)
            .clone();
        tx.subscribe()
    }

    fn cleanup_stale(&self) {
        let mut drop_channels = Vec::new();
        for entry in self.channels.iter() {
            if entry.value().receiver_count() == 0 {
                drop_channels.push(entry.key().clone());
            }
        }
        for ch in drop_channels {
            self.channels.remove(&ch);
        }

        let mut drop_patterns = Vec::new();
        for entry in self.patterns.iter() {
            if entry.value().receiver_count() == 0 {
                drop_patterns.push(entry.key().clone());
            }
        }
        for pat in drop_patterns {
            self.patterns.remove(&pat);
        }

        let mut drop_shard = Vec::new();
        for entry in self.shard_channels.iter() {
            if entry.value().receiver_count() == 0 {
                drop_shard.push(entry.key().clone());
            }
        }
        for ch in drop_shard {
            self.shard_channels.remove(&ch);
        }
    }

    fn publish(&self, channel: &str, payload: &[u8]) -> usize {
        self.cleanup_stale();

        let payload = Arc::new(payload.to_vec());
        let mut delivered = 0usize;
        let channel_name = channel.to_string();

        if let Some(sender) = self.channels.get(channel) {
            let message = PubMessage::Channel {
                channel: channel_name.clone(),
                payload: payload.clone(),
            };
            delivered += sender.send(message).unwrap_or(0);
        }

        for entry in self.patterns.iter() {
            if pattern_match(entry.key(), channel) {
                let message = PubMessage::Pattern {
                    pattern: entry.key().clone(),
                    channel: channel_name.clone(),
                    payload: payload.clone(),
                };
                delivered += entry.value().send(message).unwrap_or(0);
            }
        }

        delivered
    }

    fn publish_shard(&self, channel: &str, payload: &[u8]) -> usize {
        self.cleanup_stale();

        let payload = Arc::new(payload.to_vec());
        if let Some(sender) = self.shard_channels.get(channel) {
            let message = PubMessage::Channel {
                channel: channel.to_string(),
                payload,
            };
            sender.send(message).unwrap_or(0)
        } else {
            0
        }
    }

    fn active_channels(&self, pattern: Option<&str>) -> Vec<String> {
        self.cleanup_stale();

        let mut channels: Vec<String> = self
            .channels
            .iter()
            .filter(|entry| entry.value().receiver_count() > 0)
            .filter_map(|entry| {
                let name = entry.key();
                if let Some(pat) = pattern {
                    if !pattern_match(pat, name) {
                        return None;
                    }
                }
                Some(name.clone())
            })
            .collect();
        channels.sort();
        channels
    }

    fn channel_subscribers(&self, channel: &str) -> usize {
        self.cleanup_stale();

        self.channels
            .get(channel)
            .map(|sender| sender.receiver_count())
            .unwrap_or(0)
    }

    fn shard_channel_subscribers(&self, channel: &str) -> usize {
        self.cleanup_stale();
        self.shard_channels
            .get(channel)
            .map(|sender| sender.receiver_count())
            .unwrap_or(0)
    }

    fn active_shard_channels(&self, pattern: Option<&str>) -> Vec<String> {
        self.cleanup_stale();

        let mut channels: Vec<String> = self
            .shard_channels
            .iter()
            .filter(|entry| entry.value().receiver_count() > 0)
            .filter_map(|entry| {
                let name = entry.key();
                if let Some(pat) = pattern {
                    if !pattern_match(pat, name) {
                        return None;
                    }
                }
                Some(name.clone())
            })
            .collect();
        channels.sort();
        channels
    }

    fn pattern_subscription_count(&self) -> usize {
        self.cleanup_stale();

        self.patterns
            .iter()
            .map(|entry| entry.value().receiver_count())
            .sum()
    }
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

    // 支持带单位：KB / MB / GB（大小写不敏感）
    let (num_part, multiplier) = if let Some(stripped) = s.strip_suffix("kb") {
        (stripped.trim(), 1024u64)
    } else if let Some(stripped) = s.strip_suffix("mb") {
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

fn pattern_match(pattern: &str, value: &str) -> bool {
    fn match_set(p: &[u8], ch: u8) -> Option<(bool, usize)> {
        let mut ranges: Vec<(u8, u8)> = Vec::new();
        let mut i = 1; // skip '['
        while i < p.len() {
            if p[i] == b']' {
                let matched = ranges.iter().any(|(s, e)| ch >= *s && ch <= *e);
                return Some((matched, i + 1));
            }

            let mut start = p[i];
            i += 1;
            if start == b'\\' && i < p.len() {
                start = p[i];
                i += 1;
            }

            if i + 1 < p.len() && p[i] == b'-' {
                let mut end = p[i + 1];
                let mut consumed = 2;
                if end == b'\\' && (i + 2) < p.len() {
                    end = p[i + 2];
                    consumed = 3;
                }
                ranges.push((start, end));
                i += consumed;
            } else {
                ranges.push((start, start));
            }
        }
        None
    }

    fn helper(pat: &[u8], pi: usize, val: &[u8], vi: usize) -> bool {
        if pi == pat.len() {
            return vi == val.len();
        }
        match pat[pi] {
            b'*' => {
                let mut i = vi;
                while i <= val.len() {
                    if helper(pat, pi + 1, val, i) {
                        return true;
                    }
                    if i == val.len() {
                        break;
                    }
                    i += 1;
                }
                false
            }
            b'?' => vi < val.len() && helper(pat, pi + 1, val, vi + 1),
            b'\\' => {
                if pi + 1 >= pat.len() || vi >= val.len() {
                    false
                } else if pat[pi + 1] == val[vi] {
                    helper(pat, pi + 2, val, vi + 1)
                } else {
                    false
                }
            }
            b'[' => {
                if vi >= val.len() {
                    return false;
                }
                if let Some((ok, next_pi)) = match_set(&pat[pi..], val[vi]) {
                    ok && helper(pat, pi + next_pi, val, vi + 1)
                } else {
                    false
                }
            }
            c => vi < val.len() && c == val[vi] && helper(pat, pi + 1, val, vi + 1),
        }
    }

    helper(pattern.as_bytes(), 0, value.as_bytes(), 0)
}

fn current_max_value_bytes() -> Option<u64> {
    env::var("REDUST_MAXVALUE_BYTES")
        .ok()
        .and_then(|s| parse_maxmemory_bytes(&s))
        .filter(|v| *v > 0)
}

fn build_prometheus_metrics(storage: &Storage, metrics: &Metrics) -> String {
    let uptime = Instant::now().duration_since(metrics.start_time).as_secs();
    let connected = metrics.connected_clients.load(Ordering::Relaxed);
    let total_cmds = metrics.total_commands.load(Ordering::Relaxed);
    let keys = storage.keys("*").len();
    let pubsub_channels = metrics.pubsub_channel_subs.load(Ordering::Relaxed);
    let pubsub_patterns = metrics.pubsub_pattern_subs.load(Ordering::Relaxed);
    let pubsub_shards = metrics.pubsub_shard_subs.load(Ordering::Relaxed);
    let pubsub_delivered = metrics.pubsub_messages_delivered.load(Ordering::Relaxed);
    let pubsub_dropped = metrics.pubsub_messages_dropped.load(Ordering::Relaxed);

    let mut buf = String::new();

    buf.push_str("# TYPE redust_uptime_seconds counter\n");
    buf.push_str(&format!("redust_uptime_seconds {}\n", uptime));

    buf.push_str("# TYPE redust_connected_clients gauge\n");
    buf.push_str(&format!("redust_connected_clients {}\n", connected));

    buf.push_str("# TYPE redust_total_commands_processed counter\n");
    buf.push_str(&format!("redust_total_commands_processed {}\n", total_cmds));

    buf.push_str("# TYPE redust_keyspace_keys gauge\n");
    buf.push_str(&format!("redust_keyspace_keys{{db=\"0\"}} {}\n", keys));

    buf.push_str("# TYPE redust_pubsub_channel_subscriptions gauge\n");
    buf.push_str(&format!(
        "redust_pubsub_channel_subscriptions {}\n",
        pubsub_channels
    ));

    buf.push_str("# TYPE redust_pubsub_pattern_subscriptions gauge\n");
    buf.push_str(&format!(
        "redust_pubsub_pattern_subscriptions {}\n",
        pubsub_patterns
    ));

    buf.push_str("# TYPE redust_pubsub_shard_subscriptions gauge\n");
    buf.push_str(&format!(
        "redust_pubsub_shard_subscriptions {}\n",
        pubsub_shards
    ));

    buf.push_str("# TYPE redust_pubsub_messages_delivered counter\n");
    buf.push_str(&format!(
        "redust_pubsub_messages_delivered {}\n",
        pubsub_delivered
    ));

    buf.push_str("# TYPE redust_pubsub_messages_dropped counter\n");
    buf.push_str(&format!(
        "redust_pubsub_messages_dropped {}\n",
        pubsub_dropped
    ));

    buf
}

fn prefix_key(db: u8, key: &str) -> String {
    format!("{}:{}", db, key)
}

#[derive(Clone)]
enum PubMessage {
    Channel {
        channel: String,
        payload: Arc<Vec<u8>>,
    },
    Pattern {
        pattern: String,
        channel: String,
        payload: Arc<Vec<u8>>,
    },
    Lagged,
}

#[derive(Clone, Copy)]
enum PubSubOverflowStrategy {
    Drop,
    Disconnect,
}

async fn write_subscribe_event(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    kind: &str,
    channel: &str,
    count: usize,
) -> io::Result<()> {
    writer
        .write_all(
            format!(
                "*3\r\n${}\r\n{}\r\n${}\r\n{}\r\n:{}\r\n",
                kind.len(),
                kind,
                channel.len(),
                channel,
                count
            )
            .as_bytes(),
        )
        .await
}

async fn write_message_event(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    channel: &str,
    payload: &[u8],
) -> io::Result<()> {
    writer
        .write_all(
            format!(
                "*3\r\n$7\r\nmessage\r\n${}\r\n{}\r\n",
                channel.len(),
                channel
            )
            .as_bytes(),
        )
        .await?;
    writer
        .write_all(format!("${}\r\n", payload.len()).as_bytes())
        .await?;
    writer.write_all(payload).await?;
    writer.write_all(b"\r\n").await
}

async fn write_pmessage_event(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    pattern: &str,
    channel: &str,
    payload: &[u8],
) -> io::Result<()> {
    writer
        .write_all(
            format!(
                "*4\r\n$8\r\npmessage\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                pattern.len(),
                pattern,
                channel.len(),
                channel
            )
            .as_bytes(),
        )
        .await?;
    writer
        .write_all(format!("${}\r\n", payload.len()).as_bytes())
        .await?;
    writer.write_all(payload).await?;
    writer.write_all(b"\r\n").await
}

async fn write_pub_message_event(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    message: &PubMessage,
) -> io::Result<()> {
    match message {
        PubMessage::Channel { channel, payload } => {
            write_message_event(writer, channel, payload).await
        }
        PubMessage::Pattern {
            pattern,
            channel,
            payload,
        } => write_pmessage_event(writer, pattern, channel, payload).await,
        PubMessage::Lagged => Ok(()),
    }
}

async fn write_pong_event(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    payload: &[u8],
) -> io::Result<()> {
    writer
        .write_all(format!("*2\r\n$4\r\npong\r\n${}\r\n", payload.len()).as_bytes())
        .await?;
    writer.write_all(payload).await?;
    writer.write_all(b"\r\n").await
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
        Command::PingWithPayload(payload) => {
            respond_bulk_bytes(writer, &payload).await?;
        }
        Command::Echo(value) => {
            respond_bulk_bytes(writer, &value).await?;
        }
        Command::Quit => {
            respond_simple_string(writer, "OK").await?;
        }
        Command::Set {
            key,
            value,
            expire_millis,
            nx,
            xx,
            keep_ttl,
            get,
        } => {
            let physical = prefix_key(current_db, &key);

            // 长度限制
            if let Some(limit) = current_max_value_bytes() {
                if (value.len() as u64) > limit {
                    respond_error(writer, "ERR value exceeds REDUST_MAXVALUE_BYTES").await?;
                    return Ok(());
                }
            }

            // 获取旧值（字符串类型才有意义）
            let old_value = storage.get(&physical);

            // 若指定 KEEPTTL，则在覆盖前先读取剩余 TTL
            let keep_ttl_ms = if keep_ttl {
                let ttl = storage.pttl_millis(&physical);
                if ttl > 0 {
                    Some(ttl)
                } else {
                    None
                }
            } else {
                None
            };

            // 判断 key 是否存在（无论类型），用于 NX/XX
            let existed = storage.type_of(&physical) != "none";

            // 根据 NX / XX 判断是否写入
            let should_write = if nx {
                !existed
            } else if xx {
                existed
            } else {
                true
            };

            // 处理写入和 TTL
            if should_write {
                // 写入新值
                storage.set(physical.clone(), value);

                // TTL 处理优先级：
                // 1. KEEPTTL：恢复旧 TTL（如果存在）
                // 2. EX/PX：设置新的 TTL
                // 3. 否则：清除已有 TTL
                if let Some(ms) = keep_ttl_ms {
                    storage.expire_millis(&physical, ms);
                } else if let Some(ms) = expire_millis {
                    storage.expire_millis(&physical, ms);
                } else {
                    let _ = storage.persist(&physical);
                }
            }

            // 处理返回值
            if get {
                match old_value {
                    Some(v) => respond_bulk_bytes(writer, &v).await?,
                    None => respond_null_bulk(writer).await?,
                }
            } else if should_write {
                respond_simple_string(writer, "OK").await?;
            } else {
                // 条件失败且无 GET 时，返回 null bulk（与 Redis 行为一致）
                respond_null_bulk(writer).await?;
            }
        }
        Command::Incrbyfloat { key, delta } => {
            let physical = prefix_key(current_db, &key);
            match storage.incr_by_float(&physical, delta) {
                Ok(value) => {
                    let s = {
                        let mut s = value.to_string();
                        if s.contains('.') {
                            while s.ends_with('0') {
                                s.pop();
                            }
                            if s.ends_with('.') {
                                s.push('0');
                            }
                        }
                        s
                    };
                    respond_bulk_string(writer, &s).await?;
                }
                Err(_) => {
                    respond_error(writer, "ERR value is not a valid float").await?;
                }
            }
        }
        Command::Get { key } => {
            let physical = prefix_key(current_db, &key);
            if let Some(value) = storage.get(&physical) {
                respond_bulk_bytes(writer, &value).await?;
            } else {
                respond_null_bulk(writer).await?;
            }
        }
        Command::Getdel { key } => {
            let physical = prefix_key(current_db, &key);
            match storage.getdel(&physical) {
                Ok(Some(value)) => {
                    respond_bulk_bytes(writer, &value).await?;
                }
                Ok(None) => {
                    respond_null_bulk(writer).await?;
                }
                Err(()) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
                }
            }
        }
        Command::Getex {
            key,
            expire_millis,
            persist,
        } => {
            let physical = prefix_key(current_db, &key);

            let value_opt = storage.get(&physical);

            match value_opt {
                Some(ref v) => {
                    // 先返回当前值
                    respond_bulk_bytes(writer, v).await?;

                    // 然后根据选项更新 TTL
                    if let Some(ms) = expire_millis {
                        let _ = storage.expire_millis(&physical, ms);
                    } else if persist {
                        let _ = storage.persist(&physical);
                    }
                }
                None => {
                    // key 不存在/已过期: 返回 nil，不再尝试设置 TTL
                    respond_null_bulk(writer).await?;
                }
            }
        }
        Command::Getrange { key, start, end } => {
            let physical = prefix_key(current_db, &key);
            match storage.getrange(&physical, start, end) {
                Ok(s) => {
                    respond_bulk_bytes(writer, &s).await?;
                }
                Err(()) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
                }
            }
        }
        Command::Setrange { key, offset, value } => {
            let physical = prefix_key(current_db, &key);
            if let Some(limit) = current_max_value_bytes() {
                if (value.len() as u64) > limit {
                    respond_error(writer, "ERR value exceeds REDUST_MAXVALUE_BYTES").await?;
                    return Ok(());
                }
            }

            match storage.setrange(&physical, offset, &value) {
                Ok(len) => {
                    respond_integer(writer, len as i64).await?;
                }
                Err(()) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
                }
            }
        }
        Command::Append { key, value } => {
            let physical = prefix_key(current_db, &key);
            if let Some(limit) = current_max_value_bytes() {
                if (value.len() as u64) > limit {
                    respond_error(writer, "ERR value exceeds REDUST_MAXVALUE_BYTES").await?;
                    return Ok(());
                }
            }

            match storage.append(&physical, &value) {
                Ok(new_len) => {
                    respond_integer(writer, new_len as i64).await?;
                }
                Err(()) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
                }
            }
        }
        Command::Strlen { key } => {
            let physical = prefix_key(current_db, &key);
            match storage.strlen(&physical) {
                Ok(len) => {
                    respond_integer(writer, len as i64).await?;
                }
                Err(()) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
                }
            }
        }
        Command::Getset { key, value } => {
            let physical = prefix_key(current_db, &key);
            if let Some(limit) = current_max_value_bytes() {
                if (value.len() as u64) > limit {
                    respond_error(writer, "ERR value exceeds REDUST_MAXVALUE_BYTES").await?;
                    return Ok(());
                }
            }

            match storage.getset(&physical, value) {
                Ok(Some(old)) => {
                    respond_bulk_bytes(writer, &old).await?;
                }
                Ok(None) => {
                    respond_null_bulk(writer).await?;
                }
                Err(()) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
                }
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
            let physical: Vec<String> = keys
                .into_iter()
                .map(|k| prefix_key(current_db, &k))
                .collect();
            let removed = storage.del(&physical);
            respond_integer(writer, removed as i64).await?;
        }
        Command::Exists { keys } => {
            let physical: Vec<String> = keys
                .into_iter()
                .map(|k| prefix_key(current_db, &k))
                .collect();
            let count = storage.exists(&physical);
            respond_integer(writer, count as i64).await?;
        }
        Command::Mget { keys } => {
            let physical: Vec<String> = keys
                .into_iter()
                .map(|k| prefix_key(current_db, &k))
                .collect();
            let values = storage.mget(&physical);
            writer
                .write_all(format!("*{}\r\n", values.len()).as_bytes())
                .await?;
            for v in values {
                match v {
                    Some(s) => {
                        writer
                            .write_all(format!("${}\r\n", s.len()).as_bytes())
                            .await?;
                        writer.write_all(&s).await?;
                        writer.write_all(b"\r\n").await?;
                    }
                    None => {
                        writer.write_all(b"$-1\r\n").await?;
                    }
                }
            }
        }
        Command::Mset { pairs } => {
            if let Some(limit) = current_max_value_bytes() {
                for (_k, v) in pairs.iter() {
                    if (v.len() as u64) > limit {
                        respond_error(writer, "ERR value exceeds REDUST_MAXVALUE_BYTES").await?;
                        return Ok(());
                    }
                }
            }

            let mapped: Vec<(String, Vec<u8>)> = pairs
                .into_iter()
                .map(|(k, v)| (prefix_key(current_db, &k), v))
                .collect();
            storage.mset(&mapped);
            respond_simple_string(writer, "OK").await?;
        }
        Command::Msetnx { pairs } => {
            if let Some(limit) = current_max_value_bytes() {
                for (_k, v) in pairs.iter() {
                    if (v.len() as u64) > limit {
                        respond_error(writer, "ERR value exceeds REDUST_MAXVALUE_BYTES").await?;
                        return Ok(());
                    }
                }
            }

            let mapped: Vec<(String, Vec<u8>)> = pairs
                .into_iter()
                .map(|(k, v)| (prefix_key(current_db, &k), v))
                .collect();

            let ok = storage.msetnx(&mapped);
            let v = if ok { 1 } else { 0 };
            respond_integer(writer, v).await?;
        }
        Command::Setnx { key, value } => {
            let physical = prefix_key(current_db, &key);
            if let Some(limit) = current_max_value_bytes() {
                if (value.len() as u64) > limit {
                    respond_error(writer, "ERR value exceeds REDUST_MAXVALUE_BYTES").await?;
                    return Ok(());
                }
            }
            let inserted = storage.setnx(&physical, value);
            let v = if inserted { 1 } else { 0 };
            respond_integer(writer, v).await?;
        }
        Command::Setex {
            key,
            seconds,
            value,
        } => {
            let physical = prefix_key(current_db, &key);
            if let Some(limit) = current_max_value_bytes() {
                if (value.len() as u64) > limit {
                    respond_error(writer, "ERR value exceeds REDUST_MAXVALUE_BYTES").await?;
                    return Ok(());
                }
            }
            storage.set_with_expire_seconds(physical, value, seconds);
            respond_simple_string(writer, "OK").await?;
        }
        Command::Psetex { key, millis, value } => {
            let physical = prefix_key(current_db, &key);
            if let Some(limit) = current_max_value_bytes() {
                if (value.len() as u64) > limit {
                    respond_error(writer, "ERR value exceeds REDUST_MAXVALUE_BYTES").await?;
                    return Ok(());
                }
            }
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
            if let Some(limit) = current_max_value_bytes() {
                for v in &values {
                    if (v.as_bytes().len() as u64) > limit {
                        respond_error(writer, "ERR value exceeds REDUST_MAXVALUE_BYTES").await?;
                        return Ok(());
                    }
                }
            }
            match storage.lpush(&physical, &values) {
                Ok(len) => {
                    respond_integer(writer, len as i64).await?;
                }
                Err(()) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
                }
            }
        }
        Command::Hkeys { key } => {
            let physical = prefix_key(current_db, &key);
            match storage.hkeys(&physical) {
                Ok(keys) => {
                    let mut response = format!("*{}\r\n", keys.len());
                    for k in keys {
                        response.push_str(&format!("${}\r\n{}\r\n", k.len(), k));
                    }
                    writer.write_all(response.as_bytes()).await?;
                }
                Err(()) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
                }
            }
        }
        Command::Hvals { key } => {
            let physical = prefix_key(current_db, &key);
            match storage.hvals(&physical) {
                Ok(vals) => {
                    let mut response = format!("*{}\r\n", vals.len());
                    for v in vals {
                        response.push_str(&format!("${}\r\n{}\r\n", v.len(), v));
                    }
                    writer.write_all(response.as_bytes()).await?;
                }
                Err(()) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
                }
            }
        }
        Command::Hmget { key, fields } => {
            let physical = prefix_key(current_db, &key);
            match storage.hmget(&physical, &fields) {
                Ok(values) => {
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
                Err(()) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
                }
            }
        }
        Command::Rpush { key, values } => {
            let physical = prefix_key(current_db, &key);
            if let Some(limit) = current_max_value_bytes() {
                for v in &values {
                    if (v.as_bytes().len() as u64) > limit {
                        respond_error(writer, "ERR value exceeds REDUST_MAXVALUE_BYTES").await?;
                        return Ok(());
                    }
                }
            }
            match storage.rpush(&physical, &values) {
                Ok(len) => {
                    respond_integer(writer, len as i64).await?;
                }
                Err(()) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
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
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
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
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
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
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
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
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
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
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
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
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
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
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
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
            if let Some(limit) = current_max_value_bytes() {
                for m in &members {
                    if (m.as_bytes().len() as u64) > limit {
                        respond_error(writer, "ERR value exceeds REDUST_MAXVALUE_BYTES").await?;
                        return Ok(());
                    }
                }
            }
            match storage.sadd(&physical, &members) {
                Ok(added) => {
                    respond_integer(writer, added as i64).await?;
                }
                Err(()) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
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
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
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
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
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
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
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
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
                }
            }
        }
        Command::Sunion { keys } => {
            let physical: Vec<String> = keys
                .into_iter()
                .map(|k| prefix_key(current_db, &k))
                .collect();
            match storage.sunion(&physical) {
                Ok(members) => {
                    let mut response = format!("*{}\r\n", members.len());
                    for m in members {
                        response.push_str(&format!("${}\r\n{}\r\n", m.len(), m));
                    }
                    writer.write_all(response.as_bytes()).await?;
                }
                Err(()) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
                }
            }
        }
        Command::Sinter { keys } => {
            let physical: Vec<String> = keys
                .into_iter()
                .map(|k| prefix_key(current_db, &k))
                .collect();
            match storage.sinter(&physical) {
                Ok(members) => {
                    let mut response = format!("*{}\r\n", members.len());
                    for m in members {
                        response.push_str(&format!("${}\r\n{}\r\n", m.len(), m));
                    }
                    writer.write_all(response.as_bytes()).await?;
                }
                Err(()) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
                }
            }
        }
        Command::Sdiff { keys } => {
            let physical: Vec<String> = keys
                .into_iter()
                .map(|k| prefix_key(current_db, &k))
                .collect();
            match storage.sdiff(&physical) {
                Ok(members) => {
                    let mut response = format!("*{}\r\n", members.len());
                    for m in members {
                        response.push_str(&format!("${}\r\n{}\r\n", m.len(), m));
                    }
                    writer.write_all(response.as_bytes()).await?;
                }
                Err(()) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
                }
            }
        }
        Command::Sunionstore { dest, keys } => {
            let physical_keys: Vec<String> = keys
                .into_iter()
                .map(|k| prefix_key(current_db, &k))
                .collect();
            let physical_dest = prefix_key(current_db, &dest);
            match storage.sunionstore(&physical_dest, &physical_keys) {
                Ok(len) => {
                    respond_integer(writer, len as i64).await?;
                }
                Err(()) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
                }
            }
        }
        Command::Sinterstore { dest, keys } => {
            let physical_keys: Vec<String> = keys
                .into_iter()
                .map(|k| prefix_key(current_db, &k))
                .collect();
            let physical_dest = prefix_key(current_db, &dest);
            match storage.sinterstore(&physical_dest, &physical_keys) {
                Ok(len) => {
                    respond_integer(writer, len as i64).await?;
                }
                Err(()) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
                }
            }
        }
        Command::Sdiffstore { dest, keys } => {
            let physical_keys: Vec<String> = keys
                .into_iter()
                .map(|k| prefix_key(current_db, &k))
                .collect();
            let physical_dest = prefix_key(current_db, &dest);
            match storage.sdiffstore(&physical_dest, &physical_keys) {
                Ok(len) => {
                    respond_integer(writer, len as i64).await?;
                }
                Err(()) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
                }
            }
        }
        Command::Spop { key, count } => {
            let physical = prefix_key(current_db, &key);
            match storage.spop(&physical, count) {
                Ok(mut members) => {
                    match members.len() {
                        0 => {
                            // Redis: SPOP on empty set returns null bulk
                            respond_null_bulk(writer).await?;
                        }
                        1 => {
                            let m = members.pop().unwrap();
                            respond_bulk_string(writer, &m).await?;
                        }
                        n => {
                            let mut response = format!("*{}\r\n", n);
                            for m in members {
                                response.push_str(&format!("${}\r\n{}\r\n", m.len(), m));
                            }
                            writer.write_all(response.as_bytes()).await?;
                        }
                    }
                }
                Err(()) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
                }
            }
        }
        Command::Srandmember { key, count } => {
            let physical = prefix_key(current_db, &key);
            match storage.srandmember(&physical, count) {
                Ok(members) => match count {
                    None => {
                        if let Some(m) = members.into_iter().next() {
                            respond_bulk_string(writer, &m).await?;
                        } else {
                            respond_null_bulk(writer).await?;
                        }
                    }
                    Some(_) => {
                        let mut response = format!("*{}\r\n", members.len());
                        for m in members {
                            response.push_str(&format!("${}\r\n{}\r\n", m.len(), m));
                        }
                        writer.write_all(response.as_bytes()).await?;
                    }
                },
                Err(()) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
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
            if let Some(limit) = current_max_value_bytes() {
                if (value.as_bytes().len() as u64) > limit {
                    respond_error(writer, "ERR value exceeds REDUST_MAXVALUE_BYTES").await?;
                    return Ok(());
                }
            }
            match storage.hset(&physical, &field, value) {
                Ok(added) => {
                    respond_integer(writer, added as i64).await?;
                }
                Err(()) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
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
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
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
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
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
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
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
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
                }
            }
        }
        Command::Hkeys { key } => {
            let physical = prefix_key(current_db, &key);
            match storage.hkeys(&physical) {
                Ok(fields) => {
                    let mut response = format!("*{}\r\n", fields.len());
                    for field in fields {
                        response.push_str(&format!("${}\r\n{}\r\n", field.len(), field));
                    }
                    writer.write_all(response.as_bytes()).await?;
                }
                Err(()) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
                }
            }
        }
        Command::Hvals { key } => {
            let physical = prefix_key(current_db, &key);
            match storage.hvals(&physical) {
                Ok(values) => {
                    let mut response = format!("*{}\r\n", values.len());
                    for value in values {
                        response.push_str(&format!("${}\r\n{}\r\n", value.len(), value));
                    }
                    writer.write_all(response.as_bytes()).await?;
                }
                Err(()) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
                }
            }
        }
        Command::Hmget { key, fields } => {
            let physical = prefix_key(current_db, &key);
            match storage.hmget(&physical, &fields) {
                Ok(values) => {
                    // HMGET 返回数组，每个元素是 bulk 或 null bulk
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
                Err(()) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
                }
            }
        }
        Command::Hincrby { key, field, delta } => {
            let physical = prefix_key(current_db, &key);
            let max = current_max_value_bytes();
            match storage.hincr_by(&physical, &field, delta, max) {
                Ok(value) => {
                    respond_integer(writer, value).await?;
                }
                Err(crate::storage::HincrError::WrongType) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
                }
                Err(crate::storage::HincrError::NotInteger) => {
                    respond_error(writer, "ERR hash value is not an integer").await?;
                }
                Err(crate::storage::HincrError::Overflow) => {
                    respond_error(writer, "ERR increment or decrement would overflow").await?;
                }
                Err(crate::storage::HincrError::MaxValueExceeded) => {
                    respond_error(writer, "ERR value exceeds REDUST_MAXVALUE_BYTES").await?;
                }
            }
        }
        Command::Hincrbyfloat { key, field, delta } => {
            let physical = prefix_key(current_db, &key);
            let max = current_max_value_bytes();
            match storage.hincr_by_float(&physical, &field, delta, max) {
                Ok(value) => {
                    let mut s = value.to_string();
                    if s.contains('.') {
                        while s.ends_with('0') {
                            s.pop();
                        }
                        if s.ends_with('.') {
                            s.push('0');
                        }
                    }
                    respond_bulk_string(writer, &s).await?;
                }
                Err(crate::storage::HincrFloatError::WrongType) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
                }
                Err(crate::storage::HincrFloatError::NotFloat) => {
                    respond_error(writer, "ERR hash value is not a valid float").await?;
                }
                Err(crate::storage::HincrFloatError::MaxValueExceeded) => {
                    respond_error(writer, "ERR value exceeds REDUST_MAXVALUE_BYTES").await?;
                }
            }
        }
        Command::Hlen { key } => {
            let physical = prefix_key(current_db, &key);
            match storage.hlen(&physical) {
                Ok(len) => {
                    respond_integer(writer, len as i64).await?;
                }
                Err(()) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
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
        Command::Rename { key, newkey } => {
            let from = prefix_key(current_db, &key);
            let to = prefix_key(current_db, &newkey);
            match storage.rename(&from, &to) {
                Ok(()) => {
                    respond_simple_string(writer, "OK").await?;
                }
                Err(()) => {
                    respond_error(writer, "ERR no such key").await?;
                }
            }
        }
        Command::Renamenx { key, newkey } => {
            let from = prefix_key(current_db, &key);
            let to = prefix_key(current_db, &newkey);
            match storage.renamenx(&from, &to) {
                Ok(true) => {
                    respond_integer(writer, 1).await?;
                }
                Ok(false) => {
                    respond_integer(writer, 0).await?;
                }
                Err(()) => {
                    respond_error(writer, "ERR no such key").await?;
                }
            }
        }
        Command::Flushdb => {
            storage.flushdb(current_db);
            respond_simple_string(writer, "OK").await?;
        }
        Command::Flushall => {
            storage.flushall();
            respond_simple_string(writer, "OK").await?;
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
    info.push_str(&format!(
        "pubsub_channel_subscriptions:{}\r\n",
        metrics.pubsub_channel_subs.load(Ordering::Relaxed)
    ));
    info.push_str(&format!(
        "pubsub_pattern_subscriptions:{}\r\n",
        metrics.pubsub_pattern_subs.load(Ordering::Relaxed)
    ));
    info.push_str(&format!(
        "pubsub_shard_subscriptions:{}\r\n",
        metrics.pubsub_shard_subs.load(Ordering::Relaxed)
    ));
    info.push_str(&format!(
        "pubsub_messages_delivered:{}\r\n",
        metrics.pubsub_messages_delivered.load(Ordering::Relaxed)
    ));
    info.push_str(&format!(
        "pubsub_messages_dropped:{}\r\n",
        metrics.pubsub_messages_dropped.load(Ordering::Relaxed)
    ));
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

async fn run_metrics_exporter(
    bind_addr: &str,
    storage: Storage,
    metrics: Arc<Metrics>,
) -> io::Result<()> {
    let listener = TcpListener::bind(bind_addr).await?;
    info!("[metrics] exporter listening on {}", listener.local_addr()?);

    loop {
        let (stream, addr) = listener.accept().await?;
        let storage = storage.clone();
        let metrics = metrics.clone();
        info!("[metrics] connection from {}", addr);
        tokio::spawn(async move {
            if let Err(e) = handle_metrics_connection(stream, storage, metrics).await {
                error!("[metrics] connection error: {}", e);
            }
        });
    }
}

async fn handle_connection(
    stream: TcpStream,
    storage: Storage,
    metrics: Arc<Metrics>,
    pubsub: PubSubHub,
    overflow_strategy: PubSubOverflowStrategy,
) -> io::Result<()> {
    let peer_addr = stream.peer_addr().ok();
    info!("[conn] new connection from {:?}", peer_addr);

    metrics.connected_clients.fetch_add(1, Ordering::Relaxed);

    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut current_db: u8 = 0;

    let auth_password = env::var("REDUST_AUTH_PASSWORD")
        .ok()
        .filter(|s| !s.is_empty());
    let mut authenticated = auth_password.is_none();
    let (msg_tx, mut msg_rx) = mpsc::unbounded_channel::<PubMessage>();
    let mut channel_subscriptions: HashMap<String, tokio::task::JoinHandle<()>> = HashMap::new();
    let mut pattern_subscriptions: HashMap<String, tokio::task::JoinHandle<()>> = HashMap::new();
    let mut shard_subscriptions: HashMap<String, tokio::task::JoinHandle<()>> = HashMap::new();
    let mut subscribed_mode = false;

    loop {
        let cmd_result = if subscribed_mode {
            tokio::select! {
                maybe_msg = msg_rx.recv() => {
                    if let Some(msg) = maybe_msg {
                        if let PubMessage::Lagged = msg {
                            break;
                        }
                        write_pub_message_event(&mut write_half, &msg).await?;
                        continue;
                    }
                    read_command(&mut reader).await
                }
                cmd = read_command(&mut reader) => cmd,
            }
        } else {
            read_command(&mut reader).await
        };
        let cmd = match cmd_result {
            Ok(Some(command)) => command,
            Ok(None) => break, // Connection closed or no more data
            Err(CommandError::Io(e)) => {
                // Log the IO error and break
                error!("[conn] IO error reading command: {}", e);
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

        match &cmd {
            Command::Auth { .. } => {
                info!("[conn] received command: AUTH ****");
            }
            _ => {
                info!("[conn] received command: {:?}", cmd);
            }
        }

        // AUTH 处理与权限检查
        if let Some(ref pwd) = auth_password {
            match cmd {
                Command::Auth { ref password } => {
                    if password == pwd {
                        authenticated = true;
                        respond_simple_string(&mut write_half, "OK").await?;
                    } else {
                        respond_error(
                            &mut write_half,
                            "WRONGPASS invalid username-password pair or user is disabled",
                        )
                        .await?;
                    }
                    continue;
                }
                Command::Ping | Command::PingWithPayload(_) | Command::Echo(_) | Command::Quit => {
                    // 这些命令在未认证时仍然允许
                }
                _ => {
                    if !authenticated {
                        respond_error(&mut write_half, "NOAUTH Authentication required").await?;
                        continue;
                    }
                }
            }
        } else if let Command::Auth { .. } = cmd {
            // 未启用 AUTH，但客户端仍然发送 AUTH
            respond_error(&mut write_half, "ERR AUTH not enabled").await?;
            continue;
        }

        if subscribed_mode {
            match cmd {
                Command::Subscribe { .. }
                | Command::Unsubscribe { .. }
                | Command::Ssubscribe { .. }
                | Command::Sunsubscribe { .. }
                | Command::Psubscribe { .. }
                | Command::Punsubscribe { .. }
                | Command::Ping
                | Command::PingWithPayload(_)
                | Command::Quit => {}
                _ => {
                    respond_error(&mut write_half, "ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context").await?;
                    continue;
                }
            }
            match cmd {
                Command::Ping => {
                    write_pong_event(&mut write_half, b"").await?;
                    continue;
                }
                Command::PingWithPayload(payload) => {
                    write_pong_event(&mut write_half, &payload).await?;
                    continue;
                }
                _ => {}
            }
        }

        match cmd {
            // string / generic key-value 命令
            Command::Ping
            | Command::PingWithPayload(_)
            | Command::Echo(_)
            | Command::Set { .. }
            | Command::Get { .. }
            | Command::Getdel { .. }
            | Command::Getex { .. }
            | Command::Getrange { .. }
            | Command::Setrange { .. }
            | Command::Append { .. }
            | Command::Strlen { .. }
            | Command::Getset { .. }
            | Command::Incr { .. }
            | Command::Decr { .. }
            | Command::Incrby { .. }
            | Command::Decrby { .. }
            | Command::Incrbyfloat { .. }
            | Command::Del { .. }
            | Command::Exists { .. }
            | Command::Mget { .. }
            | Command::Mset { .. }
            | Command::Msetnx { .. }
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
            | Command::Spop { .. }
            | Command::Srandmember { .. }
            | Command::Sunion { .. }
            | Command::Sinter { .. }
            | Command::Sdiff { .. }
            | Command::Sunionstore { .. }
            | Command::Sinterstore { .. }
            | Command::Sdiffstore { .. } => {
                handle_set_command(cmd, &storage, &mut write_half, current_db).await?;
            }

            // hash 命令
            Command::Hset { .. }
            | Command::Hget { .. }
            | Command::Hdel { .. }
            | Command::Hexists { .. }
            | Command::Hgetall { .. }
            | Command::Hkeys { .. }
            | Command::Hvals { .. }
            | Command::Hmget { .. }
            | Command::Hincrby { .. }
            | Command::Hincrbyfloat { .. }
            | Command::Hlen { .. } => {
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
            | Command::Dbsize
            | Command::Rename { .. }
            | Command::Renamenx { .. }
            | Command::Flushdb
            | Command::Flushall => {
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
            // Pub/Sub 发布
            Command::Publish { channel, message } => {
                if let Some(limit) = current_max_value_bytes() {
                    if (message.len() as u64) > limit {
                        respond_error(&mut write_half, "ERR value exceeds REDUST_MAXVALUE_BYTES")
                            .await?;
                        continue;
                    }
                }
                let receivers = pubsub.publish(&channel, &message);
                metrics
                    .pubsub_messages_delivered
                    .fetch_add(receivers as u64, Ordering::Relaxed);
                respond_integer(&mut write_half, receivers as i64).await?;
            }
            Command::Spublish { channel, message } => {
                if let Some(limit) = current_max_value_bytes() {
                    if (message.len() as u64) > limit {
                        respond_error(&mut write_half, "ERR value exceeds REDUST_MAXVALUE_BYTES")
                            .await?;
                        continue;
                    }
                }
                let receivers = pubsub.publish_shard(&channel, &message);
                metrics
                    .pubsub_messages_delivered
                    .fetch_add(receivers as u64, Ordering::Relaxed);
                respond_integer(&mut write_half, receivers as i64).await?;
            }
            // Pub/Sub 订阅
            Command::Subscribe { channels } => {
                for channel in channels {
                    if !channel_subscriptions.contains_key(&channel) {
                        let mut rx = pubsub.subscribe_channel(&channel);
                        let tx = msg_tx.clone();
                        let metrics_clone = metrics.clone();
                        let handle = tokio::spawn(async move {
                            loop {
                                match rx.recv().await {
                                    Ok(message) => {
                                        let _ = tx.send(message);
                                    }
                                    Err(broadcast::error::RecvError::Lagged(n)) => {
                                        metrics_clone
                                            .pubsub_messages_dropped
                                            .fetch_add(n as u64, Ordering::Relaxed);
                                        if matches!(
                                            overflow_strategy,
                                            PubSubOverflowStrategy::Disconnect
                                        ) {
                                            let _ = tx.send(PubMessage::Lagged);
                                            break;
                                        }
                                        continue;
                                    }
                                    Err(broadcast::error::RecvError::Closed) => {
                                        if matches!(
                                            overflow_strategy,
                                            PubSubOverflowStrategy::Disconnect
                                        ) {
                                            let _ = tx.send(PubMessage::Lagged);
                                        }
                                        break;
                                    }
                                }
                            }
                        });
                        channel_subscriptions.insert(channel.clone(), handle);
                        metrics.pubsub_channel_subs.fetch_add(1, Ordering::Relaxed);
                    }
                    let count = channel_subscriptions.len()
                        + pattern_subscriptions.len()
                        + shard_subscriptions.len();
                    write_subscribe_event(&mut write_half, "subscribe", &channel, count).await?;
                }
                subscribed_mode = (channel_subscriptions.len()
                    + pattern_subscriptions.len()
                    + shard_subscriptions.len())
                    > 0;
            }
            Command::Unsubscribe { channels } => {
                let targets: Vec<String> = if channels.is_empty() {
                    let mut v: Vec<String> = channel_subscriptions.keys().cloned().collect();
                    v.sort();
                    v
                } else {
                    channels
                };

                if targets.is_empty() {
                    let count = channel_subscriptions.len()
                        + pattern_subscriptions.len()
                        + shard_subscriptions.len();
                    write_subscribe_event(&mut write_half, "unsubscribe", "", count).await?;
                } else {
                    for ch in targets {
                        if let Some(handle) = channel_subscriptions.remove(&ch) {
                            handle.abort();
                            metrics.pubsub_channel_subs.fetch_sub(1, Ordering::Relaxed);
                        }
                        let count = channel_subscriptions.len()
                            + pattern_subscriptions.len()
                            + shard_subscriptions.len();
                        write_subscribe_event(&mut write_half, "unsubscribe", &ch, count).await?;
                    }
                }
                subscribed_mode = (channel_subscriptions.len()
                    + pattern_subscriptions.len()
                    + shard_subscriptions.len())
                    > 0;
                pubsub.cleanup_stale();
            }
            Command::Ssubscribe { channels } => {
                for channel in channels {
                    if !shard_subscriptions.contains_key(&channel) {
                        let mut rx = pubsub.subscribe_shard_channel(&channel);
                        let tx = msg_tx.clone();
                        let metrics_clone = metrics.clone();
                        let handle = tokio::spawn(async move {
                            loop {
                                match rx.recv().await {
                                    Ok(message) => {
                                        let _ = tx.send(message);
                                    }
                                    Err(broadcast::error::RecvError::Lagged(n)) => {
                                        metrics_clone
                                            .pubsub_messages_dropped
                                            .fetch_add(n as u64, Ordering::Relaxed);
                                        if matches!(
                                            overflow_strategy,
                                            PubSubOverflowStrategy::Disconnect
                                        ) {
                                            let _ = tx.send(PubMessage::Lagged);
                                            break;
                                        }
                                        continue;
                                    }
                                    Err(broadcast::error::RecvError::Closed) => {
                                        if matches!(
                                            overflow_strategy,
                                            PubSubOverflowStrategy::Disconnect
                                        ) {
                                            let _ = tx.send(PubMessage::Lagged);
                                        }
                                        break;
                                    }
                                }
                            }
                        });
                        shard_subscriptions.insert(channel.clone(), handle);
                        metrics.pubsub_shard_subs.fetch_add(1, Ordering::Relaxed);
                    }
                    let count = channel_subscriptions.len()
                        + pattern_subscriptions.len()
                        + shard_subscriptions.len();
                    write_subscribe_event(&mut write_half, "ssubscribe", &channel, count).await?;
                }
                subscribed_mode = (channel_subscriptions.len()
                    + pattern_subscriptions.len()
                    + shard_subscriptions.len())
                    > 0;
            }
            Command::Sunsubscribe { channels } => {
                let targets: Vec<String> = if channels.is_empty() {
                    let mut v: Vec<String> = shard_subscriptions.keys().cloned().collect();
                    v.sort();
                    v
                } else {
                    channels
                };

                if targets.is_empty() {
                    let count = channel_subscriptions.len()
                        + pattern_subscriptions.len()
                        + shard_subscriptions.len();
                    write_subscribe_event(&mut write_half, "sunsubscribe", "", count).await?;
                } else {
                    for ch in targets {
                        if let Some(handle) = shard_subscriptions.remove(&ch) {
                            handle.abort();
                            metrics.pubsub_shard_subs.fetch_sub(1, Ordering::Relaxed);
                        }
                        let count = channel_subscriptions.len()
                            + pattern_subscriptions.len()
                            + shard_subscriptions.len();
                        write_subscribe_event(&mut write_half, "sunsubscribe", &ch, count).await?;
                    }
                }
                subscribed_mode = (channel_subscriptions.len()
                    + pattern_subscriptions.len()
                    + shard_subscriptions.len())
                    > 0;
                pubsub.cleanup_stale();
            }
            // 模式订阅
            Command::Psubscribe { patterns } => {
                for pattern in patterns {
                    if !pattern_subscriptions.contains_key(&pattern) {
                        let mut rx = pubsub.subscribe_pattern(&pattern);
                        let tx = msg_tx.clone();
                        let metrics_clone = metrics.clone();
                        let handle = tokio::spawn(async move {
                            loop {
                                match rx.recv().await {
                                    Ok(message) => {
                                        let _ = tx.send(message);
                                    }
                                    Err(broadcast::error::RecvError::Lagged(n)) => {
                                        metrics_clone
                                            .pubsub_messages_dropped
                                            .fetch_add(n as u64, Ordering::Relaxed);
                                        if matches!(
                                            overflow_strategy,
                                            PubSubOverflowStrategy::Disconnect
                                        ) {
                                            let _ = tx.send(PubMessage::Lagged);
                                            break;
                                        }
                                        continue;
                                    }
                                    Err(broadcast::error::RecvError::Closed) => {
                                        if matches!(
                                            overflow_strategy,
                                            PubSubOverflowStrategy::Disconnect
                                        ) {
                                            let _ = tx.send(PubMessage::Lagged);
                                        }
                                        break;
                                    }
                                }
                            }
                        });
                        pattern_subscriptions.insert(pattern.clone(), handle);
                        metrics.pubsub_pattern_subs.fetch_add(1, Ordering::Relaxed);
                    }
                    let count = channel_subscriptions.len() + pattern_subscriptions.len();
                    write_subscribe_event(&mut write_half, "psubscribe", &pattern, count).await?;
                }
                subscribed_mode = (channel_subscriptions.len() + pattern_subscriptions.len()) > 0;
            }
            Command::Punsubscribe { patterns } => {
                let targets: Vec<String> = if patterns.is_empty() {
                    let mut v: Vec<String> = pattern_subscriptions.keys().cloned().collect();
                    v.sort();
                    v
                } else {
                    patterns
                };

                if targets.is_empty() {
                    let count = channel_subscriptions.len() + pattern_subscriptions.len();
                    write_subscribe_event(&mut write_half, "punsubscribe", "", count).await?;
                } else {
                    for pat in targets {
                        if let Some(handle) = pattern_subscriptions.remove(&pat) {
                            handle.abort();
                            metrics.pubsub_pattern_subs.fetch_sub(1, Ordering::Relaxed);
                        }
                        let count = channel_subscriptions.len() + pattern_subscriptions.len();
                        write_subscribe_event(&mut write_half, "punsubscribe", &pat, count).await?;
                    }
                }
                subscribed_mode = (channel_subscriptions.len() + pattern_subscriptions.len()) > 0;
                pubsub.cleanup_stale();
            }
            Command::PubsubChannels { pattern } => {
                let channels = pubsub.active_channels(pattern.as_deref());
                let header = format!("*{}\r\n", channels.len());
                write_half.write_all(header.as_bytes()).await?;
                for ch in channels {
                    respond_bulk_string(&mut write_half, &ch).await?;
                }
            }
            Command::PubsubNumsub { channels } => {
                let header = format!("*{}\r\n", channels.len() * 2);
                write_half.write_all(header.as_bytes()).await?;
                for ch in channels {
                    respond_bulk_string(&mut write_half, &ch).await?;
                    let subscribers = pubsub.channel_subscribers(&ch);
                    respond_integer(&mut write_half, subscribers as i64).await?;
                }
            }
            Command::PubsubNumpat => {
                let total = pubsub.pattern_subscription_count();
                respond_integer(&mut write_half, total as i64).await?;
            }
            Command::PubsubShardchannels { pattern } => {
                let channels = pubsub.active_shard_channels(pattern.as_deref());
                let header = format!("*{}\r\n", channels.len());
                write_half.write_all(header.as_bytes()).await?;
                for ch in channels {
                    respond_bulk_string(&mut write_half, &ch).await?;
                }
            }
            Command::PubsubShardnumsub { channels } => {
                let header = format!("*{}\r\n", channels.len() * 2);
                write_half.write_all(header.as_bytes()).await?;
                for ch in channels {
                    respond_bulk_string(&mut write_half, &ch).await?;
                    let subscribers = pubsub.shard_channel_subscribers(&ch);
                    respond_integer(&mut write_half, subscribers as i64).await?;
                }
            }
            Command::PubsubHelp => {
                let lines = [
                    "PUBSUB <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                    "CHANNELS [<pattern>] -- Return the currently active channels matching a pattern (default: all).",
                    "NUMSUB [<channel> ...] -- Return the number of subscribers for the specified channels.",
                    "NUMPAT -- Return the number of subscriptions to patterns.",
                    "SHARDCHANNELS [<pattern>] -- Return the currently active shard channels.",
                    "SHARDNUMSUB [<channel> ...] -- Return the number of subscribers for shard channels.",
                ];
                let header = format!("*{}\r\n", lines.len());
                write_half.write_all(header.as_bytes()).await?;
                for line in lines {
                    respond_bulk_string(&mut write_half, line).await?;
                }
            }

            Command::Auth { .. } => {
                unreachable!();
            }

            // 解析阶段构造的错误命令
            Command::Error(msg) => {
                respond_error(&mut write_half, &msg).await?;
            }

            // 未知命令
            Command::Unknown(parts) => {
                let joined = parts
                    .iter()
                    .map(|p| String::from_utf8_lossy(p).into_owned())
                    .collect::<Vec<_>>()
                    .join(" ");
                let msg = format!("ERR unknown command '{}'", joined);
                respond_error(&mut write_half, &msg).await?;
            }
        }
    }

    let chan_len = channel_subscriptions.len();
    for (_, handle) in channel_subscriptions.drain() {
        handle.abort();
    }
    let pat_len = pattern_subscriptions.len();
    for (_, handle) in pattern_subscriptions.drain() {
        handle.abort();
    }
    let shard_len = shard_subscriptions.len();
    for (_, handle) in shard_subscriptions.drain() {
        handle.abort();
    }

    metrics
        .pubsub_channel_subs
        .fetch_sub(chan_len as u64, Ordering::Relaxed);
    metrics
        .pubsub_pattern_subs
        .fetch_sub(pat_len as u64, Ordering::Relaxed);
    metrics
        .pubsub_shard_subs
        .fetch_sub(shard_len as u64, Ordering::Relaxed);

    pubsub.cleanup_stale();

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
        error!("[rdb] failed to load RDB from {}: {}", rdb_path, e);
    } else {
        info!("[rdb] loaded RDB from {} (if existing and valid)", rdb_path);
    }

    storage.spawn_expiration_task();

    if let Ok(interval_str) = env::var("REDUST_RDB_AUTO_SAVE_SECS") {
        if let Ok(secs) = interval_str.parse::<u64>() {
            if secs > 0 {
                let storage_clone = storage.clone();
                let path_clone = rdb_path.clone();
                info!(
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
                                error!("[rdb] auto-save failed: {}", e);
                            }
                            Err(e) => {
                                error!("[rdb] auto-save task panicked: {}", e);
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
        pubsub_channel_subs: AtomicU64::new(0),
        pubsub_pattern_subs: AtomicU64::new(0),
        pubsub_shard_subs: AtomicU64::new(0),
        pubsub_messages_delivered: AtomicU64::new(0),
        pubsub_messages_dropped: AtomicU64::new(0),
    });
    let pubsub = PubSubHub::new();

    if let Ok(metrics_addr) = env::var("REDUST_METRICS_ADDR") {
        if !metrics_addr.is_empty() {
            let storage_clone = storage.clone();
            let metrics_clone = metrics.clone();
            tokio::spawn(async move {
                if let Err(e) =
                    run_metrics_exporter(&metrics_addr, storage_clone, metrics_clone).await
                {
                    error!("[metrics] exporter failed: {}", e);
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
                let pubsub = pubsub.clone();
                let overflow_strategy = match env::var("REDUST_PUBSUB_OVERFLOW") {
                    Ok(v) => match v.to_ascii_lowercase().as_str() {
                        "disconnect" => PubSubOverflowStrategy::Disconnect,
                        _ => PubSubOverflowStrategy::Drop,
                    },
                    Err(_) => PubSubOverflowStrategy::Drop,
                };
                info!("Accepted connection from {}", addr);
                tokio::spawn(async move {
                    if let Err(err) = handle_connection(stream, storage, metrics, pubsub, overflow_strategy).await {
                        error!("Connection error: {}", err);
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

    info!("Redust listening on {}", local_addr);

    serve(listener, shutdown).await
}
