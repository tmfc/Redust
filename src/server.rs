use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::env;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::sync::{
    atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering},
    Arc,
};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use tokio::io::{self, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc};
use tokio::time::{sleep, Duration};

use log::{error, info};

use crate::command::{read_command, Command, CommandError}; // Import CommandError
use crate::resp::{
    respond_bulk_bytes, respond_bulk_string, respond_error, respond_integer, respond_null_bulk,
    respond_simple_string,
};
use crate::scripting::{execute_script, ScriptCache, ScriptContext};
use crate::storage::{MaxmemoryPolicy, Storage};

// 全局客户端 ID 计数器
static CLIENT_ID_COUNTER: AtomicU64 = AtomicU64::new(1);
static CLIENT_TIMEOUT_SECS: AtomicU64 = AtomicU64::new(0);
static TCP_KEEPALIVE_SECS: AtomicU64 = AtomicU64::new(300);
// CLIENT PAUSE 截止时间（毫秒 Unix 时间戳），0 表示未暂停
static CLIENT_PAUSE_UNTIL_MS: AtomicU64 = AtomicU64::new(0);

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

/// 慢日志条目
#[derive(Clone)]
struct SlowLogEntry {
    /// 唯一 ID
    id: u64,
    /// 命令执行的 Unix 时间戳（秒）
    timestamp: u64,
    /// 执行耗时（微秒）
    duration_us: u64,
    /// 命令及参数
    command: Vec<String>,
    /// 客户端地址
    client_addr: String,
    /// 客户端名称
    client_name: String,
}

/// 慢日志管理器
struct SlowLog {
    /// 慢日志条目队列
    entries: std::sync::Mutex<std::collections::VecDeque<SlowLogEntry>>,
    /// 下一个条目 ID
    next_id: AtomicU64,
    /// 慢查询阈值（微秒），超过此值的命令会被记录
    threshold_us: AtomicU64,
    /// 最大条目数
    max_len: AtomicUsize,
}

impl SlowLog {
    fn new() -> Self {
        // 默认阈值 10ms = 10000us，最大 128 条
        let threshold = env::var("REDUST_SLOWLOG_SLOWER_THAN")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10000u64);
        let max_len = env::var("REDUST_SLOWLOG_MAX_LEN")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(128usize);

        SlowLog {
            entries: std::sync::Mutex::new(std::collections::VecDeque::new()),
            next_id: AtomicU64::new(0),
            threshold_us: AtomicU64::new(threshold),
            max_len: AtomicUsize::new(max_len),
        }
    }

    /// 记录一条慢日志（如果耗时超过阈值）
    fn log_if_slow(
        &self,
        duration_us: u64,
        command: Vec<String>,
        client_addr: &str,
        client_name: &str,
    ) {
        let threshold = self.threshold_us.load(Ordering::Relaxed);
        if duration_us < threshold {
            return;
        }

        let entry = SlowLogEntry {
            id: self.next_id.fetch_add(1, Ordering::Relaxed),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            duration_us,
            command,
            client_addr: client_addr.to_string(),
            client_name: client_name.to_string(),
        };

        let max_len = self.max_len.load(Ordering::Relaxed);
        let mut entries = self.entries.lock().unwrap();
        entries.push_front(entry);
        while entries.len() > max_len {
            entries.pop_back();
        }
    }

    /// 获取最近的 N 条慢日志
    fn get(&self, count: Option<usize>) -> Vec<SlowLogEntry> {
        let entries = self.entries.lock().unwrap();
        let n = count.unwrap_or(10).min(entries.len());
        entries.iter().take(n).cloned().collect()
    }

    /// 重置慢日志
    fn reset(&self) {
        let mut entries = self.entries.lock().unwrap();
        entries.clear();
    }

    /// 获取慢日志长度
    fn len(&self) -> usize {
        self.entries.lock().unwrap().len()
    }

    fn set_threshold_us(&self, threshold: u64) {
        self.threshold_us.store(threshold, Ordering::Relaxed);
    }

    fn set_max_len(&self, len: usize) {
        self.max_len.store(len, Ordering::Relaxed);
        // 立即裁剪队列以匹配新限制
        let mut entries = self.entries.lock().unwrap();
        while entries.len() > len {
            entries.pop_back();
        }
    }

    fn threshold_us(&self) -> u64 {
        self.threshold_us.load(Ordering::Relaxed)
    }

    fn max_len(&self) -> usize {
        self.max_len.load(Ordering::Relaxed)
    }
}

struct PersistenceState {
    rdb_path: String,
    aof_path: Option<String>,
    last_save: AtomicI64,
    bgsave_running: AtomicBool,
    enabled: bool,
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

fn parse_maxmemory_policy(input: &str) -> Option<MaxmemoryPolicy> {
    MaxmemoryPolicy::from_str(input)
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

/// 将命令转换为字符串数组用于慢日志记录
fn command_to_strings(cmd: &Command) -> Vec<String> {
    match cmd {
        Command::Ping => vec!["PING".to_string()],
        Command::PingWithPayload(msg) => {
            vec!["PING".to_string(), String::from_utf8_lossy(msg).to_string()]
        }
        Command::Echo(msg) => vec!["ECHO".to_string(), String::from_utf8_lossy(msg).to_string()],
        Command::Set { key, value, .. } => vec![
            "SET".to_string(),
            key.clone(),
            String::from_utf8_lossy(value).to_string(),
        ],
        Command::Get { key } => vec!["GET".to_string(), key.clone()],
        Command::Del { keys } => std::iter::once("DEL".to_string())
            .chain(keys.iter().cloned())
            .collect(),
        Command::Lpush { key, values } => std::iter::once("LPUSH".to_string())
            .chain(std::iter::once(key.clone()))
            .chain(values.iter().cloned())
            .collect(),
        Command::Rpush { key, values } => std::iter::once("RPUSH".to_string())
            .chain(std::iter::once(key.clone()))
            .chain(values.iter().cloned())
            .collect(),
        Command::Hset { key, field, value } => vec![
            "HSET".to_string(),
            key.clone(),
            field.clone(),
            value.clone(),
        ],
        Command::Hget { key, field } => vec!["HGET".to_string(), key.clone(), field.clone()],
        Command::Sadd { key, members } => std::iter::once("SADD".to_string())
            .chain(std::iter::once(key.clone()))
            .chain(members.iter().cloned())
            .collect(),
        Command::Zadd { key, entries, .. } => {
            let mut v = vec!["ZADD".to_string(), key.clone()];
            for (score, member) in entries {
                v.push(score.to_string());
                v.push(member.clone());
            }
            v
        }
        // 对于其他命令，使用 Debug 格式的简化表示
        _ => vec![format!("{:?}", cmd).chars().take(100).collect()],
    }
}

/// 计算键名的哈希值，用于 SCAN 游标
/// 使用稳定的哈希算法确保同一键名总是产生相同的哈希值
fn key_hash(key: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

fn pattern_match(pattern: &str, value: &str) -> bool {
    fn match_set(p: &[u8], ch: u8) -> Option<(bool, usize)> {
        let mut ranges: Vec<(u8, u8)> = Vec::new();
        let mut i = 1; // skip '['

        // 检查是否是取反字符集 [^...]
        let negate = i < p.len() && p[i] == b'^';
        if negate {
            i += 1;
        }

        while i < p.len() {
            if p[i] == b']' {
                let matched = ranges.iter().any(|(s, e)| ch >= *s && ch <= *e);
                // 如果是取反，则反转匹配结果
                let result = if negate { !matched } else { matched };
                return Some((result, i + 1));
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

async fn perform_save(
    storage: Storage,
    path: String,
    persistence: Arc<PersistenceState>,
) -> io::Result<()> {
    let res = tokio::task::spawn_blocking(move || storage.save_rdb(&path)).await;
    match res {
        Ok(Ok(())) => {
            if let Ok(dur) = SystemTime::now().duration_since(UNIX_EPOCH) {
                persistence
                    .last_save
                    .store(dur.as_secs() as i64, Ordering::Relaxed);
            }
            Ok(())
        }
        Ok(Err(e)) => Err(e),
        Err(e) => Err(io::Error::new(
            io::ErrorKind::Other,
            format!("save task failed: {}", e),
        )),
    }
}

fn current_max_value_bytes() -> Option<u64> {
    env::var("REDUST_MAXVALUE_BYTES")
        .ok()
        .and_then(|s| parse_maxmemory_bytes(&s))
        .filter(|v| *v > 0)
}

fn file_mtime_seconds(path: &str) -> Option<i64> {
    let meta = std::fs::metadata(path).ok()?;
    let modified = meta.modified().ok()?;
    let dur = modified.duration_since(UNIX_EPOCH).ok()?;
    Some(dur.as_secs() as i64)
}

fn quarantine_corrupt_file(path: &str) -> Option<String> {
    let ts = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();
    let new_path = format!("{}.corrupt.{}", path, ts);
    std::fs::rename(path, &new_path).ok()?;
    Some(new_path)
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
            expire_at_millis,
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
                // 先根据 EX/PX/EXAT/PXAT 计算“显式指定”的 TTL；存在显式 TTL 时优先级高于 KEEPTTL。
                let explicit_ttl_ms: Option<i64> = if let Some(at_ms) = expire_at_millis {
                    use std::time::{SystemTime, UNIX_EPOCH};
                    let now_ms = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as i64;
                    Some(at_ms.saturating_sub(now_ms))
                } else {
                    expire_millis
                };

                let expires_at_instant = explicit_ttl_ms.or(keep_ttl_ms).and_then(|ms| {
                    if ms > 0 {
                        Some(Instant::now() + Duration::from_millis(ms as u64))
                    } else {
                        None
                    }
                });

                // 写入新值
                if let Err(e) = storage.set_with_expiry(physical.clone(), value, expires_at_instant)
                {
                    match e {
                        crate::storage::StorageError::Oom => {
                            respond_error(
                                writer,
                                "OOM command not allowed when used memory > 'maxmemory'.",
                            )
                            .await?;
                        }
                        crate::storage::StorageError::WrongType => {
                            respond_error(
                                writer,
                                "WRONGTYPE Operation against a key holding the wrong kind of value",
                            )
                            .await?;
                        }
                    }
                    return Ok(());
                }

                // TTL 处理优先级：
                // 1. 显式 TTL（EX/PX/EXAT/PXAT）：覆盖旧 TTL
                // 2. KEEPTTL：在无显式 TTL 时，恢复旧 TTL（如果存在）
                // 3. 否则：清除已有 TTL
                if let Some(ms) = explicit_ttl_ms {
                    // Redis 语义：到期时间在“现在及以前”视为立即过期
                    storage.expire_millis(&physical, ms);
                } else if let Some(ms) = keep_ttl_ms {
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
                Err(crate::storage::IncrFloatError::Oom) => {
                    respond_error(
                        writer,
                        "OOM command not allowed when used memory > 'maxmemory'.",
                    )
                    .await?;
                }
                Err(crate::storage::IncrFloatError::WrongType) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
                }
                Err(crate::storage::IncrFloatError::NotFloat) => {
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
                Err(crate::storage::StorageError::Oom) => {
                    respond_error(
                        writer,
                        "OOM command not allowed when used memory > 'maxmemory'.",
                    )
                    .await?;
                }
                Err(crate::storage::StorageError::WrongType) => {
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
                Err(crate::storage::StorageError::Oom) => {
                    respond_error(
                        writer,
                        "OOM command not allowed when used memory > 'maxmemory'.",
                    )
                    .await?;
                }
                Err(crate::storage::StorageError::WrongType) => {
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
                Err(crate::storage::StorageError::Oom) => {
                    respond_error(
                        writer,
                        "OOM command not allowed when used memory > 'maxmemory'.",
                    )
                    .await?;
                }
                Err(crate::storage::StorageError::WrongType) => {
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
                Err(crate::storage::IncrError::Oom) => {
                    respond_error(
                        writer,
                        "OOM command not allowed when used memory > 'maxmemory'.",
                    )
                    .await?;
                }
                Err(crate::storage::IncrError::NotInteger) => {
                    respond_error(writer, "ERR value is not an integer or out of range").await?;
                }
                Err(crate::storage::IncrError::WrongType) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
                }
            }
        }
        Command::Decr { key } => {
            let physical = prefix_key(current_db, &key);
            match storage.decr(&physical) {
                Ok(value) => {
                    respond_integer(writer, value).await?;
                }
                Err(crate::storage::IncrError::Oom) => {
                    respond_error(
                        writer,
                        "OOM command not allowed when used memory > 'maxmemory'.",
                    )
                    .await?;
                }
                Err(crate::storage::IncrError::NotInteger) => {
                    respond_error(writer, "ERR value is not an integer or out of range").await?;
                }
                Err(crate::storage::IncrError::WrongType) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
                }
            }
        }
        Command::Incrby { key, delta } => {
            let physical = prefix_key(current_db, &key);
            match storage.incr_by(&physical, delta) {
                Ok(value) => {
                    respond_integer(writer, value).await?;
                }
                Err(crate::storage::IncrError::Oom) => {
                    respond_error(
                        writer,
                        "OOM command not allowed when used memory > 'maxmemory'.",
                    )
                    .await?;
                }
                Err(crate::storage::IncrError::NotInteger) => {
                    respond_error(writer, "ERR value is not an integer or out of range").await?;
                }
                Err(crate::storage::IncrError::WrongType) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
                }
            }
        }
        Command::Decrby { key, delta } => {
            let physical = prefix_key(current_db, &key);
            match storage.incr_by(&physical, -delta) {
                Ok(value) => {
                    respond_integer(writer, value).await?;
                }
                Err(crate::storage::IncrError::Oom) => {
                    respond_error(
                        writer,
                        "OOM command not allowed when used memory > 'maxmemory'.",
                    )
                    .await?;
                }
                Err(crate::storage::IncrError::NotInteger) => {
                    respond_error(writer, "ERR value is not an integer or out of range").await?;
                }
                Err(crate::storage::IncrError::WrongType) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
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
        Command::Unlink { keys } => {
            let physical: Vec<String> = keys
                .into_iter()
                .map(|k| prefix_key(current_db, &k))
                .collect();
            let removed = storage.unlink(&physical);
            respond_integer(writer, removed as i64).await?;
        }
        Command::Copy {
            source,
            destination,
            replace,
        } => {
            let src_physical = prefix_key(current_db, &source);
            let dst_physical = prefix_key(current_db, &destination);
            match storage.copy(&src_physical, &dst_physical, replace) {
                Ok(true) => respond_integer(writer, 1).await?,
                Ok(false) => respond_integer(writer, 0).await?,
                Err(()) => {
                    respond_error(writer, "ERR source and destination objects are the same").await?
                }
            }
        }
        Command::Touch { keys } => {
            let physical: Vec<String> = keys
                .into_iter()
                .map(|k| prefix_key(current_db, &k))
                .collect();
            let count = storage.touch(&physical);
            respond_integer(writer, count as i64).await?;
        }
        Command::ObjectEncoding { key } => {
            let physical = prefix_key(current_db, &key);
            match storage.object_encoding(&physical) {
                Some(encoding) => respond_bulk_string(writer, encoding).await?,
                None => respond_null_bulk(writer).await?,
            }
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
            match storage.mset(&mapped) {
                Ok(()) => respond_simple_string(writer, "OK").await?,
                Err(crate::storage::StorageError::Oom) => {
                    respond_error(
                        writer,
                        "OOM command not allowed when used memory > 'maxmemory'.",
                    )
                    .await?;
                }
                Err(crate::storage::StorageError::WrongType) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
                }
            }
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

            match storage.msetnx(&mapped) {
                Ok(ok) => {
                    let v = if ok { 1 } else { 0 };
                    respond_integer(writer, v).await?;
                }
                Err(crate::storage::StorageError::Oom) => {
                    respond_error(
                        writer,
                        "OOM command not allowed when used memory > 'maxmemory'.",
                    )
                    .await?;
                }
                Err(crate::storage::StorageError::WrongType) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
                }
            }
        }
        Command::Setnx { key, value } => {
            let physical = prefix_key(current_db, &key);
            if let Some(limit) = current_max_value_bytes() {
                if (value.len() as u64) > limit {
                    respond_error(writer, "ERR value exceeds REDUST_MAXVALUE_BYTES").await?;
                    return Ok(());
                }
            }
            match storage.setnx(&physical, value) {
                Ok(inserted) => {
                    let v = if inserted { 1 } else { 0 };
                    respond_integer(writer, v).await?;
                }
                Err(crate::storage::StorageError::Oom) => {
                    respond_error(
                        writer,
                        "OOM command not allowed when used memory > 'maxmemory'.",
                    )
                    .await?;
                }
                Err(crate::storage::StorageError::WrongType) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
                }
            }
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
            match storage.set_with_expire_seconds(physical, value, seconds) {
                Ok(()) => respond_simple_string(writer, "OK").await?,
                Err(crate::storage::StorageError::Oom) => {
                    respond_error(
                        writer,
                        "OOM command not allowed when used memory > 'maxmemory'.",
                    )
                    .await?;
                }
                Err(crate::storage::StorageError::WrongType) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
                }
            }
        }
        Command::Psetex { key, millis, value } => {
            let physical = prefix_key(current_db, &key);
            if let Some(limit) = current_max_value_bytes() {
                if (value.len() as u64) > limit {
                    respond_error(writer, "ERR value exceeds REDUST_MAXVALUE_BYTES").await?;
                    return Ok(());
                }
            }
            match storage.set_with_expire_millis(physical, value, millis) {
                Ok(()) => respond_simple_string(writer, "OK").await?,
                Err(crate::storage::StorageError::Oom) => {
                    respond_error(
                        writer,
                        "OOM command not allowed when used memory > 'maxmemory'.",
                    )
                    .await?;
                }
                Err(crate::storage::StorageError::WrongType) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
                }
            }
        }
        // HyperLogLog 命令
        Command::Pfadd { key, elements } => {
            let physical = prefix_key(current_db, &key);
            match storage.pfadd(&physical, &elements) {
                Ok(changed) => {
                    respond_integer(writer, changed).await?;
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
        Command::Pfcount { keys } => {
            let physical_keys: Vec<String> =
                keys.iter().map(|k| prefix_key(current_db, k)).collect();
            match storage.pfcount(&physical_keys) {
                Ok(count) => {
                    respond_integer(writer, count as i64).await?;
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
        Command::Pfmerge {
            destkey,
            sourcekeys,
        } => {
            let physical_dest = prefix_key(current_db, &destkey);
            let physical_sources: Vec<String> = sourcekeys
                .iter()
                .map(|k| prefix_key(current_db, k))
                .collect();
            match storage.pfmerge(&physical_dest, &physical_sources) {
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

async fn handle_meta_command(
    cmd: Command,
    storage: &Storage,
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    current_db: u8,
) -> io::Result<()> {
    match cmd {
        Command::Type { key } => {
            let physical = prefix_key(current_db, &key);
            let t = storage.type_of(&physical);
            respond_simple_string(writer, &t).await?;
        }
        Command::Keys { pattern } => {
            let all = storage.keys("*");
            let prefix = format!("{}:", current_db);
            let mut logical_keys: Vec<String> = Vec::new();
            for k in all {
                if let Some(rest) = k.strip_prefix(&prefix) {
                    logical_keys.push(rest.to_string());
                }
            }

            let mut result: Vec<String> = Vec::new();
            for k in logical_keys {
                if pattern_match(&pattern, &k) {
                    result.push(k);
                }
            }

            let mut response = format!("*{}\r\n", result.len());
            for k in result {
                response.push_str(&format!("${}\r\n{}\r\n", k.len(), k));
            }
            writer.write_all(response.as_bytes()).await?;
        }
        Command::Scan {
            cursor,
            pattern,
            count,
            type_filter,
        } => {
            let all = storage.keys("*");
            let prefix = format!("{}:", current_db);

            // 收集逻辑键及其哈希值，按哈希值排序
            let mut keyed: Vec<(u64, String)> = Vec::new();
            for k in all {
                if let Some(rest) = k.strip_prefix(&prefix) {
                    let hash = key_hash(rest);
                    keyed.push((hash, rest.to_string()));
                }
            }
            keyed.sort_by_key(|(h, _)| *h);

            let batch_size = count.unwrap_or(10) as usize;
            let pat = pattern.unwrap_or_else(|| "*".to_string());
            let mut matched: Vec<String> = Vec::new();
            let mut max_hash: u64 = 0;
            let mut found_any = false;

            for (hash, k) in &keyed {
                // 跳过哈希值小于游标的键
                if *hash < cursor {
                    continue;
                }

                // 检查模式匹配
                if !pattern_match(&pat, k) {
                    // 即使不匹配也要更新 max_hash 以推进游标
                    if !found_any || *hash > max_hash {
                        max_hash = *hash;
                        found_any = true;
                    }
                    continue;
                }

                // 检查类型过滤
                if let Some(ref type_f) = type_filter {
                    let physical = prefix_key(current_db, k);
                    let key_type = storage.type_of(&physical);
                    if key_type == "none" || key_type.to_ascii_lowercase() != *type_f {
                        if !found_any || *hash > max_hash {
                            max_hash = *hash;
                            found_any = true;
                        }
                        continue;
                    }
                }

                matched.push(k.clone());
                if !found_any || *hash > max_hash {
                    max_hash = *hash;
                    found_any = true;
                }

                // 达到批次大小后停止
                if matched.len() >= batch_size {
                    break;
                }
            }

            // 计算下一个游标
            // 如果还有更多键（哈希值 > max_hash），返回 max_hash + 1
            // 否则返回 0 表示扫描完成
            let next_cursor = if found_any {
                let has_more = keyed.iter().any(|(h, _)| *h > max_hash);
                if has_more {
                    max_hash.saturating_add(1)
                } else {
                    0
                }
            } else {
                0
            };

            let cursor_str = next_cursor.to_string();
            let mut response = format!(
                "*2\r\n${}\r\n{}\r\n*{}\r\n",
                cursor_str.len(),
                cursor_str,
                matched.len()
            );
            for k in matched {
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
        _ => {
            respond_error(writer, "ERR command not supported in transaction").await?;
        }
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
                Err(crate::storage::StorageError::Oom) => {
                    respond_error(
                        writer,
                        "OOM command not allowed when used memory > 'maxmemory'.",
                    )
                    .await?;
                }
                Err(crate::storage::StorageError::WrongType) => {
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
                Err(crate::storage::StorageError::Oom) => {
                    respond_error(
                        writer,
                        "OOM command not allowed when used memory > 'maxmemory'.",
                    )
                    .await?;
                }
                Err(crate::storage::StorageError::WrongType) => {
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
        Command::Blpop { keys, timeout } => {
            let physical_keys: Vec<String> =
                keys.iter().map(|k| prefix_key(current_db, k)).collect();

            // 先尝试立即获取
            for (i, physical) in physical_keys.iter().enumerate() {
                match storage.lpop(physical) {
                    Ok(Some(value)) => {
                        // 返回 [key, value] 数组
                        let key = &keys[i];
                        let response = format!(
                            "*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                            key.len(),
                            key,
                            value.len(),
                            value
                        );
                        writer.write_all(response.as_bytes()).await?;
                        return Ok(());
                    }
                    Ok(None) => continue,
                    Err(()) => {
                        respond_error(
                            writer,
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        )
                        .await?;
                        return Ok(());
                    }
                }
            }

            // 如果 timeout 为 0，无限等待；否则等待指定时间
            let timeout_duration = if timeout == 0.0 {
                None
            } else {
                Some(std::time::Duration::from_secs_f64(timeout))
            };

            let poll_interval = std::time::Duration::from_millis(100);
            let start = std::time::Instant::now();

            loop {
                // 检查是否超时
                if let Some(timeout_dur) = timeout_duration {
                    if start.elapsed() >= timeout_dur {
                        // BLPOP/BRPOP 超时返回 null array (*-1)，而非 null bulk ($-1)
                        writer.write_all(b"*-1\r\n").await?;
                        return Ok(());
                    }
                }

                // 等待一小段时间后重试
                tokio::time::sleep(poll_interval).await;

                // 再次尝试获取
                for (i, physical) in physical_keys.iter().enumerate() {
                    match storage.lpop(physical) {
                        Ok(Some(value)) => {
                            let key = &keys[i];
                            let response = format!(
                                "*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                                key.len(),
                                key,
                                value.len(),
                                value
                            );
                            writer.write_all(response.as_bytes()).await?;
                            return Ok(());
                        }
                        Ok(None) => continue,
                        Err(()) => {
                            respond_error(
                                writer,
                                "WRONGTYPE Operation against a key holding the wrong kind of value",
                            )
                            .await?;
                            return Ok(());
                        }
                    }
                }
            }
        }
        Command::Brpop { keys, timeout } => {
            let physical_keys: Vec<String> =
                keys.iter().map(|k| prefix_key(current_db, k)).collect();

            // 先尝试立即获取
            for (i, physical) in physical_keys.iter().enumerate() {
                match storage.rpop(physical) {
                    Ok(Some(value)) => {
                        let key = &keys[i];
                        let response = format!(
                            "*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                            key.len(),
                            key,
                            value.len(),
                            value
                        );
                        writer.write_all(response.as_bytes()).await?;
                        return Ok(());
                    }
                    Ok(None) => continue,
                    Err(()) => {
                        respond_error(
                            writer,
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        )
                        .await?;
                        return Ok(());
                    }
                }
            }

            let timeout_duration = if timeout == 0.0 {
                None
            } else {
                Some(std::time::Duration::from_secs_f64(timeout))
            };

            let poll_interval = std::time::Duration::from_millis(100);
            let start = std::time::Instant::now();

            loop {
                if let Some(timeout_dur) = timeout_duration {
                    if start.elapsed() >= timeout_dur {
                        // BLPOP/BRPOP 超时返回 null array (*-1)，而非 null bulk ($-1)
                        writer.write_all(b"*-1\r\n").await?;
                        return Ok(());
                    }
                }

                tokio::time::sleep(poll_interval).await;

                for (i, physical) in physical_keys.iter().enumerate() {
                    match storage.rpop(physical) {
                        Ok(Some(value)) => {
                            let key = &keys[i];
                            let response = format!(
                                "*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                                key.len(),
                                key,
                                value.len(),
                                value
                            );
                            writer.write_all(response.as_bytes()).await?;
                            return Ok(());
                        }
                        Ok(None) => continue,
                        Err(()) => {
                            respond_error(
                                writer,
                                "WRONGTYPE Operation against a key holding the wrong kind of value",
                            )
                            .await?;
                            return Ok(());
                        }
                    }
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
        Command::Lset { key, index, value } => {
            let physical = prefix_key(current_db, &key);
            match storage.lset(&physical, index, value) {
                Ok(()) => {
                    respond_simple_string(writer, "OK").await?;
                }
                Err(crate::storage::LsetError::WrongType) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
                }
                Err(crate::storage::LsetError::NoSuchKey) => {
                    respond_error(writer, "ERR no such key").await?;
                }
                Err(crate::storage::LsetError::OutOfRange) => {
                    respond_error(writer, "ERR index out of range").await?;
                }
            }
        }
        Command::Linsert {
            key,
            before,
            pivot,
            value,
        } => {
            let physical = prefix_key(current_db, &key);
            match storage.linsert(&physical, before, &pivot, value) {
                Ok(len) => {
                    respond_integer(writer, len as i64).await?;
                }
                Err(crate::storage::StorageError::Oom) => {
                    respond_error(
                        writer,
                        "OOM command not allowed when used memory > 'maxmemory'.",
                    )
                    .await?;
                }
                Err(crate::storage::StorageError::WrongType) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
                }
            }
        }
        Command::Rpoplpush {
            source,
            destination,
        } => {
            let src_physical = prefix_key(current_db, &source);
            let dst_physical = prefix_key(current_db, &destination);
            match storage.rpoplpush(&src_physical, &dst_physical) {
                Ok(Some(value)) => {
                    respond_bulk_string(writer, &value).await?;
                }
                Ok(None) => {
                    respond_null_bulk(writer).await?;
                }
                Err(crate::storage::StorageError::Oom) => {
                    respond_error(
                        writer,
                        "OOM command not allowed when used memory > 'maxmemory'.",
                    )
                    .await?;
                }
                Err(crate::storage::StorageError::WrongType) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
                }
            }
        }
        Command::Lpos {
            key,
            element,
            rank,
            count,
            maxlen,
        } => {
            let physical = prefix_key(current_db, &key);
            match storage.lpos(&physical, &element, rank, count, maxlen) {
                Ok(positions) => {
                    if count.is_some() {
                        // 返回数组
                        let mut response = format!("*{}\r\n", positions.len());
                        for pos in positions {
                            response.push_str(&format!(":{}\r\n", pos));
                        }
                        writer.write_all(response.as_bytes()).await?;
                    } else {
                        // 返回单个值或 null
                        if let Some(&pos) = positions.first() {
                            respond_integer(writer, pos as i64).await?;
                        } else {
                            respond_null_bulk(writer).await?;
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
        Command::Hscan {
            key,
            cursor: _,
            pattern,
            count: _,
            novalues,
        } => {
            let physical = prefix_key(current_db, &key);
            match storage.hgetall(&physical) {
                Ok(entries) => {
                    // entries: Vec<(String, String)>
                    let pat = pattern.unwrap_or_else(|| "*".to_string());
                    let mut flat: Vec<(String, String)> = Vec::new();
                    for (field, value) in &entries {
                        if pattern_match(&pat, field) {
                            flat.push((field.to_string(), value.clone()));
                        }
                    }

                    let next_cursor = 0u64;
                    let cursor_str = next_cursor.to_string();

                    let array_len = if novalues { flat.len() } else { flat.len() * 2 };
                    let mut response = format!(
                        "*2\r\n${}\r\n{}\r\n*{}\r\n",
                        cursor_str.len(),
                        cursor_str,
                        array_len
                    );
                    for (f, v) in flat {
                        response.push_str(&format!("${}\r\n{}\r\n", f.len(), f));
                        if !novalues {
                            response.push_str(&format!("${}\r\n{}\r\n", v.len(), v));
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
                Err(crate::storage::StorageError::Oom) => {
                    respond_error(
                        writer,
                        "OOM command not allowed when used memory > 'maxmemory'.",
                    )
                    .await?;
                }
                Err(crate::storage::StorageError::WrongType) => {
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
        Command::Sscan {
            key,
            cursor,
            pattern,
            count,
        } => {
            let physical = prefix_key(current_db, &key);
            // 复用 smembers 提供的有序成员视图
            match storage.smembers(&physical) {
                Ok(members) => {
                    let total = members.len() as u64;
                    let start = if cursor > total { total } else { cursor } as usize;
                    let batch_size = count.unwrap_or(10) as usize;
                    let end = std::cmp::min(start + batch_size, members.len());

                    let pat = pattern.unwrap_or_else(|| "*".to_string());
                    let mut matched: Vec<String> = Vec::new();
                    for m in &members[start..end] {
                        if pattern_match(&pat, m) {
                            matched.push(m.to_string());
                        }
                    }

                    let next_cursor = if end >= members.len() { 0 } else { end as u64 };

                    let cursor_str = next_cursor.to_string();
                    let mut response = format!(
                        "*2\r\n${}\r\n{}\r\n*{}\r\n",
                        cursor_str.len(),
                        cursor_str,
                        matched.len()
                    );
                    for m in matched {
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
                Err(crate::storage::StorageError::Oom) => {
                    respond_error(
                        writer,
                        "OOM command not allowed when used memory > 'maxmemory'.",
                    )
                    .await?;
                }
                Err(crate::storage::StorageError::WrongType) => {
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
                Err(crate::storage::StorageError::Oom) => {
                    respond_error(
                        writer,
                        "OOM command not allowed when used memory > 'maxmemory'.",
                    )
                    .await?;
                }
                Err(crate::storage::StorageError::WrongType) => {
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
                Err(crate::storage::StorageError::Oom) => {
                    respond_error(
                        writer,
                        "OOM command not allowed when used memory > 'maxmemory'.",
                    )
                    .await?;
                }
                Err(crate::storage::StorageError::WrongType) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
                }
            }
        }
        Command::Smove {
            source,
            destination,
            member,
        } => {
            let src_physical = prefix_key(current_db, &source);
            let dst_physical = prefix_key(current_db, &destination);
            match storage.smove(&src_physical, &dst_physical, &member) {
                Ok(true) => respond_integer(writer, 1).await?,
                Ok(false) => respond_integer(writer, 0).await?,
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
                Err(crate::storage::StorageError::Oom) => {
                    respond_error(
                        writer,
                        "OOM command not allowed when used memory > 'maxmemory'.",
                    )
                    .await?;
                }
                Err(crate::storage::StorageError::WrongType) => {
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
                Err(crate::storage::HincrError::Oom) => {
                    respond_error(
                        writer,
                        "OOM command not allowed when used memory > 'maxmemory'.",
                    )
                    .await?;
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
                Err(crate::storage::HincrFloatError::Oom) => {
                    respond_error(
                        writer,
                        "OOM command not allowed when used memory > 'maxmemory'.",
                    )
                    .await?;
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
        Command::Hsetnx { key, field, value } => {
            let physical = prefix_key(current_db, &key);
            match storage.hsetnx(&physical, &field, value) {
                Ok(result) => {
                    respond_integer(writer, result as i64).await?;
                }
                Err(crate::storage::StorageError::Oom) => {
                    respond_error(
                        writer,
                        "OOM command not allowed when used memory > 'maxmemory'.",
                    )
                    .await?;
                }
                Err(crate::storage::StorageError::WrongType) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
                }
            }
        }
        Command::Hstrlen { key, field } => {
            let physical = prefix_key(current_db, &key);
            match storage.hstrlen(&physical, &field) {
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
        Command::Hmset { key, field_values } => {
            let physical = prefix_key(current_db, &key);
            match storage.hmset(&physical, &field_values) {
                Ok(()) => {
                    respond_simple_string(writer, "OK").await?;
                }
                Err(crate::storage::StorageError::Oom) => {
                    respond_error(
                        writer,
                        "OOM command not allowed when used memory > 'maxmemory'.",
                    )
                    .await?;
                }
                Err(crate::storage::StorageError::WrongType) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
                }
            }
        }
        Command::Hscan {
            key,
            cursor,
            pattern,
            count,
            novalues,
        } => {
            let physical = prefix_key(current_db, &key);
            match storage.hgetall(&physical) {
                Ok(entries) => {
                    let total = entries.len() as u64;
                    let start = if cursor > total { total } else { cursor } as usize;
                    let batch_size = count.unwrap_or(10) as usize;
                    let end = std::cmp::min(start + batch_size, entries.len());

                    let pat = pattern.unwrap_or_else(|| "*".to_string());
                    let mut flat: Vec<(String, String)> = Vec::new();
                    for (field, value) in &entries[start..end] {
                        if pattern_match(&pat, field) {
                            flat.push((field.clone(), value.clone()));
                        }
                    }

                    let next_cursor = if end >= entries.len() { 0 } else { end as u64 };

                    let cursor_str = next_cursor.to_string();

                    // NOVALUES: 只返回 field，不返回 value
                    let array_len = if novalues { flat.len() } else { flat.len() * 2 };
                    let mut response = format!(
                        "*2\r\n${}\r\n{}\r\n*{}\r\n",
                        cursor_str.len(),
                        cursor_str,
                        array_len
                    );
                    for (f, v) in flat {
                        response.push_str(&format!("${}\r\n{}\r\n", f.len(), f));
                        if !novalues {
                            response.push_str(&format!("${}\r\n{}\r\n", v.len(), v));
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
        _ => {}
    }

    Ok(())
}

/// 将 ZRangeBound 转换为 (f64, exclusive) 元组
fn zrange_bound_to_f64(bound: &crate::command::ZRangeBound) -> (f64, bool) {
    use crate::command::ZRangeBound;
    match bound {
        ZRangeBound::NegInf => (f64::NEG_INFINITY, false),
        ZRangeBound::PosInf => (f64::INFINITY, false),
        ZRangeBound::Inclusive(v) => (*v, false),
        ZRangeBound::Exclusive(v) => (*v, true),
    }
}

/// 将 ZLexBound 转换为 (value, inclusive, unbounded) 元组
fn zlex_bound_to_params(bound: &crate::command::ZLexBound) -> (String, bool, bool) {
    use crate::command::ZLexBound;
    match bound {
        ZLexBound::NegInf => (String::new(), false, true),
        ZLexBound::PosInf => (String::new(), false, true),
        ZLexBound::Inclusive(v) => (v.clone(), true, false),
        ZLexBound::Exclusive(v) => (v.clone(), false, false),
    }
}

async fn handle_zset_command(
    cmd: Command,
    storage: &Storage,
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    current_db: u8,
) -> io::Result<()> {
    // helper to format floating scores similar to Redis (trim trailing zeros)
    let format_score = |score: f64| {
        let mut s = score.to_string();
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

    match cmd {
        Command::Zadd { key, entries } => {
            let physical = prefix_key(current_db, &key);
            match storage.zadd(&physical, &entries) {
                Ok(added) => {
                    respond_integer(writer, added as i64).await?;
                }
                Err(crate::storage::ZsetError::Oom) => {
                    respond_error(
                        writer,
                        "OOM command not allowed when used memory > 'maxmemory'.",
                    )
                    .await?;
                }
                Err(crate::storage::ZsetError::WrongType) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
                }
                Err(crate::storage::ZsetError::NotFloat) => {
                    respond_error(writer, "ERR value is not a valid float").await?;
                }
            }
        }
        Command::Zcard { key } => {
            let physical = prefix_key(current_db, &key);
            match storage.zcard(&physical) {
                Ok(len) => respond_integer(writer, len as i64).await?,
                Err(()) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
                }
            }
        }
        Command::Zrange {
            key,
            start,
            stop,
            withscores,
            rev,
        } => {
            let physical = prefix_key(current_db, &key);
            match storage.zrange(&physical, start, stop, rev) {
                Ok(items) => {
                    let mut response = if withscores {
                        format!("*{}\r\n", items.len() * 2)
                    } else {
                        format!("*{}\r\n", items.len())
                    };
                    for (member, score) in items {
                        response.push_str(&format!("${}\r\n{}\r\n", member.len(), member));
                        if withscores {
                            let score_s = format_score(score);
                            response.push_str(&format!("${}\r\n{}\r\n", score_s.len(), score_s));
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
        Command::Zscore { key, member } => {
            let physical = prefix_key(current_db, &key);
            match storage.zscore(&physical, &member) {
                Ok(Some(score)) => {
                    let s = format_score(score);
                    respond_bulk_string(writer, &s).await?;
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
        Command::Zrem { key, members } => {
            let physical = prefix_key(current_db, &key);
            match storage.zrem(&physical, &members) {
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
        Command::Zincrby {
            key,
            increment,
            member,
        } => {
            let physical = prefix_key(current_db, &key);
            match storage.zincrby(&physical, increment, &member) {
                Ok(score) => {
                    let s = format_score(score);
                    respond_bulk_string(writer, &s).await?;
                }
                Err(crate::storage::ZsetError::Oom) => {
                    respond_error(
                        writer,
                        "OOM command not allowed when used memory > 'maxmemory'.",
                    )
                    .await?;
                }
                Err(crate::storage::ZsetError::WrongType) => {
                    respond_error(
                        writer,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )
                    .await?;
                }
                Err(crate::storage::ZsetError::NotFloat) => {
                    respond_error(writer, "ERR value is not a valid float").await?;
                }
            }
        }
        Command::Zcount { key, min, max } => {
            let physical = prefix_key(current_db, &key);
            let (min_val, min_excl) = zrange_bound_to_f64(&min);
            let (max_val, max_excl) = zrange_bound_to_f64(&max);
            match storage.zcount(&physical, min_val, min_excl, max_val, max_excl) {
                Ok(count) => {
                    respond_integer(writer, count as i64).await?;
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
        Command::Zrank { key, member } => {
            let physical = prefix_key(current_db, &key);
            match storage.zrank(&physical, &member) {
                Ok(Some(rank)) => {
                    respond_integer(writer, rank as i64).await?;
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
        Command::Zrevrank { key, member } => {
            let physical = prefix_key(current_db, &key);
            match storage.zrevrank(&physical, &member) {
                Ok(Some(rank)) => {
                    respond_integer(writer, rank as i64).await?;
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
        Command::Zpopmin { key, count } => {
            let physical = prefix_key(current_db, &key);
            let cnt = count.unwrap_or(1);
            match storage.zpopmin(&physical, cnt) {
                Ok(items) => {
                    let mut response = format!("*{}\r\n", items.len() * 2);
                    for (member, score) in items {
                        let score_s = format_score(score);
                        response.push_str(&format!("${}\r\n{}\r\n", member.len(), member));
                        response.push_str(&format!("${}\r\n{}\r\n", score_s.len(), score_s));
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
        Command::Zpopmax { key, count } => {
            let physical = prefix_key(current_db, &key);
            let cnt = count.unwrap_or(1);
            match storage.zpopmax(&physical, cnt) {
                Ok(items) => {
                    let mut response = format!("*{}\r\n", items.len() * 2);
                    for (member, score) in items {
                        let score_s = format_score(score);
                        response.push_str(&format!("${}\r\n{}\r\n", member.len(), member));
                        response.push_str(&format!("${}\r\n{}\r\n", score_s.len(), score_s));
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
        Command::Zinter {
            keys,
            weights,
            aggregate,
            withscores,
        } => {
            let physical_keys: Vec<String> =
                keys.iter().map(|k| prefix_key(current_db, k)).collect();
            let agg = aggregate
                .map(|a| match a {
                    crate::command::ZAggregate::Sum => crate::storage::ZsetAggregate::Sum,
                    crate::command::ZAggregate::Min => crate::storage::ZsetAggregate::Min,
                    crate::command::ZAggregate::Max => crate::storage::ZsetAggregate::Max,
                })
                .unwrap_or(crate::storage::ZsetAggregate::Sum);
            match storage.zinter(&physical_keys, weights.as_deref(), agg) {
                Ok(items) => {
                    if withscores {
                        let mut response = format!("*{}\r\n", items.len() * 2);
                        for (member, score) in items {
                            let score_s = format_score(score);
                            response.push_str(&format!("${}\r\n{}\r\n", member.len(), member));
                            response.push_str(&format!("${}\r\n{}\r\n", score_s.len(), score_s));
                        }
                        writer.write_all(response.as_bytes()).await?;
                    } else {
                        let mut response = format!("*{}\r\n", items.len());
                        for (member, _) in items {
                            response.push_str(&format!("${}\r\n{}\r\n", member.len(), member));
                        }
                        writer.write_all(response.as_bytes()).await?;
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
        Command::Zunion {
            keys,
            weights,
            aggregate,
            withscores,
        } => {
            let physical_keys: Vec<String> =
                keys.iter().map(|k| prefix_key(current_db, k)).collect();
            let agg = aggregate
                .map(|a| match a {
                    crate::command::ZAggregate::Sum => crate::storage::ZsetAggregate::Sum,
                    crate::command::ZAggregate::Min => crate::storage::ZsetAggregate::Min,
                    crate::command::ZAggregate::Max => crate::storage::ZsetAggregate::Max,
                })
                .unwrap_or(crate::storage::ZsetAggregate::Sum);
            match storage.zunion(&physical_keys, weights.as_deref(), agg) {
                Ok(items) => {
                    if withscores {
                        let mut response = format!("*{}\r\n", items.len() * 2);
                        for (member, score) in items {
                            let score_s = format_score(score);
                            response.push_str(&format!("${}\r\n{}\r\n", member.len(), member));
                            response.push_str(&format!("${}\r\n{}\r\n", score_s.len(), score_s));
                        }
                        writer.write_all(response.as_bytes()).await?;
                    } else {
                        let mut response = format!("*{}\r\n", items.len());
                        for (member, _) in items {
                            response.push_str(&format!("${}\r\n{}\r\n", member.len(), member));
                        }
                        writer.write_all(response.as_bytes()).await?;
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
        Command::Zdiff { keys, withscores } => {
            let physical_keys: Vec<String> =
                keys.iter().map(|k| prefix_key(current_db, k)).collect();
            match storage.zdiff(&physical_keys) {
                Ok(items) => {
                    if withscores {
                        let mut response = format!("*{}\r\n", items.len() * 2);
                        for (member, score) in items {
                            let score_s = format_score(score);
                            response.push_str(&format!("${}\r\n{}\r\n", member.len(), member));
                            response.push_str(&format!("${}\r\n{}\r\n", score_s.len(), score_s));
                        }
                        writer.write_all(response.as_bytes()).await?;
                    } else {
                        let mut response = format!("*{}\r\n", items.len());
                        for (member, _) in items {
                            response.push_str(&format!("${}\r\n{}\r\n", member.len(), member));
                        }
                        writer.write_all(response.as_bytes()).await?;
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
        Command::Zinterstore {
            destination,
            keys,
            weights,
            aggregate,
        } => {
            let dest_physical = prefix_key(current_db, &destination);
            let physical_keys: Vec<String> =
                keys.iter().map(|k| prefix_key(current_db, k)).collect();
            let agg = aggregate
                .map(|a| match a {
                    crate::command::ZAggregate::Sum => crate::storage::ZsetAggregate::Sum,
                    crate::command::ZAggregate::Min => crate::storage::ZsetAggregate::Min,
                    crate::command::ZAggregate::Max => crate::storage::ZsetAggregate::Max,
                })
                .unwrap_or(crate::storage::ZsetAggregate::Sum);
            match storage.zinterstore(&dest_physical, &physical_keys, weights.as_deref(), agg) {
                Ok(count) => {
                    respond_integer(writer, count as i64).await?;
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
        Command::Zunionstore {
            destination,
            keys,
            weights,
            aggregate,
        } => {
            let dest_physical = prefix_key(current_db, &destination);
            let physical_keys: Vec<String> =
                keys.iter().map(|k| prefix_key(current_db, k)).collect();
            let agg = aggregate
                .map(|a| match a {
                    crate::command::ZAggregate::Sum => crate::storage::ZsetAggregate::Sum,
                    crate::command::ZAggregate::Min => crate::storage::ZsetAggregate::Min,
                    crate::command::ZAggregate::Max => crate::storage::ZsetAggregate::Max,
                })
                .unwrap_or(crate::storage::ZsetAggregate::Sum);
            match storage.zunionstore(&dest_physical, &physical_keys, weights.as_deref(), agg) {
                Ok(count) => {
                    respond_integer(writer, count as i64).await?;
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
        Command::Zdiffstore { destination, keys } => {
            let dest_physical = prefix_key(current_db, &destination);
            let physical_keys: Vec<String> =
                keys.iter().map(|k| prefix_key(current_db, k)).collect();
            match storage.zdiffstore(&dest_physical, &physical_keys) {
                Ok(count) => {
                    respond_integer(writer, count as i64).await?;
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
        Command::Zlexcount { key, min, max } => {
            let physical = prefix_key(current_db, &key);
            let (min_val, min_incl, min_unbounded) = zlex_bound_to_params(&min);
            let (max_val, max_incl, max_unbounded) = zlex_bound_to_params(&max);
            match storage.zlexcount(
                &physical,
                &min_val,
                min_incl,
                &max_val,
                max_incl,
                min_unbounded,
                max_unbounded,
            ) {
                Ok(count) => {
                    respond_integer(writer, count as i64).await?;
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
        Command::Zscan {
            key,
            cursor,
            pattern,
            count,
            novalues,
        } => {
            let physical = prefix_key(current_db, &key);
            match storage.zscan_entries(&physical) {
                Ok(entries) => {
                    let total = entries.len() as u64;
                    let start = if cursor > total { total } else { cursor } as usize;
                    let batch_size = count.unwrap_or(10) as usize;
                    let end = std::cmp::min(start + batch_size, entries.len());

                    let pat = pattern.unwrap_or_else(|| "*".to_string());
                    let mut flat: Vec<(String, f64)> = Vec::new();
                    for (member, score) in &entries[start..end] {
                        if pattern_match(&pat, member) {
                            flat.push((member.clone(), *score));
                        }
                    }

                    let next_cursor = if end >= entries.len() { 0 } else { end as u64 };
                    let cursor_str = next_cursor.to_string();

                    // NOVALUES: 只返回 member，不返回 score
                    let array_len = if novalues { flat.len() } else { flat.len() * 2 };
                    let mut response = format!(
                        "*2\r\n${}\r\n{}\r\n*{}\r\n",
                        cursor_str.len(),
                        cursor_str,
                        array_len
                    );
                    for (member, score) in flat {
                        response.push_str(&format!("${}\r\n{}\r\n", member.len(), member));
                        if !novalues {
                            let score_s = format_score(score);
                            response.push_str(&format!("${}\r\n{}\r\n", score_s.len(), score_s));
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
        _ => {}
    }

    Ok(())
}

async fn handle_persistence_command(
    cmd: Command,
    storage: &Storage,
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    persistence: Arc<PersistenceState>,
) -> io::Result<()> {
    if !persistence.enabled {
        respond_error(writer, "ERR persistence disabled").await?;
        return Ok(());
    }
    match cmd {
        Command::Save => {
            let path = persistence.rdb_path.clone();
            let save_res = perform_save(storage.clone(), path.clone(), persistence.clone()).await;
            match save_res {
                Ok(()) => respond_simple_string(writer, "OK").await?,
                Err(e) => {
                    error!("[rdb] SAVE failed: {}", e);
                    respond_error(writer, "ERR save failed").await?;
                }
            }
        }
        Command::Bgsave => {
            if persistence
                .bgsave_running
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
            {
                respond_error(writer, "ERR Background save already in progress").await?;
            } else {
                let path = persistence.rdb_path.clone();
                let storage_clone = storage.clone();
                let persistence_clone = persistence.clone();
                tokio::spawn(async move {
                    let res = perform_save(storage_clone, path, persistence_clone.clone()).await;
                    persistence_clone
                        .bgsave_running
                        .store(false, Ordering::SeqCst);
                    if let Err(e) = res {
                        error!("[rdb] BGSAVE failed: {}", e);
                    }
                });
                respond_simple_string(writer, "Background saving started").await?;
            }
        }
        Command::Lastsave => {
            let ts = persistence.last_save.load(Ordering::Relaxed);
            respond_integer(writer, ts).await?;
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
        Command::Expireat { key, timestamp } => {
            let physical = prefix_key(current_db, &key);
            let res = storage.expireat(&physical, timestamp);
            let v = if res { 1 } else { 0 };
            respond_integer(writer, v).await?;
        }
        Command::Pexpireat { key, timestamp_ms } => {
            let physical = prefix_key(current_db, &key);
            let res = storage.pexpireat(&physical, timestamp_ms);
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
        Command::Expiretime { key } => {
            let physical = prefix_key(current_db, &key);
            let ts = storage.expiretime(&physical);
            respond_integer(writer, ts).await?;
        }
        Command::Pexpiretime { key } => {
            let physical = prefix_key(current_db, &key);
            let ts = storage.pexpiretime(&physical);
            respond_integer(writer, ts).await?;
        }
        Command::Persist { key } => {
            let physical = prefix_key(current_db, &key);
            let changed = storage.persist(&physical);
            let v = if changed { 1 } else { 0 };
            respond_integer(writer, v).await?;
        }
        Command::Type { .. } | Command::Keys { .. } | Command::Scan { .. } | Command::Dbsize => {
            handle_meta_command(cmd, storage, writer, current_db).await?;
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
    let maxmemory_policy = storage.maxmemory_policy().as_str();
    let used_memory = storage.approximate_used_memory();
    let used_memory_human = format_bytes(used_memory);

    let mut info = String::new();
    info.push_str("# Server\r\n");
    info.push_str(&format!("redust_version:0.1.0\r\n"));
    info.push_str(&format!("tcp_port:{}\r\n", metrics.tcp_port));
    info.push_str(&format!("uptime_in_seconds:{}\r\n", uptime));
    info.push_str(&format!("maxmemory:{}\r\n", maxmemory));
    info.push_str(&format!("maxmemory_human:{}\r\n", maxmemory_human));
    info.push_str(&format!("maxmemory_policy:{}\r\n", maxmemory_policy));
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

/// 在事务中执行单个命令，返回结果写入 writer
async fn execute_command_in_transaction(
    cmd: Command,
    storage: &Storage,
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    current_db: u8,
    script_cache: &ScriptCache,
) -> io::Result<()> {
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
        | Command::Unlink { .. }
        | Command::Copy { .. }
        | Command::Touch { .. }
        | Command::ObjectEncoding { .. }
        | Command::Exists { .. }
        | Command::Mget { .. }
        | Command::Mset { .. }
        | Command::Msetnx { .. }
        | Command::Setnx { .. }
        | Command::Setex { .. }
        | Command::Psetex { .. }
        | Command::Quit => {
            handle_string_command(cmd, storage, writer, current_db).await?;
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
        | Command::Ltrim { .. }
        | Command::Lset { .. }
        | Command::Linsert { .. }
        | Command::Rpoplpush { .. }
        | Command::Lpos { .. } => {
            handle_list_command(cmd, storage, writer, current_db).await?;
        }

        // 阻塞命令在事务中不支持
        Command::Blpop { .. } | Command::Brpop { .. } => {
            respond_error(writer, "ERR BLPOP/BRPOP inside MULTI is not allowed").await?;
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
        | Command::Sdiffstore { .. }
        | Command::Smove { .. }
        | Command::Sscan { .. } => {
            handle_set_command(cmd, storage, writer, current_db).await?;
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
        | Command::Hlen { .. }
        | Command::Hsetnx { .. }
        | Command::Hstrlen { .. }
        | Command::Hmset { .. }
        | Command::Hscan { .. } => {
            handle_hash_command(cmd, storage, writer, current_db).await?;
        }

        // zset 命令
        Command::Zadd { .. }
        | Command::Zcard { .. }
        | Command::Zrange { .. }
        | Command::Zscore { .. }
        | Command::Zrem { .. }
        | Command::Zincrby { .. }
        | Command::Zcount { .. }
        | Command::Zrank { .. }
        | Command::Zrevrank { .. }
        | Command::Zpopmin { .. }
        | Command::Zpopmax { .. }
        | Command::Zinter { .. }
        | Command::Zunion { .. }
        | Command::Zdiff { .. }
        | Command::Zinterstore { .. }
        | Command::Zunionstore { .. }
        | Command::Zdiffstore { .. }
        | Command::Zlexcount { .. } => {
            handle_zset_command(cmd, storage, writer, current_db).await?;
        }

        // HyperLogLog 命令
        Command::Pfadd { .. } | Command::Pfcount { .. } | Command::Pfmerge { .. } => {
            handle_string_command(cmd, storage, writer, current_db).await?;
        }

        // key meta 命令
        Command::Type { key } => {
            handle_meta_command(Command::Type { key }, storage, writer, current_db).await?;
        }
        Command::Keys { pattern } => {
            handle_meta_command(Command::Keys { pattern }, storage, writer, current_db).await?;
        }
        Command::Dbsize => {
            handle_meta_command(Command::Dbsize, storage, writer, current_db).await?;
        }
        Command::Scan {
            cursor,
            pattern,
            count,
            type_filter,
        } => {
            handle_meta_command(
                Command::Scan {
                    cursor,
                    pattern,
                    count,
                    type_filter,
                },
                storage,
                writer,
                current_db,
            )
            .await?;
        }
        Command::Expire { .. }
        | Command::Pexpire { .. }
        | Command::Expireat { .. }
        | Command::Pexpireat { .. }
        | Command::Ttl { .. }
        | Command::Pttl { .. }
        | Command::Expiretime { .. }
        | Command::Pexpiretime { .. }
        | Command::Persist { .. }
        | Command::Rename { .. }
        | Command::Renamenx { .. }
        | Command::Zscan { .. } => {
            respond_error(writer, "ERR command not supported in transaction").await?;
        }

        // 不支持在事务中的命令
        Command::Multi
        | Command::Exec
        | Command::Discard
        | Command::Watch { .. }
        | Command::Unwatch => {
            respond_error(writer, "ERR command not allowed in transaction").await?;
        }

        // Lua 脚本命令
        Command::Eval { script, keys, args } => {
            // 缓存脚本
            script_cache.load(&script);

            let ctx = ScriptContext {
                storage: Arc::new(storage.clone()),
                current_db: current_db as u32,
                keys,
                args,
            };
            match execute_script(&script, ctx) {
                Ok(result) => {
                    let resp = result.to_resp_bytes();
                    writer.write_all(&resp).await?;
                }
                Err(e) => {
                    respond_error(writer, &e).await?;
                }
            }
        }
        Command::Evalsha { sha1, keys, args } => match script_cache.get(&sha1) {
            Some(script) => {
                let ctx = ScriptContext {
                    storage: Arc::new(storage.clone()),
                    current_db: current_db as u32,
                    keys,
                    args,
                };
                match execute_script(&script, ctx) {
                    Ok(result) => {
                        let resp = result.to_resp_bytes();
                        writer.write_all(&resp).await?;
                    }
                    Err(e) => {
                        respond_error(writer, &e).await?;
                    }
                }
            }
            None => {
                respond_error(writer, "NOSCRIPT No matching script. Please use EVAL.").await?;
            }
        },
        Command::ScriptLoad { script } => {
            let sha1 = script_cache.load(&script);
            respond_bulk_string(writer, &sha1).await?;
        }
        Command::ScriptExists { sha1s } => {
            let results = script_cache.exists(&sha1s);
            let mut resp = format!("*{}\r\n", results.len());
            for exists in results {
                resp.push_str(&format!(":{}\r\n", if exists { 1 } else { 0 }));
            }
            writer.write_all(resp.as_bytes()).await?;
        }
        Command::ScriptFlush => {
            script_cache.flush();
            respond_simple_string(writer, "OK").await?;
        }

        // 其他命令返回错误
        _ => {
            respond_error(writer, "ERR command not supported in transaction").await?;
        }
    }
    Ok(())
}

async fn handle_connection(
    stream: TcpStream,
    storage: Storage,
    metrics: Arc<Metrics>,
    pubsub: PubSubHub,
    overflow_strategy: PubSubOverflowStrategy,
    persistence: Arc<PersistenceState>,
    script_cache: Arc<ScriptCache>,
    slowlog: Arc<SlowLog>,
) -> io::Result<()> {
    let peer_addr = stream.peer_addr().ok();
    let client_addr = peer_addr.map(|a| a.to_string()).unwrap_or_default();
    info!("[conn] new connection from {:?}", peer_addr);

    metrics.connected_clients.fetch_add(1, Ordering::Relaxed);

    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut current_db: u8 = 0;

    // 客户端标识
    let client_id = CLIENT_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut client_name = String::new();

    let auth_password = env::var("REDUST_AUTH_PASSWORD")
        .ok()
        .filter(|s| !s.is_empty());
    let mut authenticated = auth_password.is_none();
    let (msg_tx, mut msg_rx) = mpsc::unbounded_channel::<PubMessage>();
    let mut channel_subscriptions: HashMap<String, tokio::task::JoinHandle<()>> = HashMap::new();
    let mut pattern_subscriptions: HashMap<String, tokio::task::JoinHandle<()>> = HashMap::new();
    let mut shard_subscriptions: HashMap<String, tokio::task::JoinHandle<()>> = HashMap::new();
    let mut subscribed_mode = false;

    // 事务状态
    let mut in_transaction = false;
    let mut queued_commands: Vec<Command> = Vec::new();
    // WATCH 的 key -> 版本号映射（物理 key）
    let mut watched_keys: HashMap<String, u64> = HashMap::new();
    // 事务是否因解析错误而中止
    let mut transaction_aborted = false;

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

        // 检查 CLIENT PAUSE：如果处于暂停状态，等待直到超时或被 UNPAUSE
        // 注意：CLIENT PAUSE/UNPAUSE 命令本身不受暂停影响
        let skip_pause = matches!(cmd, Command::ClientPause { .. } | Command::ClientUnpause);
        if !skip_pause {
            loop {
                let pause_until = CLIENT_PAUSE_UNTIL_MS.load(Ordering::Relaxed);
                if pause_until == 0 {
                    break;
                }
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                if now_ms >= pause_until {
                    // 超时自动解除
                    CLIENT_PAUSE_UNTIL_MS.store(0, Ordering::Relaxed);
                    break;
                }
                // 等待一小段时间后重试
                sleep(Duration::from_millis(10)).await;
            }
        }

        // 记录命令开始时间用于慢日志
        let cmd_start = Instant::now();
        let cmd_strings = command_to_strings(&cmd);

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

        // 事务处理
        match &cmd {
            Command::Multi => {
                if in_transaction {
                    respond_error(&mut write_half, "ERR MULTI calls can not be nested").await?;
                } else {
                    in_transaction = true;
                    transaction_aborted = false;
                    queued_commands.clear();
                    respond_simple_string(&mut write_half, "OK").await?;
                }
                continue;
            }
            Command::Exec => {
                if !in_transaction {
                    respond_error(&mut write_half, "ERR EXEC without MULTI").await?;
                    continue;
                }

                // 如果事务因解析错误而中止
                if transaction_aborted {
                    in_transaction = false;
                    transaction_aborted = false;
                    queued_commands.clear();
                    watched_keys.clear();
                    respond_error(
                        &mut write_half,
                        "EXECABORT Transaction discarded because of previous errors.",
                    )
                    .await?;
                    continue;
                }

                // 检查 WATCH 的 key 是否被修改
                let mut watch_failed = false;
                for (key, version) in &watched_keys {
                    if storage.get_key_version(key) != *version {
                        watch_failed = true;
                        break;
                    }
                }
                in_transaction = false;
                watched_keys.clear();

                if watch_failed {
                    queued_commands.clear();
                    // WATCH 失败返回 null bulk
                    write_half.write_all(b"*-1\r\n").await?;
                    continue;
                }

                // 执行队列中的命令
                let commands = std::mem::take(&mut queued_commands);
                let count = commands.len();
                write_half
                    .write_all(format!("*{}\r\n", count).as_bytes())
                    .await?;
                for queued_cmd in commands {
                    execute_command_in_transaction(
                        queued_cmd,
                        &storage,
                        &mut write_half,
                        current_db,
                        &script_cache,
                    )
                    .await?;
                }
                continue;
            }
            Command::Discard => {
                if !in_transaction {
                    respond_error(&mut write_half, "ERR DISCARD without MULTI").await?;
                } else {
                    in_transaction = false;
                    transaction_aborted = false;
                    queued_commands.clear();
                    watched_keys.clear();
                    respond_simple_string(&mut write_half, "OK").await?;
                }
                continue;
            }
            Command::Watch { keys } => {
                if in_transaction {
                    respond_error(&mut write_half, "ERR WATCH inside MULTI is not allowed").await?;
                } else {
                    for key in keys {
                        let physical = prefix_key(current_db, key);
                        let version = storage.get_key_version(&physical);
                        watched_keys.insert(physical, version);
                    }
                    respond_simple_string(&mut write_half, "OK").await?;
                }
                continue;
            }
            Command::Unwatch => {
                watched_keys.clear();
                respond_simple_string(&mut write_half, "OK").await?;
                continue;
            }
            _ => {}
        }

        // 如果在事务中，将命令加入队列
        if in_transaction {
            // 检查是否是解析错误
            if let Command::Error(ref msg) = cmd {
                // 标记事务为中止状态，返回原始错误
                transaction_aborted = true;
                respond_error(&mut write_half, msg).await?;
                continue;
            }
            queued_commands.push(cmd);
            respond_simple_string(&mut write_half, "QUEUED").await?;
            continue;
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
            | Command::Unlink { .. }
            | Command::Copy { .. }
            | Command::Touch { .. }
            | Command::ObjectEncoding { .. }
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
            | Command::Ltrim { .. }
            | Command::Lset { .. }
            | Command::Linsert { .. }
            | Command::Rpoplpush { .. }
            | Command::Blpop { .. }
            | Command::Brpop { .. }
            | Command::Lpos { .. } => {
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
            | Command::Sdiffstore { .. }
            | Command::Smove { .. }
            | Command::Sscan { .. } => {
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
            | Command::Hlen { .. }
            | Command::Hsetnx { .. }
            | Command::Hstrlen { .. }
            | Command::Hmset { .. }
            | Command::Hscan { .. } => {
                handle_hash_command(cmd, &storage, &mut write_half, current_db).await?;
            }

            // zset 命令
            Command::Zadd { .. }
            | Command::Zcard { .. }
            | Command::Zrange { .. }
            | Command::Zscore { .. }
            | Command::Zrem { .. }
            | Command::Zincrby { .. }
            | Command::Zcount { .. }
            | Command::Zrank { .. }
            | Command::Zrevrank { .. }
            | Command::Zpopmin { .. }
            | Command::Zpopmax { .. }
            | Command::Zinter { .. }
            | Command::Zunion { .. }
            | Command::Zdiff { .. }
            | Command::Zinterstore { .. }
            | Command::Zunionstore { .. }
            | Command::Zdiffstore { .. }
            | Command::Zlexcount { .. }
            | Command::Zscan { .. } => {
                handle_zset_command(cmd, &storage, &mut write_half, current_db).await?;
            }

            // HyperLogLog 命令
            Command::Pfadd { .. } | Command::Pfcount { .. } | Command::Pfmerge { .. } => {
                handle_string_command(cmd, &storage, &mut write_half, current_db).await?;
            }

            // 持久化控制
            Command::Save | Command::Bgsave | Command::Lastsave => {
                handle_persistence_command(cmd, &storage, &mut write_half, persistence.clone())
                    .await?;
            }

            // key 元信息、过期相关命令
            Command::Expire { .. }
            | Command::Pexpire { .. }
            | Command::Expireat { .. }
            | Command::Pexpireat { .. }
            | Command::Ttl { .. }
            | Command::Pttl { .. }
            | Command::Expiretime { .. }
            | Command::Pexpiretime { .. }
            | Command::Persist { .. }
            | Command::Type { .. }
            | Command::Keys { .. }
            | Command::Scan { .. }
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

            // 事务命令已在前面处理，这里不应该到达
            Command::Multi
            | Command::Exec
            | Command::Discard
            | Command::Watch { .. }
            | Command::Unwatch => {
                unreachable!("transaction commands should be handled earlier");
            }

            // Lua 脚本命令
            Command::Eval { script, keys, args } => {
                // 缓存脚本
                script_cache.load(&script);

                let ctx = ScriptContext {
                    storage: Arc::new(storage.clone()),
                    current_db: current_db as u32,
                    keys,
                    args,
                };
                match execute_script(&script, ctx) {
                    Ok(result) => {
                        let resp = result.to_resp_bytes();
                        write_half.write_all(&resp).await?;
                    }
                    Err(e) => {
                        respond_error(&mut write_half, &e).await?;
                    }
                }
            }
            Command::Evalsha { sha1, keys, args } => match script_cache.get(&sha1) {
                Some(script) => {
                    let ctx = ScriptContext {
                        storage: Arc::new(storage.clone()),
                        current_db: current_db as u32,
                        keys,
                        args,
                    };
                    match execute_script(&script, ctx) {
                        Ok(result) => {
                            let resp = result.to_resp_bytes();
                            write_half.write_all(&resp).await?;
                        }
                        Err(e) => {
                            respond_error(&mut write_half, &e).await?;
                        }
                    }
                }
                None => {
                    respond_error(
                        &mut write_half,
                        "NOSCRIPT No matching script. Please use EVAL.",
                    )
                    .await?;
                }
            },
            Command::ScriptLoad { script } => {
                let sha1 = script_cache.load(&script);
                respond_bulk_string(&mut write_half, &sha1).await?;
            }
            Command::ScriptExists { sha1s } => {
                let results = script_cache.exists(&sha1s);
                let mut resp = format!("*{}\r\n", results.len());
                for exists in results {
                    resp.push_str(&format!(":{}\r\n", if exists { 1 } else { 0 }));
                }
                write_half.write_all(resp.as_bytes()).await?;
            }
            Command::ScriptFlush => {
                script_cache.flush();
                respond_simple_string(&mut write_half, "OK").await?;
            }

            // 运维命令
            Command::ConfigGet { pattern } => {
                // 返回匹配的配置参数
                let configs = get_config_values(&pattern, &storage, &slowlog);
                let mut resp = format!("*{}\r\n", configs.len() * 2);
                for (key, value) in configs {
                    resp.push_str(&format!("${}\r\n{}\r\n", key.len(), key));
                    resp.push_str(&format!("${}\r\n{}\r\n", value.len(), value));
                }
                write_half.write_all(resp.as_bytes()).await?;
            }
            Command::ConfigSet { parameter, value } => {
                // 尝试设置配置参数
                match set_config_value(&parameter, &value, &storage, &slowlog) {
                    Ok(()) => respond_simple_string(&mut write_half, "OK").await?,
                    Err(e) => respond_error(&mut write_half, &e).await?,
                }
            }
            Command::ClientList => {
                // 返回当前连接信息（简化版）
                let info = format!(
                    "id={} addr={} name={} db={}\n",
                    client_id,
                    peer_addr
                        .map(|a| a.to_string())
                        .unwrap_or_else(|| "unknown".to_string()),
                    client_name,
                    current_db
                );
                let resp = format!("${}\r\n{}\r\n", info.len(), info);
                write_half.write_all(resp.as_bytes()).await?;
            }
            Command::ClientId => {
                respond_integer(&mut write_half, client_id as i64).await?;
            }
            Command::ClientSetname { name } => {
                client_name = name;
                respond_simple_string(&mut write_half, "OK").await?;
            }
            Command::ClientGetname => {
                if client_name.is_empty() {
                    write_half.write_all(b"$-1\r\n").await?;
                } else {
                    let resp = format!("${}\r\n{}\r\n", client_name.len(), client_name);
                    write_half.write_all(resp.as_bytes()).await?;
                }
            }
            Command::ClientPause { timeout_ms } => {
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                let until = now_ms.saturating_add(timeout_ms);
                CLIENT_PAUSE_UNTIL_MS.store(until, Ordering::Relaxed);
                respond_simple_string(&mut write_half, "OK").await?;
            }
            Command::ClientUnpause => {
                CLIENT_PAUSE_UNTIL_MS.store(0, Ordering::Relaxed);
                respond_simple_string(&mut write_half, "OK").await?;
            }
            Command::SlowlogGet { count } => {
                let entries = slowlog.get(count);
                // Redis SLOWLOG GET 返回格式:
                // *N (N 条记录)
                //   *6 (每条记录 6 个字段)
                //     :id
                //     :timestamp
                //     :duration_us
                //     *M (命令参数数组)
                //     $client_addr
                //     $client_name
                let mut resp = format!("*{}\r\n", entries.len());
                for entry in entries {
                    resp.push_str("*6\r\n");
                    resp.push_str(&format!(":{}\r\n", entry.id));
                    resp.push_str(&format!(":{}\r\n", entry.timestamp));
                    resp.push_str(&format!(":{}\r\n", entry.duration_us));
                    resp.push_str(&format!("*{}\r\n", entry.command.len()));
                    for arg in &entry.command {
                        resp.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
                    }
                    resp.push_str(&format!(
                        "${}\r\n{}\r\n",
                        entry.client_addr.len(),
                        entry.client_addr
                    ));
                    resp.push_str(&format!(
                        "${}\r\n{}\r\n",
                        entry.client_name.len(),
                        entry.client_name
                    ));
                }
                write_half.write_all(resp.as_bytes()).await?;
            }
            Command::SlowlogReset => {
                slowlog.reset();
                respond_simple_string(&mut write_half, "OK").await?;
            }
            Command::SlowlogLen => {
                respond_integer(&mut write_half, slowlog.len() as i64).await?;
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

        // 记录慢日志
        let duration_us = cmd_start.elapsed().as_micros() as u64;
        slowlog.log_if_slow(duration_us, cmd_strings, &client_addr, &client_name);
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
    let maxmemory_policy = env::var("REDUST_MAXMEMORY_POLICY")
        .ok()
        .and_then(|s| parse_maxmemory_policy(&s))
        .unwrap_or(MaxmemoryPolicy::AllKeysLru);
    let maxmemory_samples = env::var("REDUST_MAXMEMORY_SAMPLES")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|v| *v > 0);

    let storage = Storage::new(maxmemory_bytes, maxmemory_samples);
    storage.set_maxmemory_policy(maxmemory_policy);

    let rdb_path = env::var("REDUST_RDB_PATH").unwrap_or_else(|_| "redust.rdb".to_string());
    let persistence_disabled = env::var("REDUST_DISABLE_PERSISTENCE")
        .map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(false);
    let aof_enabled = env::var("REDUST_AOF_ENABLED")
        .map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(false);
    let aof_path = env::var("REDUST_AOF_PATH").unwrap_or_else(|_| "redust.aof".to_string());

    let persistence = Arc::new(PersistenceState {
        rdb_path: rdb_path.clone(),
        aof_path: if aof_enabled && !persistence_disabled {
            Some(aof_path.clone())
        } else {
            None
        },
        last_save: AtomicI64::new(-1),
        bgsave_running: AtomicBool::new(false),
        enabled: !persistence_disabled,
    });

    if !persistence_disabled {
        let mut loaded_ok = false;
        if let Some(path) = persistence.aof_path.clone() {
            let exists = std::path::Path::new(&path).exists();
            if exists {
                match storage.load_rdb(&path) {
                    Ok(()) => {
                        loaded_ok = true;
                        if let Some(ts) = file_mtime_seconds(&path) {
                            persistence.last_save.store(ts, Ordering::Relaxed);
                        }
                        info!("[aof] loaded snapshot from {}", path);
                    }
                    Err(e) => {
                        error!("[aof] failed to load AOF snapshot {}: {}", path, e);
                        if let Some(new_path) = quarantine_corrupt_file(&path) {
                            error!("[aof] quarantined corrupt file to {}", new_path);
                        } else {
                            error!("[aof] failed to quarantine corrupt file {}", path);
                        }
                    }
                }
            }
        }

        if !loaded_ok {
            if let Err(e) = storage.load_rdb(&rdb_path) {
                error!("[rdb] failed to load RDB from {}: {}", rdb_path, e);
                if let Some(new_path) = quarantine_corrupt_file(&rdb_path) {
                    error!("[rdb] quarantined corrupt file to {}", new_path);
                } else {
                    error!("[rdb] failed to quarantine corrupt file {}", rdb_path);
                }
            } else {
                if let Some(ts) = file_mtime_seconds(&rdb_path) {
                    persistence.last_save.store(ts, Ordering::Relaxed);
                }
                info!("[rdb] loaded RDB from {} (if existing and valid)", rdb_path);
            }
        }
    }

    storage.spawn_expiration_task();

    if !persistence_disabled && persistence.aof_path.is_none() {
        if let Ok(interval_str) = env::var("REDUST_RDB_AUTO_SAVE_SECS") {
            if let Ok(secs) = interval_str.parse::<u64>() {
                if secs > 0 {
                    let storage_clone = storage.clone();
                    let path_clone = rdb_path.clone();
                    let persistence_clone = persistence.clone();
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
                            if let Err(e) = perform_save(
                                storage_for_blocking.clone(),
                                path_for_blocking.clone(),
                                persistence_clone.clone(),
                            )
                            .await
                            {
                                error!("[rdb] auto-save failed: {}", e);
                            }
                        }
                    });
                }
            }
        }
    }

    if !persistence_disabled {
        if let Some(path) = persistence.aof_path.clone() {
            let storage_clone = storage.clone();
            let persistence_clone = persistence.clone();
            let path_for_log = path.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(1));
                // align to run after the first full interval
                interval.tick().await;
                loop {
                    interval.tick().await;
                    if let Err(e) = perform_save(
                        storage_clone.clone(),
                        path.clone(),
                        persistence_clone.clone(),
                    )
                    .await
                    {
                        error!("[aof] everysec save failed: {}", e);
                    }
                }
            });
            info!("[aof] everysec snapshot enabled to {}", path_for_log);
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
    let slowlog = Arc::new(SlowLog::new());
    let pubsub = PubSubHub::new();
    let script_cache = Arc::new(ScriptCache::new());

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
    let aof_path_for_shutdown = persistence.aof_path.clone();
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
                let persistence_clone = persistence.clone();
                let script_cache_clone = script_cache.clone();
                let slowlog_clone = slowlog.clone();
                tokio::spawn(async move {
                    if let Err(err) = handle_connection(stream, storage, metrics, pubsub, overflow_strategy, persistence_clone.clone(), script_cache_clone, slowlog_clone).await {
                        error!("Connection error: {}", err);
                    }
                });
            }
            _ = &mut shutdown => {
                break;
            }
        }
    }

    // 尝试在关闭前做一次快照（优先 aof 路径）
    if persistence.enabled {
        if let Some(path) = aof_path_for_shutdown {
            if let Err(e) = perform_save(storage.clone(), path.clone(), persistence.clone()).await {
                error!("[aof] final save failed: {}", e);
            }
        } else {
            if let Err(e) =
                perform_save(storage.clone(), rdb_path.clone(), persistence.clone()).await
            {
                error!("[rdb] final save failed: {}", e);
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

// ============================================================================
// CONFIG GET/SET 辅助函数
// ============================================================================

/// 获取匹配模式的配置值
fn get_config_values(pattern: &str, storage: &Storage, slowlog: &SlowLog) -> Vec<(String, String)> {
    let mut results = Vec::new();

    // 支持的配置参数
    let maxmemory = storage.maxmemory_bytes().unwrap_or(0);
    let policy = storage.maxmemory_policy().as_str().to_string();
    let maxmemory_samples = storage.maxmemory_samples().to_string();
    let configs = [
        ("maxmemory", maxmemory.to_string()),
        ("maxmemory-policy", policy),
        ("maxmemory-samples", maxmemory_samples),
        (
            "timeout",
            CLIENT_TIMEOUT_SECS.load(Ordering::Relaxed).to_string(),
        ),
        (
            "tcp-keepalive",
            TCP_KEEPALIVE_SECS.load(Ordering::Relaxed).to_string(),
        ),
        ("databases", "16".to_string()),
        ("save", "".to_string()),
        (
            "appendonly",
            env::var("REDUST_AOF_ENABLED").unwrap_or_else(|_| "no".to_string()),
        ),
        ("appendfsync", "everysec".to_string()),
        ("dir", ".".to_string()),
        (
            "dbfilename",
            env::var("REDUST_RDB_PATH").unwrap_or_else(|_| "redust.rdb".to_string()),
        ),
        (
            "appendfilename",
            env::var("REDUST_AOF_PATH").unwrap_or_else(|_| "redust.aof".to_string()),
        ),
        (
            "requirepass",
            if env::var("REDUST_AUTH_PASSWORD").is_ok() {
                "yes".to_string()
            } else {
                "".to_string()
            },
        ),
        ("loglevel", "notice".to_string()),
        (
            "slowlog-log-slower-than",
            slowlog.threshold_us().to_string(),
        ),
        ("slowlog-max-len", slowlog.max_len().to_string()),
    ];

    for (key, value) in configs {
        if pattern_match_config(pattern, key) {
            results.push((key.to_string(), value));
        }
    }

    results
}

/// 设置配置值（大多数配置在运行时不可修改）
fn set_config_value(
    parameter: &str,
    value: &str,
    storage: &Storage,
    slowlog: &SlowLog,
) -> Result<(), String> {
    match parameter.to_lowercase().as_str() {
        "maxmemory" => {
            let bytes = parse_maxmemory_bytes(value)
                .ok_or_else(|| "ERR invalid maxmemory value".to_string())?;
            storage.set_maxmemory_bytes(Some(bytes));
            Ok(())
        }
        "maxmemory-policy" => {
            let policy = parse_maxmemory_policy(value)
                .ok_or_else(|| "ERR invalid maxmemory-policy value".to_string())?;
            storage.set_maxmemory_policy(policy);
            Ok(())
        }
        "maxmemory-samples" => {
            let samples: usize = value
                .parse()
                .map_err(|_| "ERR invalid maxmemory-samples value".to_string())?;
            if samples == 0 {
                return Err("ERR maxmemory-samples must be positive".to_string());
            }
            storage.set_maxmemory_samples(samples);
            Ok(())
        }
        "timeout" => {
            let secs: u64 = value
                .parse()
                .map_err(|_| "ERR invalid timeout value".to_string())?;
            CLIENT_TIMEOUT_SECS.store(secs, Ordering::Relaxed);
            Ok(())
        }
        "tcp-keepalive" => {
            let secs: u64 = value
                .parse()
                .map_err(|_| "ERR invalid tcp-keepalive value".to_string())?;
            TCP_KEEPALIVE_SECS.store(secs, Ordering::Relaxed);
            Ok(())
        }
        "slowlog-log-slower-than" => {
            let us: u64 = value
                .parse()
                .map_err(|_| "ERR invalid slowlog-log-slower-than value".to_string())?;
            slowlog.set_threshold_us(us);
            Ok(())
        }
        "slowlog-max-len" => {
            let len: usize = value
                .parse()
                .map_err(|_| "ERR invalid slowlog-max-len value".to_string())?;
            if len == 0 {
                return Err("ERR slowlog-max-len must be positive".to_string());
            }
            slowlog.set_max_len(len);
            Ok(())
        }
        _ => Err(format!("ERR Unsupported CONFIG parameter: {}", parameter)),
    }
}

/// 简单的配置模式匹配（支持 * 通配符）
fn pattern_match_config(pattern: &str, key: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if pattern.ends_with('*') {
        let prefix = &pattern[..pattern.len() - 1];
        return key.starts_with(prefix);
    }
    if pattern.starts_with('*') {
        let suffix = &pattern[1..];
        return key.ends_with(suffix);
    }
    pattern.eq_ignore_ascii_case(key)
}
