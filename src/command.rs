use std::fmt;
use tokio::io::{self, BufReader};

use crate::resp::read_resp_array;

pub type Binary = Vec<u8>;

/// Custom error type for command parsing.
#[derive(Debug)]
pub enum CommandError {
    Io(io::Error),
    /// Represents a Redis-like command error, e.g., "ERR wrong number of arguments".
    RedisError(String),
}

impl From<io::Error> for CommandError {
    fn from(err: io::Error) -> Self {
        CommandError::Io(err)
    }
}

impl fmt::Display for CommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommandError::Io(err) => write!(f, "IO Error: {}", err),
            CommandError::RedisError(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for CommandError {}

/// Represents a Redis command.
#[derive(Debug)]
pub enum Command {
    Ping,
    PingWithPayload(Binary),
    Echo(Binary),
    Quit,
    Set {
        key: String,
        value: Binary,
        // 相对过期时间（EX/PX），单位：毫秒
        expire_millis: Option<i64>,
        // 绝对过期时间（EXAT/PXAT），Unix 毫秒时间戳
        expire_at_millis: Option<i64>,
        nx: bool,
        xx: bool,
        keep_ttl: bool,
        get: bool,
    },
    Get {
        key: String,
    },
    Getdel {
        key: String,
    },
    Getex {
        key: String,
        expire_millis: Option<i64>,
        persist: bool,
    },
    Getrange {
        key: String,
        start: isize,
        end: isize,
    },
    Setrange {
        key: String,
        offset: usize,
        value: Binary,
    },
    Append {
        key: String,
        value: Binary,
    },
    Strlen {
        key: String,
    },
    Getset {
        key: String,
        value: Binary,
    },
    Del {
        keys: Vec<String>,
    },
    Exists {
        keys: Vec<String>,
    },
    Incr {
        key: String,
    },
    Decr {
        key: String,
    },
    Incrby {
        key: String,
        delta: i64,
    },
    Decrby {
        key: String,
        delta: i64,
    },
    Incrbyfloat {
        key: String,
        delta: f64,
    },
    Scan {
        cursor: u64,
        pattern: Option<String>,
        count: Option<u64>,
    },
    Sscan {
        key: String,
        cursor: u64,
        pattern: Option<String>,
        count: Option<u64>,
    },
    Hscan {
        key: String,
        cursor: u64,
        pattern: Option<String>,
        count: Option<u64>,
    },
    Zscan {
        key: String,
        cursor: u64,
        pattern: Option<String>,
        count: Option<u64>,
    },
    Type {
        key: String,
    },
    Keys {
        pattern: String,
    },
    Dbsize,
    Lpush {
        key: String,
        values: Vec<String>,
    },
    Rpush {
        key: String,
        values: Vec<String>,
    },
    Lrange {
        key: String,
        start: isize,
        stop: isize,
    },
    Lpop {
        key: String,
    },
    Rpop {
        key: String,
    },
    Llen {
        key: String,
    },
    Lindex {
        key: String,
        index: isize,
    },
    Lrem {
        key: String,
        count: isize,
        value: String,
    },
    Ltrim {
        key: String,
        start: isize,
        stop: isize,
    },
    Sadd {
        key: String,
        members: Vec<String>,
    },
    Srem {
        key: String,
        members: Vec<String>,
    },
    Smembers {
        key: String,
    },
    Scard {
        key: String,
    },
    Sismember {
        key: String,
        member: String,
    },
    Spop {
        key: String,
        count: Option<i64>,
    },
    Srandmember {
        key: String,
        count: Option<i64>,
    },
    Sunion {
        keys: Vec<String>,
    },
    Sinter {
        keys: Vec<String>,
    },
    Sdiff {
        keys: Vec<String>,
    },
    Sunionstore {
        dest: String,
        keys: Vec<String>,
    },
    Sinterstore {
        dest: String,
        keys: Vec<String>,
    },
    Sdiffstore {
        dest: String,
        keys: Vec<String>,
    },
    Hset {
        key: String,
        field: String,
        value: String,
    },
    Hget {
        key: String,
        field: String,
    },
    Hdel {
        key: String,
        fields: Vec<String>,
    },
    Hexists {
        key: String,
        field: String,
    },
    Hgetall {
        key: String,
    },
    Hkeys {
        key: String,
    },
    Hvals {
        key: String,
    },
    Hmget {
        key: String,
        fields: Vec<String>,
    },
    Hincrby {
        key: String,
        field: String,
        delta: i64,
    },
    Hincrbyfloat {
        key: String,
        field: String,
        delta: f64,
    },
    Hlen {
        key: String,
    },
    Expire {
        key: String,
        seconds: i64,
    },
    Pexpire {
        key: String,
        millis: i64,
    },
    Ttl {
        key: String,
    },
    Pttl {
        key: String,
    },
    Persist {
        key: String,
    },
    Info,
    Auth {
        password: String,
    },
    Select {
        db: u8,
    },
    Mget {
        keys: Vec<String>,
    },
    Mset {
        pairs: Vec<(String, Binary)>,
    },
    Msetnx {
        pairs: Vec<(String, Binary)>,
    },
    Rename {
        key: String,
        newkey: String,
    },
    Renamenx {
        key: String,
        newkey: String,
    },
    Flushdb,
    Flushall,
    Setnx {
        key: String,
        value: Binary,
    },
    Setex {
        key: String,
        seconds: i64,
        value: Binary,
    },
    Psetex {
        key: String,
        millis: i64,
        value: Binary,
    },
    Subscribe {
        channels: Vec<String>,
    },
    Unsubscribe {
        channels: Vec<String>,
    },
    Ssubscribe {
        channels: Vec<String>,
    },
    Sunsubscribe {
        channels: Vec<String>,
    },
    Psubscribe {
        patterns: Vec<String>,
    },
    Punsubscribe {
        patterns: Vec<String>,
    },
    Publish {
        channel: String,
        message: Binary,
    },
    Spublish {
        channel: String,
        message: Binary,
    },
    PubsubChannels {
        pattern: Option<String>,
    },
    PubsubNumsub {
        channels: Vec<String>,
    },
    PubsubNumpat,
    PubsubShardchannels {
        pattern: Option<String>,
    },
    PubsubShardnumsub {
        channels: Vec<String>,
    },
    PubsubHelp,
    Save,
    Bgsave,
    Lastsave,
    // 事务命令
    Multi,
    Exec,
    Discard,
    Watch {
        keys: Vec<String>,
    },
    Unwatch,
    Zadd {
        key: String,
        entries: Vec<(f64, String)>,
    },
    Zcard {
        key: String,
    },
    Zrange {
        key: String,
        start: isize,
        stop: isize,
        withscores: bool,
        rev: bool,
    },
    Zscore {
        key: String,
        member: String,
    },
    Zrem {
        key: String,
        members: Vec<String>,
    },
    Zincrby {
        key: String,
        increment: f64,
        member: String,
    },
    Unknown(Vec<Binary>),
    /// Represents an error that should be sent back to the client.
    Error(String),
}

fn err_wrong_args(cmd: &str) -> Command {
    // Redis 错误消息中命令名通常是小写形式
    Command::Error(format!(
        "ERR wrong number of arguments for '{}' command",
        cmd.to_lowercase()
    ))
}

fn err_not_integer() -> Command {
    Command::Error("ERR value is not an integer or out of range".to_string())
}

fn err_not_float() -> Command {
    Command::Error("ERR value is not a valid float".to_string())
}

fn err_syntax() -> Command {
    Command::Error("ERR syntax error".to_string())
}

fn err_invalid_bulk() -> Command {
    Command::Error("ERR invalid bulk string encoding".to_string())
}

fn err_pubsub_args() -> Command {
    Command::Error(
        "ERR Unknown subcommand or wrong number of arguments for 'pubsub'. Try PUBSUB HELP."
            .to_string(),
    )
}

fn parse_bulk_string(bytes: Vec<u8>) -> Result<String, Command> {
    String::from_utf8(bytes).map_err(|_| err_invalid_bulk())
}

fn parse_i64_from_bulk(bytes: Vec<u8>) -> Result<i64, Command> {
    let s = parse_bulk_string(bytes)?;
    s.parse::<i64>().map_err(|_| err_not_integer())
}

fn parse_isize_from_bulk(bytes: Vec<u8>) -> Result<isize, Command> {
    let s = parse_bulk_string(bytes)?;
    s.parse::<isize>().map_err(|_| err_not_integer())
}

fn parse_f64_from_bulk(bytes: Vec<u8>) -> Result<f64, Command> {
    let s = parse_bulk_string(bytes)?;
    let v = s.parse::<f64>().map_err(|_| err_not_float())?;
    if !v.is_finite() {
        return Err(err_not_float());
    }
    Ok(v)
}

pub async fn read_command(
    reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
) -> Result<Option<Command>, CommandError> {
    let Some(parts) = read_resp_array(reader).await? else {
        return Ok(None);
    };

    let mut iter = parts.into_iter();
    let Some(command_bytes) = iter.next() else {
        return Ok(None);
    };

    let upper = match std::str::from_utf8(&command_bytes) {
        Ok(s) => s.to_ascii_uppercase(),
        Err(_) => return Ok(Some(err_invalid_bulk())),
    };
    let cmd = match upper.as_str() {
        "PING" => {
            if let Some(payload) = iter.next() {
                if iter.next().is_some() {
                    return Ok(Some(err_wrong_args("ping")));
                }
                Command::PingWithPayload(payload)
            } else {
                Command::Ping
            }
        }
        "ECHO" => {
            let Some(value) = iter.next() else {
                return Ok(Some(err_wrong_args("echo")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("echo")));
            }
            Command::Echo(value)
        }
        "QUIT" => {
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("quit")));
            }
            Command::Quit
        }
        "SET" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("set")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(value) = iter.next() else {
                return Ok(Some(err_wrong_args("set")));
            };

            // 解析可选参数：
            // EX seconds | PX milliseconds | EXAT unix-time | PXAT ms-unix-time | NX | XX | KEEPTTL | GET
            let mut expire_millis: Option<i64> = None;
            let mut expire_at_millis: Option<i64> = None;
            // 任意一种过期方式（EX/PX/EXAT/PXAT）只能出现一次
            let mut has_expire = false;
            let mut nx = false;
            let mut xx = false;
            let mut keep_ttl = false;
            let mut get = false;

            while let Some(opt) = iter.next() {
                let opt_upper = match std::str::from_utf8(&opt) {
                    Ok(s) => s.to_ascii_uppercase(),
                    Err(_) => return Ok(Some(err_syntax())),
                };
                match opt_upper.as_str() {
                    "EX" => {
                        if has_expire {
                            return Ok(Some(err_syntax()));
                        }
                        let Some(sec_bytes) = iter.next() else {
                            return Ok(Some(err_syntax()));
                        };
                        let sec = match parse_i64_from_bulk(sec_bytes) {
                            Ok(v) => v,
                            Err(e) => return Ok(Some(e)),
                        };
                        if sec < 0 {
                            return Ok(Some(err_not_integer()));
                        }
                        expire_millis = Some(sec.saturating_mul(1000));
                        has_expire = true;
                    }
                    "PX" => {
                        if has_expire {
                            return Ok(Some(err_syntax()));
                        }
                        let Some(ms_bytes) = iter.next() else {
                            return Ok(Some(err_syntax()));
                        };
                        let ms = match parse_i64_from_bulk(ms_bytes) {
                            Ok(v) => v,
                            Err(e) => return Ok(Some(e)),
                        };
                        if ms < 0 {
                            return Ok(Some(err_not_integer()));
                        }
                        expire_millis = Some(ms);
                        has_expire = true;
                    }
                    "EXAT" => {
                        if has_expire {
                            return Ok(Some(err_syntax()));
                        }
                        let Some(sec_bytes) = iter.next() else {
                            return Ok(Some(err_syntax()));
                        };
                        let sec = match parse_i64_from_bulk(sec_bytes) {
                            Ok(v) => v,
                            Err(e) => return Ok(Some(e)),
                        };
                        if sec < 0 {
                            return Ok(Some(err_not_integer()));
                        }
                        // 转为毫秒级绝对时间戳
                        expire_at_millis = Some(sec.saturating_mul(1000));
                        has_expire = true;
                    }
                    "PXAT" => {
                        if has_expire {
                            return Ok(Some(err_syntax()));
                        }
                        let Some(ms_bytes) = iter.next() else {
                            return Ok(Some(err_syntax()));
                        };
                        let ms = match parse_i64_from_bulk(ms_bytes) {
                            Ok(v) => v,
                            Err(e) => return Ok(Some(e)),
                        };
                        if ms < 0 {
                            return Ok(Some(err_not_integer()));
                        }
                        expire_at_millis = Some(ms);
                        has_expire = true;
                    }
                    "NX" => {
                        if xx {
                            return Ok(Some(err_syntax()));
                        }
                        nx = true;
                    }
                    "XX" => {
                        if nx {
                            return Ok(Some(err_syntax()));
                        }
                        xx = true;
                    }
                    "KEEPTTL" => {
                        keep_ttl = true;
                    }
                    "GET" => {
                        get = true;
                    }
                    _ => {
                        return Ok(Some(err_syntax()));
                    }
                }
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
            }
        }
        "GET" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("get")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("get")));
            }
            Command::Get { key }
        }
        "GETDEL" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("getdel")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("getdel")));
            }
            Command::Getdel { key }
        }
        "GETEX" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("getex")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };

            let mut expire_millis: Option<i64> = None;
            let mut has_ex = false;
            let mut has_px = false;
            let mut persist = false;

            while let Some(opt) = iter.next() {
                let opt_upper = match std::str::from_utf8(&opt) {
                    Ok(s) => s.to_ascii_uppercase(),
                    Err(_) => return Ok(Some(err_syntax())),
                };
                match opt_upper.as_str() {
                    "EX" => {
                        if has_ex || has_px || persist {
                            return Ok(Some(err_syntax()));
                        }
                        let Some(sec_bytes) = iter.next() else {
                            return Ok(Some(err_syntax()));
                        };
                        let sec = match parse_i64_from_bulk(sec_bytes) {
                            Ok(v) => v,
                            Err(e) => return Ok(Some(e)),
                        };
                        if sec < 0 {
                            return Ok(Some(err_not_integer()));
                        }
                        expire_millis = Some(sec.saturating_mul(1000));
                        has_ex = true;
                    }
                    "PX" => {
                        if has_ex || has_px || persist {
                            return Ok(Some(err_syntax()));
                        }
                        let Some(ms_bytes) = iter.next() else {
                            return Ok(Some(err_syntax()));
                        };
                        let ms = match parse_i64_from_bulk(ms_bytes) {
                            Ok(v) => v,
                            Err(e) => return Ok(Some(e)),
                        };
                        if ms < 0 {
                            return Ok(Some(err_not_integer()));
                        }
                        expire_millis = Some(ms);
                        has_px = true;
                    }
                    "PERSIST" => {
                        if has_ex || has_px || persist {
                            return Ok(Some(err_syntax()));
                        }
                        persist = true;
                    }
                    _ => {
                        return Ok(Some(err_syntax()));
                    }
                }
            }

            Command::Getex {
                key,
                expire_millis,
                persist,
            }
        }
        "GETRANGE" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("getrange")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(start_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("getrange")));
            };
            let Some(end_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("getrange")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("getrange")));
            }
            let start = match parse_isize_from_bulk(start_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(Some(e)),
            };
            let end = match parse_isize_from_bulk(end_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(Some(e)),
            };
            Command::Getrange { key, start, end }
        }
        "SETRANGE" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("setrange")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(offset_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("setrange")));
            };
            let Some(value) = iter.next() else {
                return Ok(Some(err_wrong_args("setrange")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("setrange")));
            }
            let offset_i64 = match parse_i64_from_bulk(offset_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(Some(e)),
            };
            if offset_i64 < 0 {
                return Ok(Some(Command::Error(
                    "ERR offset is out of range".to_string(),
                )));
            }
            let offset = offset_i64 as usize;
            Command::Setrange { key, offset, value }
        }
        "APPEND" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("append")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(value) = iter.next() else {
                return Ok(Some(err_wrong_args("append")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("append")));
            }
            Command::Append { key, value }
        }
        "STRLEN" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("strlen")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("strlen")));
            }
            Command::Strlen { key }
        }
        "GETSET" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("getset")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(value) = iter.next() else {
                return Ok(Some(err_wrong_args("getset")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("getset")));
            }
            Command::Getset { key, value }
        }
        "INCR" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("incr")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("incr")));
            }
            Command::Incr { key }
        }
        "DECR" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("decr")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("decr")));
            }
            Command::Decr { key }
        }
        "INCRBY" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("incrby")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(delta_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("incrby")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("incrby")));
            }
            let delta = match parse_i64_from_bulk(delta_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(Some(e)),
            };
            Command::Incrby { key, delta }
        }
        "INCRBYFLOAT" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("incrbyfloat")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(delta_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("incrbyfloat")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("incrbyfloat")));
            }
            let delta = match parse_f64_from_bulk(delta_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(Some(e)),
            };
            Command::Incrbyfloat { key, delta }
        }
        "DECRBY" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("decrby")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(delta_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("decrby")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("decrby")));
            }
            let delta = match parse_i64_from_bulk(delta_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(Some(e)),
            };
            Command::Decrby { key, delta }
        }
        "DEL" => {
            let mut keys = Vec::new();
            for b in iter {
                match parse_bulk_string(b) {
                    Ok(k) => keys.push(k),
                    Err(e) => return Ok(Some(e)),
                }
            }
            if keys.is_empty() {
                return Ok(Some(err_wrong_args("del")));
            }
            Command::Del { keys }
        }
        "EXISTS" => {
            let mut keys = Vec::new();
            for b in iter {
                match parse_bulk_string(b) {
                    Ok(k) => keys.push(k),
                    Err(e) => return Ok(Some(e)),
                }
            }
            if keys.is_empty() {
                return Ok(Some(err_wrong_args("exists")));
            }
            Command::Exists { keys }
        }
        "TYPE" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("type")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("type")));
            }
            Command::Type { key }
        }
        "DBSIZE" => {
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("dbsize")));
            }
            Command::Dbsize
        }
        "SCAN" => {
            let Some(cursor_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("scan")));
            };
            let cursor_i64 = match parse_i64_from_bulk(cursor_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(Some(e)),
            };
            if cursor_i64 < 0 {
                return Ok(Some(err_not_integer()));
            }
            let mut pattern: Option<String> = None;
            let mut count: Option<u64> = None;

            while let Some(opt) = iter.next() {
                let opt_upper = match std::str::from_utf8(&opt) {
                    Ok(s) => s.to_ascii_uppercase(),
                    Err(_) => return Ok(Some(err_syntax())),
                };
                match opt_upper.as_str() {
                    "MATCH" => {
                        if pattern.is_some() {
                            return Ok(Some(err_syntax()));
                        }
                        let Some(pat_bytes) = iter.next() else {
                            return Ok(Some(err_syntax()));
                        };
                        let pat = match parse_bulk_string(pat_bytes) {
                            Ok(p) => p,
                            Err(e) => return Ok(Some(e)),
                        };
                        pattern = Some(pat);
                    }
                    "COUNT" => {
                        if count.is_some() {
                            return Ok(Some(err_syntax()));
                        }
                        let Some(count_bytes) = iter.next() else {
                            return Ok(Some(err_syntax()));
                        };
                        let c_i64 = match parse_i64_from_bulk(count_bytes) {
                            Ok(v) => v,
                            Err(e) => return Ok(Some(e)),
                        };
                        if c_i64 < 0 {
                            return Ok(Some(err_not_integer()));
                        }
                        count = Some(c_i64 as u64);
                    }
                    _ => {
                        return Ok(Some(err_syntax()));
                    }
                }
            }

            Command::Scan {
                cursor: cursor_i64 as u64,
                pattern,
                count,
            }
        }
        "SSCAN" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("sscan")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(cursor_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("sscan")));
            };
            let cursor_i64 = match parse_i64_from_bulk(cursor_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(Some(e)),
            };
            if cursor_i64 < 0 {
                return Ok(Some(err_not_integer()));
            }
            let mut pattern: Option<String> = None;
            let mut count: Option<u64> = None;

            while let Some(opt) = iter.next() {
                let opt_upper = match std::str::from_utf8(&opt) {
                    Ok(s) => s.to_ascii_uppercase(),
                    Err(_) => return Ok(Some(err_syntax())),
                };
                match opt_upper.as_str() {
                    "MATCH" => {
                        if pattern.is_some() {
                            return Ok(Some(err_syntax()));
                        }
                        let Some(pat_bytes) = iter.next() else {
                            return Ok(Some(err_syntax()));
                        };
                        let pat = match parse_bulk_string(pat_bytes) {
                            Ok(p) => p,
                            Err(e) => return Ok(Some(e)),
                        };
                        pattern = Some(pat);
                    }
                    "COUNT" => {
                        if count.is_some() {
                            return Ok(Some(err_syntax()));
                        }
                        let Some(count_bytes) = iter.next() else {
                            return Ok(Some(err_syntax()));
                        };
                        let c_i64 = match parse_i64_from_bulk(count_bytes) {
                            Ok(v) => v,
                            Err(e) => return Ok(Some(e)),
                        };
                        if c_i64 < 0 {
                            return Ok(Some(err_not_integer()));
                        }
                        count = Some(c_i64 as u64);
                    }
                    _ => {
                        return Ok(Some(err_syntax()));
                    }
                }
            }

            Command::Sscan {
                key,
                cursor: cursor_i64 as u64,
                pattern,
                count,
            }
        }
        "HSCAN" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("hscan")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(cursor_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("hscan")));
            };
            let cursor_i64 = match parse_i64_from_bulk(cursor_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(Some(e)),
            };
            if cursor_i64 < 0 {
                return Ok(Some(err_not_integer()));
            }
            let mut pattern: Option<String> = None;
            let mut count: Option<u64> = None;

            while let Some(opt) = iter.next() {
                let opt_upper = match std::str::from_utf8(&opt) {
                    Ok(s) => s.to_ascii_uppercase(),
                    Err(_) => return Ok(Some(err_syntax())),
                };
                match opt_upper.as_str() {
                    "MATCH" => {
                        if pattern.is_some() {
                            return Ok(Some(err_syntax()));
                        }
                        let Some(pat_bytes) = iter.next() else {
                            return Ok(Some(err_syntax()));
                        };
                        let pat = match parse_bulk_string(pat_bytes) {
                            Ok(p) => p,
                            Err(e) => return Ok(Some(e)),
                        };
                        pattern = Some(pat);
                    }
                    "COUNT" => {
                        if count.is_some() {
                            return Ok(Some(err_syntax()));
                        }
                        let Some(count_bytes) = iter.next() else {
                            return Ok(Some(err_syntax()));
                        };
                        let c_i64 = match parse_i64_from_bulk(count_bytes) {
                            Ok(v) => v,
                            Err(e) => return Ok(Some(e)),
                        };
                        if c_i64 < 0 {
                            return Ok(Some(err_not_integer()));
                        }
                        count = Some(c_i64 as u64);
                    }
                    _ => {
                        return Ok(Some(err_syntax()));
                    }
                }
            }

            Command::Hscan {
                key,
                cursor: cursor_i64 as u64,
                pattern,
                count,
            }
        }
        "ZSCAN" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("zscan")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(cursor_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("zscan")));
            };
            let cursor_i64 = match parse_i64_from_bulk(cursor_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(Some(e)),
            };
            if cursor_i64 < 0 {
                return Ok(Some(err_not_integer()));
            }
            let mut pattern: Option<String> = None;
            let mut count: Option<u64> = None;

            while let Some(opt) = iter.next() {
                let opt_upper = match std::str::from_utf8(&opt) {
                    Ok(s) => s.to_ascii_uppercase(),
                    Err(_) => return Ok(Some(err_syntax())),
                };
                match opt_upper.as_str() {
                    "MATCH" => {
                        if pattern.is_some() {
                            return Ok(Some(err_syntax()));
                        }
                        let Some(pat_bytes) = iter.next() else {
                            return Ok(Some(err_syntax()));
                        };
                        let pat = match parse_bulk_string(pat_bytes) {
                            Ok(p) => p,
                            Err(e) => return Ok(Some(e)),
                        };
                        pattern = Some(pat);
                    }
                    "COUNT" => {
                        if count.is_some() {
                            return Ok(Some(err_syntax()));
                        }
                        let Some(count_bytes) = iter.next() else {
                            return Ok(Some(err_syntax()));
                        };
                        let c_i64 = match parse_i64_from_bulk(count_bytes) {
                            Ok(v) => v,
                            Err(e) => return Ok(Some(e)),
                        };
                        if c_i64 < 0 {
                            return Ok(Some(err_not_integer()));
                        }
                        count = Some(c_i64 as u64);
                    }
                    _ => {
                        return Ok(Some(err_syntax()));
                    }
                }
            }

            Command::Zscan {
                key,
                cursor: cursor_i64 as u64,
                pattern,
                count,
            }
        }
        "ZADD" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("zadd")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };

            let mut entries: Vec<(f64, String)> = Vec::new();
            while let Some(score_bytes) = iter.next() {
                let score = match parse_f64_from_bulk(score_bytes) {
                    Ok(v) => v,
                    Err(e) => return Ok(Some(e)),
                };
                let Some(member_bytes) = iter.next() else {
                    return Ok(Some(err_wrong_args("zadd")));
                };
                let member = match parse_bulk_string(member_bytes) {
                    Ok(m) => m,
                    Err(e) => return Ok(Some(e)),
                };
                entries.push((score, member));
            }

            if entries.is_empty() {
                return Ok(Some(err_wrong_args("zadd")));
            }

            Command::Zadd { key, entries }
        }
        "ZCARD" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("zcard")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };

            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("zcard")));
            }

            Command::Zcard { key }
        }
        "ZRANGE" | "ZREVRANGE" => {
            let is_rev = upper == "ZREVRANGE";
            let err_cmd = if is_rev { "zrevrange" } else { "zrange" };
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args(err_cmd)));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(start_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args(err_cmd)));
            };
            let start = match parse_isize_from_bulk(start_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(Some(e)),
            };
            let Some(stop_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args(err_cmd)));
            };
            let stop = match parse_isize_from_bulk(stop_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(Some(e)),
            };

            let mut withscores = false;
            if let Some(opt) = iter.next() {
                let upper_opt = match std::str::from_utf8(&opt) {
                    Ok(s) => s.to_ascii_uppercase(),
                    Err(_) => return Ok(Some(err_syntax())),
                };
                if upper_opt == "WITHSCORES" {
                    withscores = true;
                } else {
                    return Ok(Some(err_syntax()));
                }
                if iter.next().is_some() {
                    return Ok(Some(err_wrong_args(err_cmd)));
                }
            }

            Command::Zrange {
                key,
                start,
                stop,
                withscores,
                rev: is_rev,
            }
        }
        "ZSCORE" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("zscore")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(member_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("zscore")));
            };
            let member = match parse_bulk_string(member_bytes) {
                Ok(m) => m,
                Err(e) => return Ok(Some(e)),
            };

            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("zscore")));
            }

            Command::Zscore { key, member }
        }
        "ZREM" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("zrem")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };

            let mut members: Vec<String> = Vec::new();
            for member_bytes in iter {
                let m = match parse_bulk_string(member_bytes) {
                    Ok(v) => v,
                    Err(e) => return Ok(Some(e)),
                };
                members.push(m);
            }

            if members.is_empty() {
                return Ok(Some(err_wrong_args("zrem")));
            }

            Command::Zrem { key, members }
        }
        "ZINCRBY" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("zincrby")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(incr_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("zincrby")));
            };
            let increment = match parse_f64_from_bulk(incr_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(Some(e)),
            };
            let Some(member_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("zincrby")));
            };
            let member = match parse_bulk_string(member_bytes) {
                Ok(m) => m,
                Err(e) => return Ok(Some(e)),
            };

            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("zincrby")));
            }

            Command::Zincrby {
                key,
                increment,
                member,
            }
        }
        "SAVE" => {
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("save")));
            }
            Command::Save
        }
        "BGSAVE" => {
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("bgsave")));
            }
            Command::Bgsave
        }
        "LASTSAVE" => {
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("lastsave")));
            }
            Command::Lastsave
        }
        "MULTI" => {
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("multi")));
            }
            Command::Multi
        }
        "EXEC" => {
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("exec")));
            }
            Command::Exec
        }
        "DISCARD" => {
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("discard")));
            }
            Command::Discard
        }
        "WATCH" => {
            let mut keys: Vec<String> = Vec::new();
            for b in iter {
                match parse_bulk_string(b) {
                    Ok(k) => keys.push(k),
                    Err(e) => return Ok(Some(e)),
                }
            }
            if keys.is_empty() {
                return Ok(Some(err_wrong_args("watch")));
            }
            Command::Watch { keys }
        }
        "UNWATCH" => {
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("unwatch")));
            }
            Command::Unwatch
        }
        "KEYS" => {
            let Some(pattern_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("keys")));
            };
            let pattern = match parse_bulk_string(pattern_bytes) {
                Ok(p) => p,
                Err(e) => return Ok(Some(e)),
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("keys")));
            }
            Command::Keys { pattern }
        }
        "LPUSH" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("lpush")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let mut values: Vec<String> = Vec::new();
            for b in iter {
                match parse_bulk_string(b) {
                    Ok(v) => values.push(v),
                    Err(e) => return Ok(Some(e)),
                }
            }
            if values.is_empty() {
                return Ok(Some(err_wrong_args("lpush")));
            }
            Command::Lpush { key, values }
        }
        "RPUSH" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("rpush")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let mut values: Vec<String> = Vec::new();
            for b in iter {
                match parse_bulk_string(b) {
                    Ok(v) => values.push(v),
                    Err(e) => return Ok(Some(e)),
                }
            }
            if values.is_empty() {
                return Ok(Some(err_wrong_args("rpush")));
            }
            Command::Rpush { key, values }
        }
        "LPOP" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("lpop")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("lpop")));
            }
            Command::Lpop { key }
        }
        "RPOP" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("rpop")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("rpop")));
            }
            Command::Rpop { key }
        }
        "LRANGE" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("lrange")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(start_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("lrange")));
            };
            let Some(stop_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("lrange")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("lrange")));
            }
            let start = match parse_isize_from_bulk(start_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(Some(e)),
            };
            let stop = match parse_isize_from_bulk(stop_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(Some(e)),
            };
            Command::Lrange { key, start, stop }
        }
        "LLEN" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("llen")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("llen")));
            }
            Command::Llen { key }
        }
        "LINDEX" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("lindex")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(idx_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("lindex")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("lindex")));
            }
            let index = match parse_isize_from_bulk(idx_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(Some(e)),
            };
            Command::Lindex { key, index }
        }
        "LREM" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("lrem")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(count_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("lrem")));
            };
            let Some(value_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("lrem")));
            };
            let value = match parse_bulk_string(value_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(Some(e)),
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("lrem")));
            }
            let count = match parse_isize_from_bulk(count_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(Some(e)),
            };
            Command::Lrem { key, count, value }
        }
        "LTRIM" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("ltrim")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(start_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("ltrim")));
            };
            let Some(stop_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("ltrim")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("ltrim")));
            }
            let start = match parse_isize_from_bulk(start_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(Some(e)),
            };
            let stop = match parse_isize_from_bulk(stop_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(Some(e)),
            };
            Command::Ltrim { key, start, stop }
        }
        "SADD" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("sadd")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let mut members: Vec<String> = Vec::new();
            for b in iter {
                match parse_bulk_string(b) {
                    Ok(m) => members.push(m),
                    Err(e) => return Ok(Some(e)),
                }
            }
            if members.is_empty() {
                return Ok(Some(err_wrong_args("sadd")));
            }
            Command::Sadd { key, members }
        }
        "SREM" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("srem")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let mut members: Vec<String> = Vec::new();
            for b in iter {
                match parse_bulk_string(b) {
                    Ok(m) => members.push(m),
                    Err(e) => return Ok(Some(e)),
                }
            }
            if members.is_empty() {
                return Ok(Some(err_wrong_args("srem")));
            }
            Command::Srem { key, members }
        }
        "SMEMBERS" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("smembers")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("smembers")));
            }
            Command::Smembers { key }
        }
        "SCARD" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("scard")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("scard")));
            }
            Command::Scard { key }
        }
        "SPOP" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("spop")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let count = if let Some(next) = iter.next() {
                if iter.next().is_some() {
                    return Ok(Some(err_wrong_args("spop")));
                }
                let n = match parse_i64_from_bulk(next) {
                    Ok(v) => v,
                    Err(e) => return Ok(Some(e)),
                };
                if n < 0 {
                    return Ok(Some(err_not_integer()));
                }
                Some(n)
            } else {
                None
            };
            Command::Spop { key, count }
        }
        "SRANDMEMBER" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("srandmember")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let count = if let Some(next) = iter.next() {
                if iter.next().is_some() {
                    return Ok(Some(err_wrong_args("srandmember")));
                }
                let n = match parse_i64_from_bulk(next) {
                    Ok(v) => v,
                    Err(e) => return Ok(Some(e)),
                };
                Some(n)
            } else {
                None
            };
            Command::Srandmember { key, count }
        }
        "SISMEMBER" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("sismember")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(member_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("sismember")));
            };
            let member = match parse_bulk_string(member_bytes) {
                Ok(m) => m,
                Err(e) => return Ok(Some(e)),
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("sismember")));
            }
            Command::Sismember { key, member }
        }
        "SUNION" => {
            let mut keys = Vec::new();
            for b in iter {
                match parse_bulk_string(b) {
                    Ok(k) => keys.push(k),
                    Err(e) => return Ok(Some(e)),
                }
            }
            if keys.is_empty() {
                return Ok(Some(err_wrong_args("sunion")));
            }
            Command::Sunion { keys }
        }
        "SINTER" => {
            let mut keys = Vec::new();
            for b in iter {
                match parse_bulk_string(b) {
                    Ok(k) => keys.push(k),
                    Err(e) => return Ok(Some(e)),
                }
            }
            if keys.is_empty() {
                return Ok(Some(err_wrong_args("sinter")));
            }
            Command::Sinter { keys }
        }
        "SDIFF" => {
            let mut keys = Vec::new();
            for b in iter {
                match parse_bulk_string(b) {
                    Ok(k) => keys.push(k),
                    Err(e) => return Ok(Some(e)),
                }
            }
            if keys.is_empty() {
                return Ok(Some(err_wrong_args("sdiff")));
            }
            Command::Sdiff { keys }
        }
        "SUNIONSTORE" => {
            let Some(dest_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("sunionstore")));
            };
            let dest = match parse_bulk_string(dest_bytes) {
                Ok(d) => d,
                Err(e) => return Ok(Some(e)),
            };
            let mut keys: Vec<String> = Vec::new();
            for b in iter {
                match parse_bulk_string(b) {
                    Ok(k) => keys.push(k),
                    Err(e) => return Ok(Some(e)),
                }
            }
            if keys.is_empty() {
                return Ok(Some(err_wrong_args("sunionstore")));
            }
            Command::Sunionstore { dest, keys }
        }
        "SINTERSTORE" => {
            let Some(dest_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("sinterstore")));
            };
            let dest = match parse_bulk_string(dest_bytes) {
                Ok(d) => d,
                Err(e) => return Ok(Some(e)),
            };
            let mut keys: Vec<String> = Vec::new();
            for b in iter {
                match parse_bulk_string(b) {
                    Ok(k) => keys.push(k),
                    Err(e) => return Ok(Some(e)),
                }
            }
            if keys.is_empty() {
                return Ok(Some(err_wrong_args("sinterstore")));
            }
            Command::Sinterstore { dest, keys }
        }
        "SDIFFSTORE" => {
            let Some(dest_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("sdiffstore")));
            };
            let dest = match parse_bulk_string(dest_bytes) {
                Ok(d) => d,
                Err(e) => return Ok(Some(e)),
            };
            let mut keys: Vec<String> = Vec::new();
            for b in iter {
                match parse_bulk_string(b) {
                    Ok(k) => keys.push(k),
                    Err(e) => return Ok(Some(e)),
                }
            }
            if keys.is_empty() {
                return Ok(Some(err_wrong_args("sdiffstore")));
            }
            Command::Sdiffstore { dest, keys }
        }
        "HSET" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("hset")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(field_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("hset")));
            };
            let field = match parse_bulk_string(field_bytes) {
                Ok(f) => f,
                Err(e) => return Ok(Some(e)),
            };
            let Some(value_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("hset")));
            };
            let value = match parse_bulk_string(value_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(Some(e)),
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("hset")));
            }
            Command::Hset { key, field, value }
        }
        "HGET" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("hget")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(field_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("hget")));
            };
            let field = match parse_bulk_string(field_bytes) {
                Ok(f) => f,
                Err(e) => return Ok(Some(e)),
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("hget")));
            }
            Command::Hget { key, field }
        }
        "HDEL" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("hdel")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let mut fields: Vec<String> = Vec::new();
            for b in iter {
                match parse_bulk_string(b) {
                    Ok(f) => fields.push(f),
                    Err(e) => return Ok(Some(e)),
                }
            }
            if fields.is_empty() {
                return Ok(Some(err_wrong_args("hdel")));
            }
            Command::Hdel { key, fields }
        }
        "HEXISTS" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("hexists")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(field_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("hexists")));
            };
            let field = match parse_bulk_string(field_bytes) {
                Ok(f) => f,
                Err(e) => return Ok(Some(e)),
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("hexists")));
            }
            Command::Hexists { key, field }
        }
        "HGETALL" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("hgetall")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("hgetall")));
            }
            Command::Hgetall { key }
        }
        "HKEYS" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("hkeys")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("hkeys")));
            }
            Command::Hkeys { key }
        }
        "HVALS" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("hvals")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("hvals")));
            }
            Command::Hvals { key }
        }
        "HMGET" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("hmget")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let mut fields: Vec<String> = Vec::new();
            for b in iter {
                match parse_bulk_string(b) {
                    Ok(f) => fields.push(f),
                    Err(e) => return Ok(Some(e)),
                }
            }
            if fields.is_empty() {
                return Ok(Some(err_wrong_args("hmget")));
            }
            Command::Hmget { key, fields }
        }
        "HINCRBY" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("hincrby")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(field_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("hincrby")));
            };
            let field = match parse_bulk_string(field_bytes) {
                Ok(f) => f,
                Err(e) => return Ok(Some(e)),
            };
            let Some(delta_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("hincrby")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("hincrby")));
            }
            let delta = match parse_i64_from_bulk(delta_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(Some(e)),
            };
            Command::Hincrby { key, field, delta }
        }
        "HINCRBYFLOAT" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("hincrbyfloat")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(field_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("hincrbyfloat")));
            };
            let field = match parse_bulk_string(field_bytes) {
                Ok(f) => f,
                Err(e) => return Ok(Some(e)),
            };
            let Some(delta_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("hincrbyfloat")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("hincrbyfloat")));
            }
            let delta = match parse_f64_from_bulk(delta_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(Some(e)),
            };
            Command::Hincrbyfloat { key, field, delta }
        }
        "HLEN" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("hlen")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("hlen")));
            }
            Command::Hlen { key }
        }
        "EXPIRE" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("expire")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(sec_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("expire")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("expire")));
            }
            let seconds = match parse_i64_from_bulk(sec_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(Some(e)),
            };
            Command::Expire { key, seconds }
        }
        "PEXPIRE" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("pexpire")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(ms_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("pexpire")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("pexpire")));
            }
            let millis = match parse_i64_from_bulk(ms_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(Some(e)),
            };
            Command::Pexpire { key, millis }
        }
        "TTL" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("ttl")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("ttl")));
            }
            Command::Ttl { key }
        }
        "PTTL" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("pttl")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("pttl")));
            }
            Command::Pttl { key }
        }
        "PERSIST" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("persist")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("persist")));
            }
            Command::Persist { key }
        }
        "INFO" => {
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("info")));
            }
            Command::Info
        }
        "AUTH" => {
            let Some(password_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("auth")));
            };
            let password = match parse_bulk_string(password_bytes) {
                Ok(p) => p,
                Err(e) => return Ok(Some(e)),
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("auth")));
            }
            Command::Auth { password }
        }
        "SELECT" => {
            let Some(db_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("select")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("select")));
            }

            let db_idx = match parse_i64_from_bulk(db_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(Some(e)),
            };

            if db_idx < 0 || db_idx >= 16 {
                return Ok(Some(Command::Error(
                    "ERR DB index is out of range".to_string(),
                )));
            }

            Command::Select { db: db_idx as u8 }
        }
        "RENAME" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("rename")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(newkey_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("rename")));
            };
            let newkey = match parse_bulk_string(newkey_bytes) {
                Ok(n) => n,
                Err(e) => return Ok(Some(e)),
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("rename")));
            }
            Command::Rename { key, newkey }
        }
        "RENAMENX" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("renamenx")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(newkey_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("renamenx")));
            };
            let newkey = match parse_bulk_string(newkey_bytes) {
                Ok(n) => n,
                Err(e) => return Ok(Some(e)),
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("renamenx")));
            }
            Command::Renamenx { key, newkey }
        }
        "FLUSHDB" => match (iter.next(), iter.next()) {
            (None, None) => Command::Flushdb,
            (Some(arg_bytes), None) => {
                let u = match parse_bulk_string(arg_bytes) {
                    Ok(s) => s.to_ascii_uppercase(),
                    Err(e) => return Ok(Some(e)),
                };
                if u == "ASYNC" || u == "SYNC" {
                    Command::Flushdb
                } else {
                    return Ok(Some(err_wrong_args("flushdb")));
                }
            }
            _ => return Ok(Some(err_wrong_args("flushdb"))),
        },
        "FLUSHALL" => match (iter.next(), iter.next()) {
            (None, None) => Command::Flushall,
            (Some(arg_bytes), None) => {
                let u = match parse_bulk_string(arg_bytes) {
                    Ok(s) => s.to_ascii_uppercase(),
                    Err(e) => return Ok(Some(e)),
                };
                if u == "ASYNC" || u == "SYNC" {
                    Command::Flushall
                } else {
                    return Ok(Some(err_wrong_args("flushall")));
                }
            }
            _ => return Ok(Some(err_wrong_args("flushall"))),
        },
        "SUBSCRIBE" => {
            let mut channels = Vec::new();
            for b in iter {
                match parse_bulk_string(b) {
                    Ok(c) => channels.push(c),
                    Err(e) => return Ok(Some(e)),
                }
            }
            if channels.is_empty() {
                return Ok(Some(err_wrong_args("subscribe")));
            }
            Command::Subscribe { channels }
        }
        "UNSUBSCRIBE" => {
            let mut channels = Vec::new();
            for b in iter {
                match parse_bulk_string(b) {
                    Ok(c) => channels.push(c),
                    Err(e) => return Ok(Some(e)),
                }
            }
            Command::Unsubscribe { channels }
        }
        "SSUBSCRIBE" => {
            let mut channels = Vec::new();
            for b in iter {
                match parse_bulk_string(b) {
                    Ok(c) => channels.push(c),
                    Err(e) => return Ok(Some(e)),
                }
            }
            if channels.is_empty() {
                return Ok(Some(err_wrong_args("ssubscribe")));
            }
            Command::Ssubscribe { channels }
        }
        "SUNSUBSCRIBE" => {
            let mut channels = Vec::new();
            for b in iter {
                match parse_bulk_string(b) {
                    Ok(c) => channels.push(c),
                    Err(e) => return Ok(Some(e)),
                }
            }
            Command::Sunsubscribe { channels }
        }
        "PSUBSCRIBE" => {
            let mut patterns = Vec::new();
            for b in iter {
                match parse_bulk_string(b) {
                    Ok(p) => patterns.push(p),
                    Err(e) => return Ok(Some(e)),
                }
            }
            if patterns.is_empty() {
                return Ok(Some(err_wrong_args("psubscribe")));
            }
            Command::Psubscribe { patterns }
        }
        "PUNSUBSCRIBE" => {
            let mut patterns = Vec::new();
            for b in iter {
                match parse_bulk_string(b) {
                    Ok(p) => patterns.push(p),
                    Err(e) => return Ok(Some(e)),
                }
            }
            Command::Punsubscribe { patterns }
        }
        "PUBLISH" => {
            let Some(channel_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("publish")));
            };
            let Some(message) = iter.next() else {
                return Ok(Some(err_wrong_args("publish")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("publish")));
            }
            let channel = match parse_bulk_string(channel_bytes) {
                Ok(c) => c,
                Err(e) => return Ok(Some(e)),
            };
            Command::Publish { channel, message }
        }
        "SPUBLISH" => {
            let Some(channel_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("spublish")));
            };
            let Some(message) = iter.next() else {
                return Ok(Some(err_wrong_args("spublish")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("spublish")));
            }
            let channel = match parse_bulk_string(channel_bytes) {
                Ok(c) => c,
                Err(e) => return Ok(Some(e)),
            };
            Command::Spublish { channel, message }
        }
        "PUBSUB" => {
            let Some(subcmd_bytes) = iter.next() else {
                return Ok(Some(err_pubsub_args()));
            };
            let subcmd_upper = match parse_bulk_string(subcmd_bytes) {
                Ok(s) => s.to_ascii_uppercase(),
                Err(e) => return Ok(Some(e)),
            };

            match subcmd_upper.as_str() {
                "CHANNELS" => {
                    let pattern = if let Some(pat_bytes) = iter.next() {
                        let pat = match parse_bulk_string(pat_bytes) {
                            Ok(p) => p,
                            Err(e) => return Ok(Some(e)),
                        };
                        if iter.next().is_some() {
                            return Ok(Some(err_pubsub_args()));
                        }
                        Some(pat)
                    } else {
                        None
                    };
                    Command::PubsubChannels { pattern }
                }
                "NUMSUB" => {
                    let mut channels = Vec::new();
                    for b in iter {
                        match parse_bulk_string(b) {
                            Ok(c) => channels.push(c),
                            Err(e) => return Ok(Some(e)),
                        }
                    }
                    Command::PubsubNumsub { channels }
                }
                "NUMPAT" => {
                    if iter.next().is_some() {
                        return Ok(Some(err_pubsub_args()));
                    }
                    Command::PubsubNumpat
                }
                "SHARDCHANNELS" => {
                    let pattern = if let Some(pat_bytes) = iter.next() {
                        let pat = match parse_bulk_string(pat_bytes) {
                            Ok(p) => p,
                            Err(e) => return Ok(Some(e)),
                        };
                        if iter.next().is_some() {
                            return Ok(Some(err_pubsub_args()));
                        }
                        Some(pat)
                    } else {
                        None
                    };
                    Command::PubsubShardchannels { pattern }
                }
                "SHARDNUMSUB" => {
                    let mut channels = Vec::new();
                    for b in iter {
                        match parse_bulk_string(b) {
                            Ok(c) => channels.push(c),
                            Err(e) => return Ok(Some(e)),
                        }
                    }
                    Command::PubsubShardnumsub { channels }
                }
                "HELP" => {
                    if iter.next().is_some() {
                        return Ok(Some(err_pubsub_args()));
                    }
                    Command::PubsubHelp
                }
                _ => return Ok(Some(err_pubsub_args())),
            }
        }
        "MGET" => {
            let mut keys = Vec::new();
            for b in iter {
                match parse_bulk_string(b) {
                    Ok(k) => keys.push(k),
                    Err(e) => return Ok(Some(e)),
                }
            }
            if keys.is_empty() {
                return Ok(Some(err_wrong_args("mget")));
            }
            Command::Mget { keys }
        }
        "MSET" => {
            let mut pairs = Vec::new();
            let mut args_iter = iter;
            loop {
                let Some(k_bytes) = args_iter.next() else {
                    break;
                };
                let Some(v_bytes) = args_iter.next() else {
                    return Ok(Some(err_wrong_args("mset")));
                };
                let k = match parse_bulk_string(k_bytes) {
                    Ok(k) => k,
                    Err(e) => return Ok(Some(e)),
                };
                pairs.push((k, v_bytes));
            }
            if pairs.is_empty() {
                return Ok(Some(err_wrong_args("mset")));
            }
            Command::Mset { pairs }
        }
        "MSETNX" => {
            let mut pairs = Vec::new();
            let mut args_iter = iter;
            loop {
                let Some(k_bytes) = args_iter.next() else {
                    break;
                };
                let Some(v_bytes) = args_iter.next() else {
                    return Ok(Some(err_wrong_args("msetnx")));
                };
                let k = match parse_bulk_string(k_bytes) {
                    Ok(k) => k,
                    Err(e) => return Ok(Some(e)),
                };
                pairs.push((k, v_bytes));
            }
            if pairs.is_empty() {
                return Ok(Some(err_wrong_args("msetnx")));
            }
            Command::Msetnx { pairs }
        }
        "SETNX" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("setnx")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(value) = iter.next() else {
                return Ok(Some(err_wrong_args("setnx")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("setnx")));
            }
            Command::Setnx { key, value }
        }
        "SETEX" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("setex")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(sec_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("setex")));
            };
            let Some(value) = iter.next() else {
                return Ok(Some(err_wrong_args("setex")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("setex")));
            }
            let seconds = match parse_i64_from_bulk(sec_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(Some(e)),
            };
            if seconds < 0 {
                return Ok(Some(err_not_integer()));
            }
            Command::Setex {
                key,
                seconds,
                value,
            }
        }
        "PSETEX" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("psetex")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(ms_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("psetex")));
            };
            let Some(value) = iter.next() else {
                return Ok(Some(err_wrong_args("psetex")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("psetex")));
            }
            let millis = match parse_i64_from_bulk(ms_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(Some(e)),
            };
            if millis < 0 {
                return Ok(Some(err_not_integer()));
            }
            Command::Psetex { key, millis, value }
        }
        _ => Command::Unknown(std::iter::once(command_bytes).chain(iter).collect()),
    };

    Ok(Some(cmd))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn parses_basic_commands() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = tokio::spawn(async move {
            let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            stream.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
            stream
                .write_all(b"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n")
                .await
                .unwrap();
            stream.write_all(b"*1\r\n$4\r\nQUIT\r\n").await.unwrap();
            stream
                .write_all(b"*2\r\n$7\r\nCOMMAND\r\n$4\r\nDOCS\r\n")
                .await
                .unwrap();
            // Test payload for PING
            stream
                .write_all(b"*2\r\n$4\r\nPING\r\n$1\r\nX\r\n")
                .await
                .unwrap();
            // Test too many arguments for PING
            stream
                .write_all(b"*3\r\n$4\r\nPING\r\n$1\r\nY\r\n$1\r\nZ\r\n")
                .await
                .unwrap();
            // Test too few arguments for ECHO
            stream.write_all(b"*1\r\n$4\r\nECHO\r\n").await.unwrap();
            // Test too many arguments for ECHO
            stream
                .write_all(b"*3\r\n$4\r\nECHO\r\n$5\r\nhello\r\n$5\r\nworld\r\n")
                .await
                .unwrap();
        });

        let (stream, _) = listener.accept().await.unwrap();
        let (read_half, _) = stream.into_split();
        let mut reader = BufReader::new(read_half);

        // PING
        if let Some(cmd) = read_command(&mut reader).await.unwrap() {
            assert!(matches!(cmd, Command::Ping));
        } else {
            panic!("expected PING command");
        }

        // ECHO hello
        if let Some(cmd) = read_command(&mut reader).await.unwrap() {
            if let Command::Echo(value) = cmd {
                assert_eq!(value, b"hello".to_vec());
            } else {
                panic!("expected ECHO command");
            }
        } else {
            panic!("expected ECHO command");
        }

        // QUIT
        if let Some(cmd) = read_command(&mut reader).await.unwrap() {
            assert!(matches!(cmd, Command::Quit));
        } else {
            panic!("expected QUIT command");
        }

        // COMMAND DOCS
        if let Some(cmd) = read_command(&mut reader).await.unwrap() {
            if let Command::Unknown(parts) = cmd {
                assert_eq!(parts, vec![b"COMMAND".to_vec(), b"DOCS".to_vec()]);
            } else {
                panic!("expected Unknown command");
            }
        } else {
            panic!("expected Unknown command");
        }

        // PING X payload
        if let Some(cmd) = read_command(&mut reader).await.unwrap() {
            if let Command::PingWithPayload(p) = cmd {
                assert_eq!(p, b"X".to_vec());
            } else {
                panic!("expected PING with payload");
            }
        } else {
            panic!("expected PING payload command");
        }

        // PING with too many args -> Error
        if let Some(cmd) = read_command(&mut reader).await.unwrap() {
            if let Command::Error(msg) = cmd {
                assert_eq!(msg, "ERR wrong number of arguments for 'ping' command");
            } else {
                panic!("expected Command::Error for too many PING arguments");
            }
        } else {
            panic!("expected PING error command");
        }

        // ECHO (Error)
        if let Some(cmd) = read_command(&mut reader).await.unwrap() {
            if let Command::Error(msg) = cmd {
                assert_eq!(msg, "ERR wrong number of arguments for 'echo' command");
            } else {
                panic!("expected Command::Error for too few ECHO arguments");
            }
        } else {
            panic!("expected ECHO error command");
        }

        // ECHO hello world (Error)
        if let Some(cmd) = read_command(&mut reader).await.unwrap() {
            if let Command::Error(msg) = cmd {
                assert_eq!(msg, "ERR wrong number of arguments for 'echo' command");
            } else {
                panic!("expected Command::Error for too many ECHO arguments");
            }
        } else {
            panic!("expected ECHO error command");
        }

        client.await.unwrap();
    }

    #[tokio::test]
    async fn parses_list_commands() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = tokio::spawn(async move {
            let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            // LPUSH mylist a b
            stream
                .write_all(b"*4\r\n$5\r\nLPUSH\r\n$6\r\nmylist\r\n$1\r\na\r\n$1\r\nb\r\n")
                .await
                .unwrap();
            // RPUSH mylist c
            stream
                .write_all(b"*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$1\r\nc\r\n")
                .await
                .unwrap();
            // LRANGE mylist 0 -1
            stream
                .write_all(b"*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$1\r\n0\r\n$2\r\n-1\r\n")
                .await
                .unwrap();
            // LPUSH mylist (Error)
            stream
                .write_all(b"*2\r\n$5\r\nLPUSH\r\n$6\r\nmylist\r\n")
                .await
                .unwrap();
            // LPOP (Error)
            stream.write_all(b"*1\r\n$4\r\nLPOP\r\n").await.unwrap();
            // LPOP mylist X (Error)
            stream
                .write_all(b"*3\r\n$4\r\nLPOP\r\n$6\r\nmylist\r\n$1\r\nX\r\n")
                .await
                .unwrap();
        });

        let (stream, _) = listener.accept().await.unwrap();
        let (read_half, _) = stream.into_split();
        let mut reader = BufReader::new(read_half);

        // LPUSH mylist a b
        if let Some(cmd) = read_command(&mut reader).await.unwrap() {
            match cmd {
                Command::Lpush { key, values } => {
                    assert_eq!(key, "mylist");
                    assert_eq!(values, vec!["a".to_string(), "b".to_string()]);
                }
                _ => panic!("expected LPUSH"),
            }
        } else {
            panic!("expected LPUSH command");
        }

        // RPUSH mylist c
        if let Some(cmd) = read_command(&mut reader).await.unwrap() {
            match cmd {
                Command::Rpush { key, values } => {
                    assert_eq!(key, "mylist");
                    assert_eq!(values, vec!["c".to_string()]);
                }
                _ => panic!("expected RPUSH"),
            }
        } else {
            panic!("expected RPUSH command");
        }

        // LRANGE mylist 0 -1
        if let Some(cmd) = read_command(&mut reader).await.unwrap() {
            match cmd {
                Command::Lrange { key, start, stop } => {
                    assert_eq!(key, "mylist");
                    assert_eq!(start, 0);
                    assert_eq!(stop, -1);
                }
                _ => panic!("expected LRANGE"),
            }
        } else {
            panic!("expected LRANGE command");
        }

        // LPUSH mylist (Error)
        if let Some(cmd) = read_command(&mut reader).await.unwrap() {
            if let Command::Error(msg) = cmd {
                assert_eq!(msg, "ERR wrong number of arguments for 'lpush' command");
            } else {
                panic!("expected Command::Error for too few LPUSH arguments");
            }
        } else {
            panic!("expected LPUSH error command");
        }

        // LPOP (Error)
        if let Some(cmd) = read_command(&mut reader).await.unwrap() {
            if let Command::Error(msg) = cmd {
                assert_eq!(msg, "ERR wrong number of arguments for 'lpop' command");
            } else {
                panic!("expected Command::Error for too few LPOP arguments");
            }
        } else {
            panic!("expected LPOP error command");
        }

        // LPOP mylist X (Error)
        if let Some(cmd) = read_command(&mut reader).await.unwrap() {
            if let Command::Error(msg) = cmd {
                assert_eq!(msg, "ERR wrong number of arguments for 'lpop' command");
            } else {
                panic!("expected Command::Error for too many LPOP arguments");
            }
        } else {
            panic!("expected LPOP error command");
        }

        client.await.unwrap();
    }

    #[tokio::test]
    async fn parses_set_commands() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = tokio::spawn(async move {
            let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            // SADD myset a b
            stream
                .write_all(b"*4\r\n$4\r\nSADD\r\n$5\r\nmyset\r\n$1\r\na\r\n$1\r\nb\r\n")
                .await
                .unwrap();
            // SCARD myset
            stream
                .write_all(b"*2\r\n$5\r\nSCARD\r\n$5\r\nmyset\r\n")
                .await
                .unwrap();
            // SISMEMBER myset a
            stream
                .write_all(b"*3\r\n$9\r\nSISMEMBER\r\n$5\r\nmyset\r\n$1\r\na\r\n")
                .await
                .unwrap();
            // SADD myset (Error)
            stream
                .write_all(b"*2\r\n$4\r\nSADD\r\n$5\r\nmyset\r\n")
                .await
                .unwrap();
            // SMEMBERS (Error)
            stream.write_all(b"*1\r\n$8\r\nSMEMBERS\r\n").await.unwrap();
            // SMEMBERS myset X (Error)
            stream
                .write_all(b"*3\r\n$8\r\nSMEMBERS\r\n$5\r\nmyset\r\n$1\r\nX\r\n")
                .await
                .unwrap();
        });

        let (stream, _) = listener.accept().await.unwrap();
        let (read_half, _) = stream.into_split();
        let mut reader = BufReader::new(read_half);

        // SADD myset a b
        if let Some(cmd) = read_command(&mut reader).await.unwrap() {
            match cmd {
                Command::Sadd { key, members } => {
                    assert_eq!(key, "myset");
                    assert_eq!(members, vec!["a".to_string(), "b".to_string()]);
                }
                _ => panic!("expected SADD"),
            }
        } else {
            panic!("expected SADD command");
        }

        // SCARD myset
        if let Some(cmd) = read_command(&mut reader).await.unwrap() {
            match cmd {
                Command::Scard { key } => {
                    assert_eq!(key, "myset");
                }
                _ => panic!("expected SCARD"),
            }
        } else {
            panic!("expected SCARD command");
        }

        // SISMEMBER myset a
        if let Some(cmd) = read_command(&mut reader).await.unwrap() {
            match cmd {
                Command::Sismember { key, member } => {
                    assert_eq!(key, "myset");
                    assert_eq!(member, "a");
                }
                _ => panic!("expected SISMEMBER"),
            }
        } else {
            panic!("expected SISMEMBER command");
        }

        // SADD myset (Error)
        if let Some(cmd) = read_command(&mut reader).await.unwrap() {
            if let Command::Error(msg) = cmd {
                assert_eq!(msg, "ERR wrong number of arguments for 'sadd' command");
            } else {
                panic!("expected Command::Error for too few SADD arguments");
            }
        } else {
            panic!("expected SADD error command");
        }

        // SMEMBERS (Error)
        if let Some(cmd) = read_command(&mut reader).await.unwrap() {
            if let Command::Error(msg) = cmd {
                assert_eq!(msg, "ERR wrong number of arguments for 'smembers' command");
            } else {
                panic!("expected Command::Error for too few SMEMBERS arguments");
            }
        } else {
            panic!("expected SMEMBERS error command");
        }

        // SMEMBERS myset X (Error)
        if let Some(cmd) = read_command(&mut reader).await.unwrap() {
            if let Command::Error(msg) = cmd {
                assert_eq!(msg, "ERR wrong number of arguments for 'smembers' command");
            } else {
                panic!("expected Command::Error for too many SMEMBERS arguments");
            }
        } else {
            panic!("expected SMEMBERS error command");
        }

        client.await.unwrap();
    }
}
