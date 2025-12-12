use std::fmt;
use tokio::io::{self, BufReader};

use crate::resp::read_resp_array;

pub type Binary = Vec<u8>;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub struct StreamId {
    pub ms: u64,
    pub seq: u64,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum StreamReadId {
    Id(StreamId),
    Latest,
}

/// ZINTER/ZUNION 的聚合方式
#[derive(Debug, Clone, Copy)]
pub enum ZAggregate {
    Sum,
    Min,
    Max,
}

/// ZCOUNT/ZRANGEBYSCORE 的分数边界
#[derive(Debug, Clone)]
pub enum ZRangeBound {
    /// 负无穷
    NegInf,
    /// 正无穷
    PosInf,
    /// 包含该值 (inclusive)
    Inclusive(f64),
    /// 不包含该值 (exclusive)
    Exclusive(f64),
}

/// ZLEXCOUNT/ZRANGEBYLEX 的字典序边界
#[derive(Debug, Clone)]
pub enum ZLexBound {
    /// 负无穷 (-)
    NegInf,
    /// 正无穷 (+)
    PosInf,
    /// 包含该值 [value
    Inclusive(String),
    /// 不包含该值 (value
    Exclusive(String),
}

/// BITFIELD 整数编码类型
#[derive(Debug, Clone, Copy)]
pub enum BitfieldEncoding {
    /// 有符号整数 (i1-i64)
    Signed(u8),
    /// 无符号整数 (u1-u63)
    Unsigned(u8),
}

/// BITFIELD 溢出行为
#[derive(Debug, Clone, Copy, Default)]
pub enum BitfieldOverflow {
    /// 环绕（默认）
    #[default]
    Wrap,
    /// 饱和
    Sat,
    /// 失败（返回 nil）
    Fail,
}

/// Geo 距离单位
#[derive(Debug, Clone, Copy, Default)]
pub enum GeoUnit {
    #[default]
    Meters,
    Kilometers,
    Miles,
    Feet,
}

impl GeoUnit {
    pub fn to_meters(&self, value: f64) -> f64 {
        match self {
            GeoUnit::Meters => value,
            GeoUnit::Kilometers => value * 1000.0,
            GeoUnit::Miles => value * 1609.34,
            GeoUnit::Feet => value * 0.3048,
        }
    }

    pub fn from_meters(&self, meters: f64) -> f64 {
        match self {
            GeoUnit::Meters => meters,
            GeoUnit::Kilometers => meters / 1000.0,
            GeoUnit::Miles => meters / 1609.34,
            GeoUnit::Feet => meters / 0.3048,
        }
    }
}

/// BITFIELD 子命令
#[derive(Debug, Clone)]
pub enum BitfieldOp {
    /// GET <encoding> <offset>
    Get {
        encoding: BitfieldEncoding,
        offset: i64,
        offset_multiplier: bool,
    },
    /// SET <encoding> <offset> <value>
    Set {
        encoding: BitfieldEncoding,
        offset: i64,
        offset_multiplier: bool,
        value: i64,
    },
    /// INCRBY <encoding> <offset> <increment>
    Incrby {
        encoding: BitfieldEncoding,
        offset: i64,
        offset_multiplier: bool,
        increment: i64,
    },
    /// OVERFLOW [WRAP|SAT|FAIL]
    Overflow(BitfieldOverflow),
}

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
    Time,
    Randomkey,
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
    // Bitmap commands
    Setbit {
        key: String,
        offset: u64,
        value: bool,
    },
    Getbit {
        key: String,
        offset: u64,
    },
    Bitcount {
        key: String,
        start: Option<i64>,
        end: Option<i64>,
        use_bit: bool,
    },
    Bitpos {
        key: String,
        bit: bool,
        start: Option<i64>,
        end: Option<i64>,
        use_bit: bool,
    },
    Bitop {
        op: String,
        destkey: String,
        keys: Vec<String>,
    },
    Bitfield {
        key: String,
        ops: Vec<BitfieldOp>,
    },
    BitfieldRo {
        key: String,
        ops: Vec<BitfieldOp>,
    },
    // Streams
    Xadd {
        key: String,
        id: Option<StreamId>,
        fields: Vec<(Binary, Binary)>,
    },
    Xlen {
        key: String,
    },
    XinfoStream {
        key: String,
    },
    Xrange {
        key: String,
        start: Option<StreamId>,
        end: Option<StreamId>,
        count: Option<usize>,
    },
    Xread {
        count: Option<usize>,
        block_millis: Option<u64>,
        streams: Vec<(String, StreamReadId)>,
    },
    // XGROUP subcommands
    XgroupCreate {
        key: String,
        group: String,
        id: StreamId,
        mkstream: bool,
    },
    XgroupSetid {
        key: String,
        group: String,
        id: StreamId,
    },
    XgroupDestroy {
        key: String,
        group: String,
    },
    XgroupCreateconsumer {
        key: String,
        group: String,
        consumer: String,
    },
    XgroupDelconsumer {
        key: String,
        group: String,
        consumer: String,
    },
    Xreadgroup {
        group: String,
        consumer: String,
        count: Option<usize>,
        block_millis: Option<u64>,
        noack: bool,
        streams: Vec<(String, Option<StreamId>)>, // None means ">"
    },
    Xack {
        key: String,
        group: String,
        ids: Vec<StreamId>,
    },
    Xpending {
        key: String,
        group: String,
    },
    // Geo commands
    Geoadd {
        key: String,
        members: Vec<(f64, f64, String)>, // (longitude, latitude, member)
    },
    Geopos {
        key: String,
        members: Vec<String>,
    },
    Geodist {
        key: String,
        member1: String,
        member2: String,
        unit: GeoUnit,
    },
    Geohash {
        key: String,
        members: Vec<String>,
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
    Unlink {
        keys: Vec<String>,
    },
    Copy {
        source: String,
        destination: String,
        replace: bool,
    },
    Touch {
        keys: Vec<String>,
    },
    ObjectEncoding {
        key: String,
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
        type_filter: Option<String>,
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
        novalues: bool,
    },
    Zscan {
        key: String,
        cursor: u64,
        pattern: Option<String>,
        count: Option<u64>,
        novalues: bool,
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
    Lset {
        key: String,
        index: isize,
        value: String,
    },
    Linsert {
        key: String,
        before: bool, // true = BEFORE, false = AFTER
        pivot: String,
        value: String,
    },
    Rpoplpush {
        source: String,
        destination: String,
    },
    Blpop {
        keys: Vec<String>,
        timeout: f64,
    },
    Brpop {
        keys: Vec<String>,
        timeout: f64,
    },
    Lpos {
        key: String,
        element: String,
        rank: Option<isize>,
        count: Option<isize>,
        maxlen: Option<isize>,
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
    Smove {
        source: String,
        destination: String,
        member: String,
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
    Hsetnx {
        key: String,
        field: String,
        value: String,
    },
    Hstrlen {
        key: String,
        field: String,
    },
    Hmset {
        key: String,
        field_values: Vec<(String, String)>,
    },
    Expire {
        key: String,
        seconds: i64,
    },
    Pexpire {
        key: String,
        millis: i64,
    },
    Expireat {
        key: String,
        timestamp: i64,
    },
    Pexpireat {
        key: String,
        timestamp_ms: i64,
    },
    Ttl {
        key: String,
    },
    Pttl {
        key: String,
    },
    Expiretime {
        key: String,
    },
    Pexpiretime {
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
    Zmscore {
        key: String,
        members: Vec<String>,
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
    Zcount {
        key: String,
        min: ZRangeBound,
        max: ZRangeBound,
    },
    Zrank {
        key: String,
        member: String,
    },
    Zrevrank {
        key: String,
        member: String,
    },
    Zpopmin {
        key: String,
        count: Option<usize>,
    },
    Zpopmax {
        key: String,
        count: Option<usize>,
    },
    Zinter {
        keys: Vec<String>,
        weights: Option<Vec<f64>>,
        aggregate: Option<ZAggregate>,
        withscores: bool,
    },
    Zunion {
        keys: Vec<String>,
        weights: Option<Vec<f64>>,
        aggregate: Option<ZAggregate>,
        withscores: bool,
    },
    Zdiff {
        keys: Vec<String>,
        withscores: bool,
    },
    Zinterstore {
        destination: String,
        keys: Vec<String>,
        weights: Option<Vec<f64>>,
        aggregate: Option<ZAggregate>,
    },
    Zunionstore {
        destination: String,
        keys: Vec<String>,
        weights: Option<Vec<f64>>,
        aggregate: Option<ZAggregate>,
    },
    Zdiffstore {
        destination: String,
        keys: Vec<String>,
    },
    Zlexcount {
        key: String,
        min: ZLexBound,
        max: ZLexBound,
    },
    // HyperLogLog 命令
    Pfadd {
        key: String,
        elements: Vec<Binary>,
    },
    Pfcount {
        keys: Vec<String>,
    },
    Pfmerge {
        destkey: String,
        sourcekeys: Vec<String>,
    },
    // Lua 脚本命令
    Eval {
        script: String,
        keys: Vec<String>,
        args: Vec<String>,
    },
    Evalsha {
        sha1: String,
        keys: Vec<String>,
        args: Vec<String>,
    },
    ScriptLoad {
        script: String,
    },
    ScriptExists {
        sha1s: Vec<String>,
    },
    ScriptFlush,
    // 运维命令
    ConfigGet {
        pattern: String,
    },
    ConfigSet {
        parameter: String,
        value: String,
    },
    ClientList,
    ClientId,
    ClientSetname {
        name: String,
    },
    ClientGetname,
    ClientPause {
        timeout_ms: u64,
    },
    ClientUnpause,
    SlowlogGet {
        count: Option<usize>,
    },
    SlowlogReset,
    SlowlogLen,
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

/// 从迭代器中提取必需的 key 参数
fn require_key(iter: &mut impl Iterator<Item = Vec<u8>>, cmd: &str) -> Result<String, Command> {
    let key_bytes = iter.next().ok_or_else(|| err_wrong_args(cmd))?;
    parse_bulk_string(key_bytes)
}

/// 从迭代器中提取必需的 i64 参数
fn require_i64(iter: &mut impl Iterator<Item = Vec<u8>>, cmd: &str) -> Result<i64, Command> {
    let bytes = iter.next().ok_or_else(|| err_wrong_args(cmd))?;
    parse_i64_from_bulk(bytes)
}

/// 从迭代器中提取必需的 f64 参数
fn require_f64(iter: &mut impl Iterator<Item = Vec<u8>>, cmd: &str) -> Result<f64, Command> {
    let bytes = iter.next().ok_or_else(|| err_wrong_args(cmd))?;
    parse_f64_from_bulk(bytes)
}

fn parse_stream_id_token(token: Vec<u8>) -> Result<StreamId, Command> {
    let s = parse_bulk_string(token)?;
    parse_stream_id(&s)
}

fn parse_stream_id(s: &str) -> Result<StreamId, Command> {
    let Some((ms_s, seq_s)) = s.split_once('-') else {
        return Err(err_syntax());
    };
    let ms: u64 = ms_s.parse().map_err(|_| err_syntax())?;
    let seq: u64 = seq_s.parse().map_err(|_| err_syntax())?;
    Ok(StreamId { ms, seq })
}

fn parse_stream_read_id_token(token: Vec<u8>) -> Result<StreamReadId, Command> {
    let s = parse_bulk_string(token)?;
    if s == "$" {
        return Ok(StreamReadId::Latest);
    }
    if let Some((ms_s, seq_s)) = s.split_once('-') {
        let ms: u64 = ms_s.parse().map_err(|_| err_syntax())?;
        let seq: u64 = seq_s.parse().map_err(|_| err_syntax())?;
        return Ok(StreamReadId::Id(StreamId { ms, seq }));
    }
    let ms: u64 = s.parse().map_err(|_| err_syntax())?;
    Ok(StreamReadId::Id(StreamId { ms, seq: 0 }))
}

/// 确保迭代器中没有多余参数
fn ensure_no_more_args(iter: &mut impl Iterator<Item = Vec<u8>>, cmd: &str) -> Result<(), Command> {
    if iter.next().is_some() {
        Err(err_wrong_args(cmd))
    } else {
        Ok(())
    }
}

/// 收集所有剩余参数为字符串列表
fn collect_keys(iter: impl Iterator<Item = Vec<u8>>) -> Result<Vec<String>, Command> {
    iter.map(parse_bulk_string).collect()
}

/// 宏：简化从 Result<T, Command> 到 Ok(Some(Command)) 的错误处理
macro_rules! try_cmd {
    ($expr:expr) => {
        match $expr {
            Ok(v) => v,
            Err(e) => return Ok(Some(e)),
        }
    };
}

fn parse_i64_from_bulk(bytes: Vec<u8>) -> Result<i64, Command> {
    let s = parse_bulk_string(bytes)?;
    s.parse::<i64>().map_err(|_| err_not_integer())
}

fn parse_u64_from_bulk(bytes: Vec<u8>) -> Result<u64, Command> {
    let s = parse_bulk_string(bytes)?;
    s.parse::<u64>().map_err(|_| err_not_integer())
}

fn parse_bitfield_encoding(s: &str) -> Result<BitfieldEncoding, String> {
    if s.is_empty() {
        return Err("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is".to_string());
    }
    let (signed, rest) = match s.chars().next() {
        Some('i') | Some('I') => (true, &s[1..]),
        Some('u') | Some('U') => (false, &s[1..]),
        _ => return Err("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is".to_string()),
    };
    let bits: u8 = rest.parse().map_err(|_| {
        "ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is".to_string()
    })?;
    if bits == 0 {
        return Err("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is".to_string());
    }
    if signed {
        if bits > 64 {
            return Err("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is".to_string());
        }
        Ok(BitfieldEncoding::Signed(bits))
    } else {
        if bits > 63 {
            return Err("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is".to_string());
        }
        Ok(BitfieldEncoding::Unsigned(bits))
    }
}

fn parse_bitfield_offset(s: &str) -> Result<(i64, bool), String> {
    if s.is_empty() {
        return Err("ERR bit offset is not an integer or out of range".to_string());
    }
    let (multiplier, rest) = if let Some(stripped) = s.strip_prefix('#') {
        (true, stripped)
    } else {
        (false, s)
    };
    let offset: i64 = rest.parse().map_err(|_| {
        "ERR bit offset is not an integer or out of range".to_string()
    })?;
    if offset < 0 {
        return Err("ERR bit offset is not an integer or out of range".to_string());
    }
    Ok((offset, multiplier))
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

/// 解析 ZCOUNT/ZRANGEBYSCORE 的分数边界
/// 支持: -inf, +inf, (value (exclusive), value (inclusive)
fn parse_zrange_bound(bytes: Vec<u8>) -> Result<ZRangeBound, Command> {
    let s = parse_bulk_string(bytes)?;
    let s = s.trim();

    if s.eq_ignore_ascii_case("-inf") {
        return Ok(ZRangeBound::NegInf);
    }
    if s.eq_ignore_ascii_case("+inf") || s.eq_ignore_ascii_case("inf") {
        return Ok(ZRangeBound::PosInf);
    }

    // 检查是否是 exclusive (以 '(' 开头)
    if let Some(rest) = s.strip_prefix('(') {
        let v = rest.parse::<f64>().map_err(|_| err_not_float())?;
        if !v.is_finite() {
            return Err(err_not_float());
        }
        return Ok(ZRangeBound::Exclusive(v));
    }

    // 默认是 inclusive
    let v = s.parse::<f64>().map_err(|_| err_not_float())?;
    if !v.is_finite() {
        return Err(err_not_float());
    }
    Ok(ZRangeBound::Inclusive(v))
}

/// 解析 ZLEXCOUNT/ZRANGEBYLEX 的字典序边界
/// 支持: -, +, [value (inclusive), (value (exclusive)
fn parse_zlex_bound(bytes: Vec<u8>) -> Result<ZLexBound, Command> {
    let s = parse_bulk_string(bytes)?;
    let s = s.trim();

    if s == "-" {
        return Ok(ZLexBound::NegInf);
    }
    if s == "+" {
        return Ok(ZLexBound::PosInf);
    }

    // [value - inclusive
    if let Some(rest) = s.strip_prefix('[') {
        return Ok(ZLexBound::Inclusive(rest.to_string()));
    }

    // (value - exclusive
    if let Some(rest) = s.strip_prefix('(') {
        return Ok(ZLexBound::Exclusive(rest.to_string()));
    }

    // 无效格式
    Err(Command::Error(
        "ERR min or max not valid string range item".to_string(),
    ))
}

/// 解析 ZINTER/ZUNION 的可选参数: WEIGHTS, AGGREGATE, WITHSCORES
#[allow(clippy::type_complexity)]
fn parse_zset_options(
    iter: &mut std::vec::IntoIter<Vec<u8>>,
    numkeys: usize,
) -> Result<(Option<Vec<f64>>, Option<ZAggregate>, bool), CommandError> {
    let mut weights: Option<Vec<f64>> = None;
    let mut aggregate: Option<ZAggregate> = None;
    let mut withscores = false;

    // 收集剩余参数
    let remaining: Vec<Vec<u8>> = iter.collect();
    let mut i = 0;

    while i < remaining.len() {
        let opt = match parse_bulk_string(remaining[i].clone()) {
            Ok(o) => o.to_uppercase(),
            Err(_) => return Err(CommandError::RedisError("ERR syntax error".to_string())),
        };

        match opt.as_str() {
            "WEIGHTS" => {
                i += 1;
                let mut w = Vec::with_capacity(numkeys);
                for _ in 0..numkeys {
                    if i >= remaining.len() {
                        return Err(CommandError::RedisError("ERR syntax error".to_string()));
                    }
                    let weight = match parse_f64_from_bulk(remaining[i].clone()) {
                        Ok(v) => v,
                        Err(_) => {
                            return Err(CommandError::RedisError(
                                "ERR weight value is not a float".to_string(),
                            ))
                        }
                    };
                    w.push(weight);
                    i += 1;
                }
                weights = Some(w);
            }
            "AGGREGATE" => {
                i += 1;
                if i >= remaining.len() {
                    return Err(CommandError::RedisError("ERR syntax error".to_string()));
                }
                let agg = match parse_bulk_string(remaining[i].clone()) {
                    Ok(a) => a.to_uppercase(),
                    Err(_) => return Err(CommandError::RedisError("ERR syntax error".to_string())),
                };
                aggregate = Some(match agg.as_str() {
                    "SUM" => ZAggregate::Sum,
                    "MIN" => ZAggregate::Min,
                    "MAX" => ZAggregate::Max,
                    _ => return Err(CommandError::RedisError("ERR syntax error".to_string())),
                });
                i += 1;
            }
            "WITHSCORES" => {
                withscores = true;
                i += 1;
            }
            _ => return Err(CommandError::RedisError("ERR syntax error".to_string())),
        }
    }

    Ok((weights, aggregate, withscores))
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
        "TIME" => {
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("time")));
            }
            Command::Time
        }
        "RANDOMKEY" => {
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("randomkey")));
            }
            Command::Randomkey
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
            let key = try_cmd!(require_key(&mut iter, "get"));
            try_cmd!(ensure_no_more_args(&mut iter, "get"));
            Command::Get { key }
        }
        "GETDEL" => {
            let key = try_cmd!(require_key(&mut iter, "getdel"));
            try_cmd!(ensure_no_more_args(&mut iter, "getdel"));
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
        "SETBIT" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("setbit")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(offset_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("setbit")));
            };
            let Some(value_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("setbit")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("setbit")));
            }
            let offset = match parse_u64_from_bulk(offset_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(Some(e)),
            };
            let value_i64 = match parse_i64_from_bulk(value_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(Some(e)),
            };
            if value_i64 != 0 && value_i64 != 1 {
                return Ok(Some(Command::Error(
                    "ERR bit is not an integer or out of range".to_string(),
                )));
            }
            Command::Setbit { key, offset, value: value_i64 == 1 }
        }
        "GETBIT" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("getbit")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(offset_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("getbit")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("getbit")));
            }
            let offset = match parse_u64_from_bulk(offset_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(Some(e)),
            };
            Command::Getbit { key, offset }
        }
        "BITCOUNT" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("bitcount")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let mut start: Option<i64> = None;
            let mut end: Option<i64> = None;
            let mut use_bit = false;

            if let Some(start_bytes) = iter.next() {
                start = Some(match parse_i64_from_bulk(start_bytes) {
                    Ok(v) => v,
                    Err(e) => return Ok(Some(e)),
                });
                if let Some(end_bytes) = iter.next() {
                    end = Some(match parse_i64_from_bulk(end_bytes) {
                        Ok(v) => v,
                        Err(e) => return Ok(Some(e)),
                    });
                    // 检查是否有 BYTE/BIT 修饰符
                    if let Some(mode_bytes) = iter.next() {
                        let mode = match parse_bulk_string(mode_bytes) {
                            Ok(m) => m.to_uppercase(),
                            Err(e) => return Ok(Some(e)),
                        };
                        match mode.as_str() {
                            "BIT" => use_bit = true,
                            "BYTE" => use_bit = false,
                            _ => return Ok(Some(Command::Error(
                                "ERR syntax error".to_string(),
                            ))),
                        }
                    }
                } else {
                    // 只有 start 没有 end 是错误的
                    return Ok(Some(err_wrong_args("bitcount")));
                }
            }
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("bitcount")));
            }
            Command::Bitcount { key, start, end, use_bit }
        }
        "BITPOS" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("bitpos")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(bit_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("bitpos")));
            };
            let bit_i64 = match parse_i64_from_bulk(bit_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(Some(e)),
            };
            if bit_i64 != 0 && bit_i64 != 1 {
                return Ok(Some(Command::Error(
                    "ERR bit is not an integer or out of range".to_string(),
                )));
            }
            let bit = bit_i64 == 1;

            let mut start: Option<i64> = None;
            let mut end: Option<i64> = None;
            let mut use_bit = false;

            if let Some(start_bytes) = iter.next() {
                start = Some(match parse_i64_from_bulk(start_bytes) {
                    Ok(v) => v,
                    Err(e) => return Ok(Some(e)),
                });
                if let Some(end_bytes) = iter.next() {
                    end = Some(match parse_i64_from_bulk(end_bytes) {
                        Ok(v) => v,
                        Err(e) => return Ok(Some(e)),
                    });
                    // 检查是否有 BYTE/BIT 修饰符
                    if let Some(mode_bytes) = iter.next() {
                        let mode = match parse_bulk_string(mode_bytes) {
                            Ok(m) => m.to_uppercase(),
                            Err(e) => return Ok(Some(e)),
                        };
                        match mode.as_str() {
                            "BIT" => use_bit = true,
                            "BYTE" => use_bit = false,
                            _ => return Ok(Some(Command::Error(
                                "ERR syntax error".to_string(),
                            ))),
                        }
                    }
                }
            }
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("bitpos")));
            }
            Command::Bitpos { key, bit, start, end, use_bit }
        }
        "BITOP" => {
            let Some(op_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("bitop")));
            };
            let op = match parse_bulk_string(op_bytes) {
                Ok(o) => o.to_uppercase(),
                Err(e) => return Ok(Some(e)),
            };
            let Some(destkey_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("bitop")));
            };
            let destkey = match parse_bulk_string(destkey_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let mut keys = Vec::new();
            for key_bytes in iter {
                let key = match parse_bulk_string(key_bytes) {
                    Ok(k) => k,
                    Err(e) => return Ok(Some(e)),
                };
                keys.push(key);
            }
            if keys.is_empty() {
                return Ok(Some(err_wrong_args("bitop")));
            }
            Command::Bitop { op, destkey, keys }
        }
        "BITFIELD" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("bitfield")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };

            let mut ops = Vec::new();
            while let Some(subcmd_bytes) = iter.next() {
                let subcmd = match parse_bulk_string(subcmd_bytes) {
                    Ok(s) => s.to_uppercase(),
                    Err(e) => return Ok(Some(e)),
                };

                match subcmd.as_str() {
                    "GET" => {
                        let Some(enc_bytes) = iter.next() else {
                            return Ok(Some(Command::Error("ERR syntax error".to_string())));
                        };
                        let enc_str = match parse_bulk_string(enc_bytes) {
                            Ok(s) => s,
                            Err(e) => return Ok(Some(e)),
                        };
                        let encoding = match parse_bitfield_encoding(&enc_str) {
                            Ok(e) => e,
                            Err(msg) => return Ok(Some(Command::Error(msg))),
                        };

                        let Some(off_bytes) = iter.next() else {
                            return Ok(Some(Command::Error("ERR syntax error".to_string())));
                        };
                        let off_str = match parse_bulk_string(off_bytes) {
                            Ok(s) => s,
                            Err(e) => return Ok(Some(e)),
                        };
                        let (offset, offset_multiplier) = match parse_bitfield_offset(&off_str) {
                            Ok(o) => o,
                            Err(msg) => return Ok(Some(Command::Error(msg))),
                        };

                        ops.push(BitfieldOp::Get {
                            encoding,
                            offset,
                            offset_multiplier,
                        });
                    }
                    "SET" => {
                        let Some(enc_bytes) = iter.next() else {
                            return Ok(Some(Command::Error("ERR syntax error".to_string())));
                        };
                        let enc_str = match parse_bulk_string(enc_bytes) {
                            Ok(s) => s,
                            Err(e) => return Ok(Some(e)),
                        };
                        let encoding = match parse_bitfield_encoding(&enc_str) {
                            Ok(e) => e,
                            Err(msg) => return Ok(Some(Command::Error(msg))),
                        };

                        let Some(off_bytes) = iter.next() else {
                            return Ok(Some(Command::Error("ERR syntax error".to_string())));
                        };
                        let off_str = match parse_bulk_string(off_bytes) {
                            Ok(s) => s,
                            Err(e) => return Ok(Some(e)),
                        };
                        let (offset, offset_multiplier) = match parse_bitfield_offset(&off_str) {
                            Ok(o) => o,
                            Err(msg) => return Ok(Some(Command::Error(msg))),
                        };

                        let Some(val_bytes) = iter.next() else {
                            return Ok(Some(Command::Error("ERR syntax error".to_string())));
                        };
                        let value = match parse_i64_from_bulk(val_bytes) {
                            Ok(v) => v,
                            Err(_) => {
                                return Ok(Some(Command::Error(
                                    "ERR value is not an integer or out of range".to_string(),
                                )))
                            }
                        };

                        ops.push(BitfieldOp::Set {
                            encoding,
                            offset,
                            offset_multiplier,
                            value,
                        });
                    }
                    "INCRBY" => {
                        let Some(enc_bytes) = iter.next() else {
                            return Ok(Some(Command::Error("ERR syntax error".to_string())));
                        };
                        let enc_str = match parse_bulk_string(enc_bytes) {
                            Ok(s) => s,
                            Err(e) => return Ok(Some(e)),
                        };
                        let encoding = match parse_bitfield_encoding(&enc_str) {
                            Ok(e) => e,
                            Err(msg) => return Ok(Some(Command::Error(msg))),
                        };

                        let Some(off_bytes) = iter.next() else {
                            return Ok(Some(Command::Error("ERR syntax error".to_string())));
                        };
                        let off_str = match parse_bulk_string(off_bytes) {
                            Ok(s) => s,
                            Err(e) => return Ok(Some(e)),
                        };
                        let (offset, offset_multiplier) = match parse_bitfield_offset(&off_str) {
                            Ok(o) => o,
                            Err(msg) => return Ok(Some(Command::Error(msg))),
                        };

                        let Some(incr_bytes) = iter.next() else {
                            return Ok(Some(Command::Error("ERR syntax error".to_string())));
                        };
                        let increment = match parse_i64_from_bulk(incr_bytes) {
                            Ok(v) => v,
                            Err(_) => {
                                return Ok(Some(Command::Error(
                                    "ERR value is not an integer or out of range".to_string(),
                                )))
                            }
                        };

                        ops.push(BitfieldOp::Incrby {
                            encoding,
                            offset,
                            offset_multiplier,
                            increment,
                        });
                    }
                    "OVERFLOW" => {
                        let Some(mode_bytes) = iter.next() else {
                            return Ok(Some(Command::Error("ERR syntax error".to_string())));
                        };
                        let mode_str = match parse_bulk_string(mode_bytes) {
                            Ok(s) => s.to_uppercase(),
                            Err(e) => return Ok(Some(e)),
                        };
                        let overflow = match mode_str.as_str() {
                            "WRAP" => BitfieldOverflow::Wrap,
                            "SAT" => BitfieldOverflow::Sat,
                            "FAIL" => BitfieldOverflow::Fail,
                            _ => {
                                return Ok(Some(Command::Error(
                                    "ERR Invalid OVERFLOW type (should be one of WRAP, SAT, FAIL)"
                                        .to_string(),
                                )))
                            }
                        };
                        ops.push(BitfieldOp::Overflow(overflow));
                    }
                    _ => {
                        return Ok(Some(Command::Error(format!(
                            "ERR Unknown subcommand or wrong number of arguments for '{}'",
                            subcmd
                        ))));
                    }
                }
            }

            Command::Bitfield { key, ops }
        }
        "BITFIELD_RO" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("bitfield_ro")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };

            let mut ops = Vec::new();
            while let Some(subcmd_bytes) = iter.next() {
                let subcmd = match parse_bulk_string(subcmd_bytes) {
                    Ok(s) => s.to_uppercase(),
                    Err(e) => return Ok(Some(e)),
                };

                if subcmd != "GET" {
                    return Ok(Some(Command::Error(
                        "ERR BITFIELD_RO only supports the GET subcommand".to_string(),
                    )));
                }

                let Some(enc_bytes) = iter.next() else {
                    return Ok(Some(Command::Error("ERR syntax error".to_string())));
                };
                let enc_str = match parse_bulk_string(enc_bytes) {
                    Ok(s) => s,
                    Err(e) => return Ok(Some(e)),
                };
                let encoding = match parse_bitfield_encoding(&enc_str) {
                    Ok(e) => e,
                    Err(msg) => return Ok(Some(Command::Error(msg))),
                };

                let Some(off_bytes) = iter.next() else {
                    return Ok(Some(Command::Error("ERR syntax error".to_string())));
                };
                let off_str = match parse_bulk_string(off_bytes) {
                    Ok(s) => s,
                    Err(e) => return Ok(Some(e)),
                };
                let (offset, offset_multiplier) = match parse_bitfield_offset(&off_str) {
                    Ok(o) => o,
                    Err(msg) => return Ok(Some(Command::Error(msg))),
                };

                ops.push(BitfieldOp::Get {
                    encoding,
                    offset,
                    offset_multiplier,
                });
            }

            Command::BitfieldRo { key, ops }
        }
        "XADD" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("xadd")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };

            let Some(id_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("xadd")));
            };
            let id_str = match parse_bulk_string(id_bytes) {
                Ok(s) => s,
                Err(e) => return Ok(Some(e)),
            };
            let id = if id_str == "*" {
                None
            } else {
                match parse_stream_id_token(id_str.into_bytes()) {
                    Ok(v) => Some(v),
                    Err(e) => return Ok(Some(e)),
                }
            };

            let mut fields: Vec<(Binary, Binary)> = Vec::new();
            while let Some(field) = iter.next() {
                let Some(value) = iter.next() else {
                    return Ok(Some(err_wrong_args("xadd")));
                };
                fields.push((field, value));
            }
            if fields.is_empty() {
                return Ok(Some(err_wrong_args("xadd")));
            }
            Command::Xadd { key, id, fields }
        }
        "XLEN" => {
            let key = try_cmd!(require_key(&mut iter, "xlen"));
            try_cmd!(ensure_no_more_args(&mut iter, "xlen"));
            Command::Xlen { key }
        }
        "XRANGE" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("xrange")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(start_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("xrange")));
            };
            let start_s = match parse_bulk_string(start_bytes) {
                Ok(s) => s,
                Err(e) => return Ok(Some(e)),
            };
            let start = if start_s == "-" {
                None
            } else {
                match parse_stream_id_token(start_s.into_bytes()) {
                    Ok(v) => Some(v),
                    Err(e) => return Ok(Some(e)),
                }
            };

            let Some(end_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("xrange")));
            };
            let end_s = match parse_bulk_string(end_bytes) {
                Ok(s) => s,
                Err(e) => return Ok(Some(e)),
            };
            let end = if end_s == "+" {
                None
            } else {
                match parse_stream_id_token(end_s.into_bytes()) {
                    Ok(v) => Some(v),
                    Err(e) => return Ok(Some(e)),
                }
            };

            let mut count: Option<usize> = None;
            while let Some(opt) = iter.next() {
                let opt_upper = match parse_bulk_string(opt) {
                    Ok(s) => s.to_uppercase(),
                    Err(e) => return Ok(Some(e)),
                };
                match opt_upper.as_str() {
                    "COUNT" => {
                        let Some(n_bytes) = iter.next() else {
                            return Ok(Some(err_wrong_args("xrange")));
                        };
                        let n = match parse_i64_from_bulk(n_bytes) {
                            Ok(v) => v,
                            Err(e) => return Ok(Some(e)),
                        };
                        if n < 0 {
                            return Ok(Some(err_not_integer()));
                        }
                        count = Some(n as usize);
                    }
                    _ => {
                        return Ok(Some(err_syntax()));
                    }
                }
            }

            Command::Xrange {
                key,
                start,
                end,
                count,
            }
        }
        "XREAD" => {
            let mut count: Option<usize> = None;
            let mut block_millis: Option<u64> = None;
            let mut found_streams = false;

            while let Some(opt) = iter.next() {
                let opt_upper = match parse_bulk_string(opt) {
                    Ok(s) => s.to_uppercase(),
                    Err(e) => return Ok(Some(e)),
                };
                match opt_upper.as_str() {
                    "COUNT" => {
                        let Some(n_bytes) = iter.next() else {
                            return Ok(Some(err_wrong_args("xread")));
                        };
                        let n = match parse_i64_from_bulk(n_bytes) {
                            Ok(v) => v,
                            Err(e) => return Ok(Some(e)),
                        };
                        if n < 0 {
                            return Ok(Some(err_not_integer()));
                        }
                        count = Some(n as usize);
                    }
                    "BLOCK" => {
                        let Some(ms_bytes) = iter.next() else {
                            return Ok(Some(err_wrong_args("xread")));
                        };
                        let ms = match parse_i64_from_bulk(ms_bytes) {
                            Ok(v) => v,
                            Err(e) => return Ok(Some(e)),
                        };
                        if ms < 0 {
                            return Ok(Some(err_not_integer()));
                        }
                        block_millis = Some(ms as u64);
                    }
                    "STREAMS" => {
                        found_streams = true;
                        break;
                    }
                    _ => {
                        return Ok(Some(err_syntax()));
                    }
                }
            }

            if !found_streams {
                return Ok(Some(err_wrong_args("xread")));
            }

            let mut rest: Vec<Vec<u8>> = iter.collect();
            if rest.len() < 2 || !rest.len().is_multiple_of(2) {
                return Ok(Some(err_wrong_args("xread")));
            }
            let n = rest.len() / 2;
            let ids = rest.split_off(n);

            let mut streams: Vec<(String, StreamReadId)> = Vec::with_capacity(n);
            for (k_bytes, id_bytes) in rest.into_iter().zip(ids.into_iter()) {
                let key = match parse_bulk_string(k_bytes) {
                    Ok(k) => k,
                    Err(e) => return Ok(Some(e)),
                };
                let id = match parse_stream_read_id_token(id_bytes) {
                    Ok(v) => v,
                    Err(e) => return Ok(Some(e)),
                };
                streams.push((key, id));
            }

            Command::Xread {
                count,
                block_millis,
                streams,
            }
        }
        "XINFO" => {
            let Some(sub_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("xinfo")));
            };
            let sub = match parse_bulk_string(sub_bytes) {
                Ok(s) => s.to_uppercase(),
                Err(e) => return Ok(Some(e)),
            };
            match sub.as_str() {
                "STREAM" => {
                    let key = try_cmd!(require_key(&mut iter, "xinfo"));
                    try_cmd!(ensure_no_more_args(&mut iter, "xinfo"));
                    Command::XinfoStream { key }
                }
                _ => return Ok(Some(err_syntax())),
            }
        }
        "XGROUP" => {
            let Some(sub_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("xgroup")));
            };
            let sub = match parse_bulk_string(sub_bytes) {
                Ok(s) => s.to_uppercase(),
                Err(e) => return Ok(Some(e)),
            };
            match sub.as_str() {
                "CREATE" => {
                    let key = try_cmd!(require_key(&mut iter, "xgroup"));
                    let group = try_cmd!(require_key(&mut iter, "xgroup"));
                    let Some(id_bytes) = iter.next() else {
                        return Ok(Some(err_wrong_args("xgroup")));
                    };
                    let id_str = match parse_bulk_string(id_bytes) {
                        Ok(s) => s,
                        Err(e) => return Ok(Some(e)),
                    };
                    let id = if id_str == "$" {
                        StreamId { ms: u64::MAX, seq: u64::MAX } // Special marker for "last ID"
                    } else {
                        match parse_stream_id(&id_str) {
                            Ok(sid) => sid,
                            Err(e) => return Ok(Some(e)),
                        }
                    };
                    let mut mkstream = false;
                    while let Some(opt_bytes) = iter.next() {
                        let opt = match parse_bulk_string(opt_bytes) {
                            Ok(s) => s.to_uppercase(),
                            Err(e) => return Ok(Some(e)),
                        };
                        if opt == "MKSTREAM" {
                            mkstream = true;
                        } else if opt == "ENTRIESREAD" {
                            // Skip the entries_read value (not implemented)
                            let _ = iter.next();
                        } else {
                            return Ok(Some(err_syntax()));
                        }
                    }
                    Command::XgroupCreate { key, group, id, mkstream }
                }
                "SETID" => {
                    let key = try_cmd!(require_key(&mut iter, "xgroup"));
                    let group = try_cmd!(require_key(&mut iter, "xgroup"));
                    let Some(id_bytes) = iter.next() else {
                        return Ok(Some(err_wrong_args("xgroup")));
                    };
                    let id_str = match parse_bulk_string(id_bytes) {
                        Ok(s) => s,
                        Err(e) => return Ok(Some(e)),
                    };
                    let id = if id_str == "$" {
                        StreamId { ms: u64::MAX, seq: u64::MAX }
                    } else {
                        match parse_stream_id(&id_str) {
                            Ok(sid) => sid,
                            Err(e) => return Ok(Some(e)),
                        }
                    };
                    try_cmd!(ensure_no_more_args(&mut iter, "xgroup"));
                    Command::XgroupSetid { key, group, id }
                }
                "DESTROY" => {
                    let key = try_cmd!(require_key(&mut iter, "xgroup"));
                    let group = try_cmd!(require_key(&mut iter, "xgroup"));
                    try_cmd!(ensure_no_more_args(&mut iter, "xgroup"));
                    Command::XgroupDestroy { key, group }
                }
                "CREATECONSUMER" => {
                    let key = try_cmd!(require_key(&mut iter, "xgroup"));
                    let group = try_cmd!(require_key(&mut iter, "xgroup"));
                    let consumer = try_cmd!(require_key(&mut iter, "xgroup"));
                    try_cmd!(ensure_no_more_args(&mut iter, "xgroup"));
                    Command::XgroupCreateconsumer { key, group, consumer }
                }
                "DELCONSUMER" => {
                    let key = try_cmd!(require_key(&mut iter, "xgroup"));
                    let group = try_cmd!(require_key(&mut iter, "xgroup"));
                    let consumer = try_cmd!(require_key(&mut iter, "xgroup"));
                    try_cmd!(ensure_no_more_args(&mut iter, "xgroup"));
                    Command::XgroupDelconsumer { key, group, consumer }
                }
                _ => return Ok(Some(err_syntax())),
            }
        }
        "XREADGROUP" => {
            // XREADGROUP GROUP group consumer [COUNT count] [BLOCK milliseconds] [NOACK] STREAMS key [key ...] id [id ...]
            let Some(group_kw_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("xreadgroup")));
            };
            let group_kw = match parse_bulk_string(group_kw_bytes) {
                Ok(s) => s.to_uppercase(),
                Err(e) => return Ok(Some(e)),
            };
            if group_kw != "GROUP" {
                return Ok(Some(err_syntax()));
            }

            let group = try_cmd!(require_key(&mut iter, "xreadgroup"));
            let consumer = try_cmd!(require_key(&mut iter, "xreadgroup"));

            let mut count: Option<usize> = None;
            let mut block_millis: Option<u64> = None;
            let mut noack = false;
            let mut keys: Vec<String> = Vec::new();

            // Parse options until STREAMS keyword
            while let Some(opt_bytes) = iter.next() {
                let opt = match parse_bulk_string(opt_bytes.clone()) {
                    Ok(s) => s.to_uppercase(),
                    Err(e) => return Ok(Some(e)),
                };
                match opt.as_str() {
                    "COUNT" => {
                        let Some(count_bytes) = iter.next() else {
                            return Ok(Some(err_wrong_args("xreadgroup")));
                        };
                        count = Some(match parse_u64_from_bulk(count_bytes) {
                            Ok(c) => c as usize,
                            Err(e) => return Ok(Some(e)),
                        });
                    }
                    "BLOCK" => {
                        let Some(block_bytes) = iter.next() else {
                            return Ok(Some(err_wrong_args("xreadgroup")));
                        };
                        block_millis = Some(match parse_u64_from_bulk(block_bytes) {
                            Ok(b) => b,
                            Err(e) => return Ok(Some(e)),
                        });
                    }
                    "NOACK" => {
                        noack = true;
                    }
                    "STREAMS" => {
                        // Collect remaining tokens as keys and IDs
                        let remaining: Vec<Vec<u8>> = iter.collect();
                        if remaining.is_empty() || !remaining.len().is_multiple_of(2) {
                            return Ok(Some(err_wrong_args("xreadgroup")));
                        }
                        let half = remaining.len() / 2;
                        for item in remaining.iter().take(half) {
                            let key = match parse_bulk_string(item.clone()) {
                                Ok(k) => k,
                                Err(e) => return Ok(Some(e)),
                            };
                            keys.push(key);
                        }
                        let mut streams: Vec<(String, Option<StreamId>)> = Vec::new();
                        for (i, item) in remaining.iter().skip(half).enumerate() {
                            let id_str = match parse_bulk_string(item.clone()) {
                                Ok(s) => s,
                                Err(e) => return Ok(Some(e)),
                            };
                            let id = if id_str == ">" {
                                None // Special marker for "new messages"
                            } else {
                                Some(match parse_stream_id(&id_str) {
                                    Ok(sid) => sid,
                                    Err(e) => return Ok(Some(e)),
                                })
                            };
                            streams.push((keys[i].clone(), id));
                        }
                        return Ok(Some(Command::Xreadgroup {
                            group,
                            consumer,
                            count,
                            block_millis,
                            noack,
                            streams,
                        }));
                    }
                    _ => return Ok(Some(err_syntax())),
                }
            }

            // No STREAMS keyword found
            return Ok(Some(err_wrong_args("xreadgroup")));
        }
        "XACK" => {
            // XACK key group id [id ...]
            let key = try_cmd!(require_key(&mut iter, "xack"));
            let group = try_cmd!(require_key(&mut iter, "xack"));
            let mut ids: Vec<StreamId> = Vec::new();
            for id_bytes in iter.by_ref() {
                let id_str = match parse_bulk_string(id_bytes) {
                    Ok(s) => s,
                    Err(e) => return Ok(Some(e)),
                };
                let id = match parse_stream_id(&id_str) {
                    Ok(sid) => sid,
                    Err(e) => return Ok(Some(e)),
                };
                ids.push(id);
            }
            if ids.is_empty() {
                return Ok(Some(err_wrong_args("xack")));
            }
            Command::Xack { key, group, ids }
        }
        "XPENDING" => {
            // XPENDING key group [start end count] [consumer] - simplified version
            let key = try_cmd!(require_key(&mut iter, "xpending"));
            let group = try_cmd!(require_key(&mut iter, "xpending"));
            // For now, only support the summary form (no range/consumer filtering)
            try_cmd!(ensure_no_more_args(&mut iter, "xpending"));
            Command::Xpending { key, group }
        }
        "GEOADD" => {
            let key = try_cmd!(require_key(&mut iter, "geoadd"));
            let mut members: Vec<(f64, f64, String)> = Vec::new();
            while let Some(lon_bytes) = iter.next() {
                let lon = match parse_f64_from_bulk(lon_bytes) {
                    Ok(v) => v,
                    Err(e) => return Ok(Some(e)),
                };
                let Some(lat_bytes) = iter.next() else {
                    return Ok(Some(err_wrong_args("geoadd")));
                };
                let lat = match parse_f64_from_bulk(lat_bytes) {
                    Ok(v) => v,
                    Err(e) => return Ok(Some(e)),
                };
                let Some(member_bytes) = iter.next() else {
                    return Ok(Some(err_wrong_args("geoadd")));
                };
                let member = match parse_bulk_string(member_bytes) {
                    Ok(m) => m,
                    Err(e) => return Ok(Some(e)),
                };
                members.push((lon, lat, member));
            }
            if members.is_empty() {
                return Ok(Some(err_wrong_args("geoadd")));
            }
            Command::Geoadd { key, members }
        }
        "GEOPOS" => {
            let key = try_cmd!(require_key(&mut iter, "geopos"));
            let mut members: Vec<String> = Vec::new();
            for member_bytes in iter.by_ref() {
                let member = match parse_bulk_string(member_bytes) {
                    Ok(m) => m,
                    Err(e) => return Ok(Some(e)),
                };
                members.push(member);
            }
            if members.is_empty() {
                return Ok(Some(err_wrong_args("geopos")));
            }
            Command::Geopos { key, members }
        }
        "GEODIST" => {
            let key = try_cmd!(require_key(&mut iter, "geodist"));
            let member1 = try_cmd!(require_key(&mut iter, "geodist"));
            let member2 = try_cmd!(require_key(&mut iter, "geodist"));
            let mut unit = GeoUnit::Meters;
            if let Some(unit_bytes) = iter.next() {
                let unit_str = match parse_bulk_string(unit_bytes) {
                    Ok(s) => s.to_uppercase(),
                    Err(e) => return Ok(Some(e)),
                };
                unit = match unit_str.as_str() {
                    "M" => GeoUnit::Meters,
                    "KM" => GeoUnit::Kilometers,
                    "MI" => GeoUnit::Miles,
                    "FT" => GeoUnit::Feet,
                    _ => return Ok(Some(err_syntax())),
                };
            }
            try_cmd!(ensure_no_more_args(&mut iter, "geodist"));
            Command::Geodist { key, member1, member2, unit }
        }
        "GEOHASH" => {
            let key = try_cmd!(require_key(&mut iter, "geohash"));
            let mut members: Vec<String> = Vec::new();
            for member_bytes in iter.by_ref() {
                let member = match parse_bulk_string(member_bytes) {
                    Ok(m) => m,
                    Err(e) => return Ok(Some(e)),
                };
                members.push(member);
            }
            if members.is_empty() {
                return Ok(Some(err_wrong_args("geohash")));
            }
            Command::Geohash { key, members }
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
            let key = try_cmd!(require_key(&mut iter, "strlen"));
            try_cmd!(ensure_no_more_args(&mut iter, "strlen"));
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
            let key = try_cmd!(require_key(&mut iter, "incr"));
            try_cmd!(ensure_no_more_args(&mut iter, "incr"));
            Command::Incr { key }
        }
        "DECR" => {
            let key = try_cmd!(require_key(&mut iter, "decr"));
            try_cmd!(ensure_no_more_args(&mut iter, "decr"));
            Command::Decr { key }
        }
        "INCRBY" => {
            let key = try_cmd!(require_key(&mut iter, "incrby"));
            let delta = try_cmd!(require_i64(&mut iter, "incrby"));
            try_cmd!(ensure_no_more_args(&mut iter, "incrby"));
            Command::Incrby { key, delta }
        }
        "INCRBYFLOAT" => {
            let key = try_cmd!(require_key(&mut iter, "incrbyfloat"));
            let delta = try_cmd!(require_f64(&mut iter, "incrbyfloat"));
            try_cmd!(ensure_no_more_args(&mut iter, "incrbyfloat"));
            Command::Incrbyfloat { key, delta }
        }
        "DECRBY" => {
            let key = try_cmd!(require_key(&mut iter, "decrby"));
            let delta = try_cmd!(require_i64(&mut iter, "decrby"));
            try_cmd!(ensure_no_more_args(&mut iter, "decrby"));
            Command::Decrby { key, delta }
        }
        "DEL" => {
            let keys = try_cmd!(collect_keys(iter));
            if keys.is_empty() {
                return Ok(Some(err_wrong_args("del")));
            }
            Command::Del { keys }
        }
        "UNLINK" => {
            let keys = try_cmd!(collect_keys(iter));
            if keys.is_empty() {
                return Ok(Some(err_wrong_args("unlink")));
            }
            Command::Unlink { keys }
        }
        "COPY" => {
            let Some(src_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("copy")));
            };
            let source = match parse_bulk_string(src_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(dst_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("copy")));
            };
            let destination = match parse_bulk_string(dst_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let mut replace = false;
            for opt_bytes in iter {
                let opt = match parse_bulk_string(opt_bytes) {
                    Ok(o) => o.to_uppercase(),
                    Err(e) => return Ok(Some(e)),
                };
                if opt == "REPLACE" {
                    replace = true;
                } else if opt == "DB" {
                    // 跳过 DB 参数（不支持跨 DB 复制）
                    return Ok(Some(Command::Error(
                        "ERR COPY with DB option is not supported".to_string(),
                    )));
                } else {
                    return Ok(Some(err_wrong_args("copy")));
                }
            }
            Command::Copy {
                source,
                destination,
                replace,
            }
        }
        "TOUCH" => {
            let mut keys = Vec::new();
            for b in iter {
                match parse_bulk_string(b) {
                    Ok(k) => keys.push(k),
                    Err(e) => return Ok(Some(e)),
                }
            }
            if keys.is_empty() {
                return Ok(Some(err_wrong_args("touch")));
            }
            Command::Touch { keys }
        }
        "OBJECT" => {
            let Some(subcmd_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("object")));
            };
            let subcmd = match parse_bulk_string(subcmd_bytes) {
                Ok(s) => s.to_uppercase(),
                Err(e) => return Ok(Some(e)),
            };
            match subcmd.as_str() {
                "ENCODING" => {
                    let Some(key_bytes) = iter.next() else {
                        return Ok(Some(err_wrong_args("object")));
                    };
                    let key = match parse_bulk_string(key_bytes) {
                        Ok(k) => k,
                        Err(e) => return Ok(Some(e)),
                    };
                    if iter.next().is_some() {
                        return Ok(Some(err_wrong_args("object")));
                    }
                    Command::ObjectEncoding { key }
                }
                _ => Command::Error(format!(
                    "ERR Unknown subcommand or wrong number of arguments for '{}'",
                    subcmd
                )),
            }
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
            let cursor = match parse_u64_from_bulk(cursor_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(Some(e)),
            };
            let mut pattern: Option<String> = None;
            let mut count: Option<u64> = None;
            let mut type_filter: Option<String> = None;

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
                    "TYPE" => {
                        if type_filter.is_some() {
                            return Ok(Some(err_syntax()));
                        }
                        let Some(type_bytes) = iter.next() else {
                            return Ok(Some(err_syntax()));
                        };
                        let t = match parse_bulk_string(type_bytes) {
                            Ok(t) => t.to_ascii_lowercase(),
                            Err(e) => return Ok(Some(e)),
                        };
                        type_filter = Some(t);
                    }
                    _ => {
                        return Ok(Some(err_syntax()));
                    }
                }
            }

            Command::Scan {
                cursor,
                pattern,
                count,
                type_filter,
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
            let mut novalues = false;

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
                    "NOVALUES" => {
                        novalues = true;
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
                novalues,
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
            let mut novalues = false;

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
                    "NOVALUES" => {
                        novalues = true;
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
                novalues,
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
        "ZMSCORE" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("zmscore")));
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
                return Ok(Some(err_wrong_args("zmscore")));
            }

            Command::Zmscore { key, members }
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
        "ZCOUNT" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("zcount")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(min_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("zcount")));
            };
            let min = match parse_zrange_bound(min_bytes) {
                Ok(b) => b,
                Err(e) => return Ok(Some(e)),
            };
            let Some(max_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("zcount")));
            };
            let max = match parse_zrange_bound(max_bytes) {
                Ok(b) => b,
                Err(e) => return Ok(Some(e)),
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("zcount")));
            }
            Command::Zcount { key, min, max }
        }
        "ZRANK" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("zrank")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(member_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("zrank")));
            };
            let member = match parse_bulk_string(member_bytes) {
                Ok(m) => m,
                Err(e) => return Ok(Some(e)),
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("zrank")));
            }
            Command::Zrank { key, member }
        }
        "ZREVRANK" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("zrevrank")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(member_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("zrevrank")));
            };
            let member = match parse_bulk_string(member_bytes) {
                Ok(m) => m,
                Err(e) => return Ok(Some(e)),
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("zrevrank")));
            }
            Command::Zrevrank { key, member }
        }
        "ZPOPMIN" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("zpopmin")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let count = if let Some(count_bytes) = iter.next() {
                Some(match parse_isize_from_bulk(count_bytes) {
                    Ok(v) if v > 0 => v as usize,
                    Ok(_) => {
                        return Ok(Some(Command::Error(
                            "ERR value is out of range, must be positive".to_string(),
                        )))
                    }
                    Err(e) => return Ok(Some(e)),
                })
            } else {
                None
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("zpopmin")));
            }
            Command::Zpopmin { key, count }
        }
        "ZPOPMAX" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("zpopmax")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let count = if let Some(count_bytes) = iter.next() {
                Some(match parse_isize_from_bulk(count_bytes) {
                    Ok(v) if v > 0 => v as usize,
                    Ok(_) => {
                        return Ok(Some(Command::Error(
                            "ERR value is out of range, must be positive".to_string(),
                        )))
                    }
                    Err(e) => return Ok(Some(e)),
                })
            } else {
                None
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("zpopmax")));
            }
            Command::Zpopmax { key, count }
        }
        "ZINTER" => {
            // ZINTER numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
            let Some(numkeys_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("zinter")));
            };
            let numkeys = match parse_isize_from_bulk(numkeys_bytes) {
                Ok(n) if n > 0 => n as usize,
                _ => return Ok(Some(err_wrong_args("zinter"))),
            };
            let mut keys = Vec::with_capacity(numkeys);
            for _ in 0..numkeys {
                let Some(key_bytes) = iter.next() else {
                    return Ok(Some(err_wrong_args("zinter")));
                };
                keys.push(match parse_bulk_string(key_bytes) {
                    Ok(k) => k,
                    Err(e) => return Ok(Some(e)),
                });
            }
            let (weights, aggregate, withscores) = parse_zset_options(&mut iter, numkeys)?;
            Command::Zinter {
                keys,
                weights,
                aggregate,
                withscores,
            }
        }
        "ZUNION" => {
            let Some(numkeys_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("zunion")));
            };
            let numkeys = match parse_isize_from_bulk(numkeys_bytes) {
                Ok(n) if n > 0 => n as usize,
                _ => return Ok(Some(err_wrong_args("zunion"))),
            };
            let mut keys = Vec::with_capacity(numkeys);
            for _ in 0..numkeys {
                let Some(key_bytes) = iter.next() else {
                    return Ok(Some(err_wrong_args("zunion")));
                };
                keys.push(match parse_bulk_string(key_bytes) {
                    Ok(k) => k,
                    Err(e) => return Ok(Some(e)),
                });
            }
            let (weights, aggregate, withscores) = parse_zset_options(&mut iter, numkeys)?;
            Command::Zunion {
                keys,
                weights,
                aggregate,
                withscores,
            }
        }
        "ZDIFF" => {
            let Some(numkeys_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("zdiff")));
            };
            let numkeys = match parse_isize_from_bulk(numkeys_bytes) {
                Ok(n) if n > 0 => n as usize,
                _ => return Ok(Some(err_wrong_args("zdiff"))),
            };
            let mut keys = Vec::with_capacity(numkeys);
            for _ in 0..numkeys {
                let Some(key_bytes) = iter.next() else {
                    return Ok(Some(err_wrong_args("zdiff")));
                };
                keys.push(match parse_bulk_string(key_bytes) {
                    Ok(k) => k,
                    Err(e) => return Ok(Some(e)),
                });
            }
            let mut withscores = false;
            while let Some(opt_bytes) = iter.next() {
                let opt = match parse_bulk_string(opt_bytes) {
                    Ok(o) => o.to_uppercase(),
                    Err(e) => return Ok(Some(e)),
                };
                if opt == "WITHSCORES" {
                    withscores = true;
                } else {
                    return Ok(Some(err_syntax()));
                }
            }
            Command::Zdiff { keys, withscores }
        }
        "ZINTERSTORE" => {
            let Some(dest_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("zinterstore")));
            };
            let destination = match parse_bulk_string(dest_bytes) {
                Ok(d) => d,
                Err(e) => return Ok(Some(e)),
            };
            let Some(numkeys_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("zinterstore")));
            };
            let numkeys = match parse_isize_from_bulk(numkeys_bytes) {
                Ok(n) if n > 0 => n as usize,
                _ => return Ok(Some(err_wrong_args("zinterstore"))),
            };
            let mut keys = Vec::with_capacity(numkeys);
            for _ in 0..numkeys {
                let Some(key_bytes) = iter.next() else {
                    return Ok(Some(err_wrong_args("zinterstore")));
                };
                keys.push(match parse_bulk_string(key_bytes) {
                    Ok(k) => k,
                    Err(e) => return Ok(Some(e)),
                });
            }
            let (weights, aggregate, _) = parse_zset_options(&mut iter, numkeys)?;
            Command::Zinterstore {
                destination,
                keys,
                weights,
                aggregate,
            }
        }
        "ZUNIONSTORE" => {
            let Some(dest_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("zunionstore")));
            };
            let destination = match parse_bulk_string(dest_bytes) {
                Ok(d) => d,
                Err(e) => return Ok(Some(e)),
            };
            let Some(numkeys_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("zunionstore")));
            };
            let numkeys = match parse_isize_from_bulk(numkeys_bytes) {
                Ok(n) if n > 0 => n as usize,
                _ => return Ok(Some(err_wrong_args("zunionstore"))),
            };
            let mut keys = Vec::with_capacity(numkeys);
            for _ in 0..numkeys {
                let Some(key_bytes) = iter.next() else {
                    return Ok(Some(err_wrong_args("zunionstore")));
                };
                keys.push(match parse_bulk_string(key_bytes) {
                    Ok(k) => k,
                    Err(e) => return Ok(Some(e)),
                });
            }
            let (weights, aggregate, _) = parse_zset_options(&mut iter, numkeys)?;
            Command::Zunionstore {
                destination,
                keys,
                weights,
                aggregate,
            }
        }
        "ZDIFFSTORE" => {
            let Some(dest_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("zdiffstore")));
            };
            let destination = match parse_bulk_string(dest_bytes) {
                Ok(d) => d,
                Err(e) => return Ok(Some(e)),
            };
            let Some(numkeys_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("zdiffstore")));
            };
            let numkeys = match parse_isize_from_bulk(numkeys_bytes) {
                Ok(n) if n > 0 => n as usize,
                _ => return Ok(Some(err_wrong_args("zdiffstore"))),
            };
            let mut keys = Vec::with_capacity(numkeys);
            for _ in 0..numkeys {
                let Some(key_bytes) = iter.next() else {
                    return Ok(Some(err_wrong_args("zdiffstore")));
                };
                keys.push(match parse_bulk_string(key_bytes) {
                    Ok(k) => k,
                    Err(e) => return Ok(Some(e)),
                });
            }
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("zdiffstore")));
            }
            Command::Zdiffstore { destination, keys }
        }
        "ZLEXCOUNT" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("zlexcount")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(min_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("zlexcount")));
            };
            let min = match parse_zlex_bound(min_bytes) {
                Ok(b) => b,
                Err(e) => return Ok(Some(e)),
            };
            let Some(max_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("zlexcount")));
            };
            let max = match parse_zlex_bound(max_bytes) {
                Ok(b) => b,
                Err(e) => return Ok(Some(e)),
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("zlexcount")));
            }
            Command::Zlexcount { key, min, max }
        }
        "PFADD" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("pfadd")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };

            let mut elements: Vec<Binary> = Vec::new();
            for elem_bytes in iter {
                // elem_bytes 已经是 &Vec<u8>，直接克隆即可
                elements.push(elem_bytes.clone());
            }

            if elements.is_empty() {
                return Ok(Some(err_wrong_args("pfadd")));
            }

            Command::Pfadd { key, elements }
        }
        "PFCOUNT" => {
            let mut keys: Vec<String> = Vec::new();
            for key_bytes in iter {
                let k = match parse_bulk_string(key_bytes) {
                    Ok(v) => v,
                    Err(e) => return Ok(Some(e)),
                };
                keys.push(k);
            }

            if keys.is_empty() {
                return Ok(Some(err_wrong_args("pfcount")));
            }

            Command::Pfcount { keys }
        }
        "PFMERGE" => {
            let Some(destkey_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("pfmerge")));
            };
            let destkey = match parse_bulk_string(destkey_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };

            let mut sourcekeys: Vec<String> = Vec::new();
            for key_bytes in iter {
                let k = match parse_bulk_string(key_bytes) {
                    Ok(v) => v,
                    Err(e) => return Ok(Some(e)),
                };
                sourcekeys.push(k);
            }

            if sourcekeys.is_empty() {
                return Ok(Some(err_wrong_args("pfmerge")));
            }

            Command::Pfmerge {
                destkey,
                sourcekeys,
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
        "LSET" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("lset")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(index_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("lset")));
            };
            let index = match parse_isize_from_bulk(index_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(Some(e)),
            };
            let Some(value_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("lset")));
            };
            let value = match parse_bulk_string(value_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(Some(e)),
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("lset")));
            }
            Command::Lset { key, index, value }
        }
        "LINSERT" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("linsert")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(pos_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("linsert")));
            };
            let pos = match parse_bulk_string(pos_bytes) {
                Ok(p) => p.to_uppercase(),
                Err(e) => return Ok(Some(e)),
            };
            let before = match pos.as_str() {
                "BEFORE" => true,
                "AFTER" => false,
                _ => return Ok(Some(err_syntax())),
            };
            let Some(pivot_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("linsert")));
            };
            let pivot = match parse_bulk_string(pivot_bytes) {
                Ok(p) => p,
                Err(e) => return Ok(Some(e)),
            };
            let Some(value_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("linsert")));
            };
            let value = match parse_bulk_string(value_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(Some(e)),
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("linsert")));
            }
            Command::Linsert {
                key,
                before,
                pivot,
                value,
            }
        }
        "RPOPLPUSH" => {
            let Some(src_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("rpoplpush")));
            };
            let source = match parse_bulk_string(src_bytes) {
                Ok(s) => s,
                Err(e) => return Ok(Some(e)),
            };
            let Some(dst_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("rpoplpush")));
            };
            let destination = match parse_bulk_string(dst_bytes) {
                Ok(d) => d,
                Err(e) => return Ok(Some(e)),
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("rpoplpush")));
            }
            Command::Rpoplpush {
                source,
                destination,
            }
        }
        "BLPOP" => {
            // BLPOP key [key ...] timeout
            let mut keys = Vec::new();
            let mut last_bytes: Option<Vec<u8>> = None;
            for b in iter.by_ref() {
                if let Some(prev) = last_bytes.take() {
                    match parse_bulk_string(prev) {
                        Ok(k) => keys.push(k),
                        Err(e) => return Ok(Some(e)),
                    }
                }
                last_bytes = Some(b);
            }
            let Some(timeout_bytes) = last_bytes else {
                return Ok(Some(err_wrong_args("blpop")));
            };
            if keys.is_empty() {
                return Ok(Some(err_wrong_args("blpop")));
            }
            let timeout = match parse_bulk_string(timeout_bytes.clone()) {
                Ok(s) => match s.parse::<f64>() {
                    Ok(t) if t >= 0.0 => t,
                    _ => {
                        return Ok(Some(Command::Error(
                            "ERR timeout is not a float or out of range".to_string(),
                        )))
                    }
                },
                Err(e) => return Ok(Some(e)),
            };
            Command::Blpop { keys, timeout }
        }
        "BRPOP" => {
            // BRPOP key [key ...] timeout
            let mut keys = Vec::new();
            let mut last_bytes: Option<Vec<u8>> = None;
            for b in iter.by_ref() {
                if let Some(prev) = last_bytes.take() {
                    match parse_bulk_string(prev) {
                        Ok(k) => keys.push(k),
                        Err(e) => return Ok(Some(e)),
                    }
                }
                last_bytes = Some(b);
            }
            let Some(timeout_bytes) = last_bytes else {
                return Ok(Some(err_wrong_args("brpop")));
            };
            if keys.is_empty() {
                return Ok(Some(err_wrong_args("brpop")));
            }
            let timeout = match parse_bulk_string(timeout_bytes.clone()) {
                Ok(s) => match s.parse::<f64>() {
                    Ok(t) if t >= 0.0 => t,
                    _ => {
                        return Ok(Some(Command::Error(
                            "ERR timeout is not a float or out of range".to_string(),
                        )))
                    }
                },
                Err(e) => return Ok(Some(e)),
            };
            Command::Brpop { keys, timeout }
        }
        "LPOS" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("lpos")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(elem_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("lpos")));
            };
            let element = match parse_bulk_string(elem_bytes) {
                Ok(e) => e,
                Err(e) => return Ok(Some(e)),
            };
            let mut rank: Option<isize> = None;
            let mut count: Option<isize> = None;
            let mut maxlen: Option<isize> = None;
            while let Some(opt_bytes) = iter.next() {
                let opt = match parse_bulk_string(opt_bytes) {
                    Ok(o) => o.to_uppercase(),
                    Err(e) => return Ok(Some(e)),
                };
                match opt.as_str() {
                    "RANK" => {
                        let Some(val_bytes) = iter.next() else {
                            return Ok(Some(err_wrong_args("lpos")));
                        };
                        rank = Some(match parse_isize_from_bulk(val_bytes) {
                            Ok(v) => v,
                            Err(e) => return Ok(Some(e)),
                        });
                    }
                    "COUNT" => {
                        let Some(val_bytes) = iter.next() else {
                            return Ok(Some(err_wrong_args("lpos")));
                        };
                        count = Some(match parse_isize_from_bulk(val_bytes) {
                            Ok(v) => v,
                            Err(e) => return Ok(Some(e)),
                        });
                    }
                    "MAXLEN" => {
                        let Some(val_bytes) = iter.next() else {
                            return Ok(Some(err_wrong_args("lpos")));
                        };
                        maxlen = Some(match parse_isize_from_bulk(val_bytes) {
                            Ok(v) => v,
                            Err(e) => return Ok(Some(e)),
                        });
                    }
                    _ => return Ok(Some(err_syntax())),
                }
            }
            Command::Lpos {
                key,
                element,
                rank,
                count,
                maxlen,
            }
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
        "SMOVE" => {
            let Some(src_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("smove")));
            };
            let source = match parse_bulk_string(src_bytes) {
                Ok(s) => s,
                Err(e) => return Ok(Some(e)),
            };
            let Some(dst_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("smove")));
            };
            let destination = match parse_bulk_string(dst_bytes) {
                Ok(d) => d,
                Err(e) => return Ok(Some(e)),
            };
            let Some(member_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("smove")));
            };
            let member = match parse_bulk_string(member_bytes) {
                Ok(m) => m,
                Err(e) => return Ok(Some(e)),
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("smove")));
            }
            Command::Smove {
                source,
                destination,
                member,
            }
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
        "HSETNX" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("hsetnx")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(field_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("hsetnx")));
            };
            let field = match parse_bulk_string(field_bytes) {
                Ok(f) => f,
                Err(e) => return Ok(Some(e)),
            };
            let Some(value_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("hsetnx")));
            };
            let value = match parse_bulk_string(value_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(Some(e)),
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("hsetnx")));
            }
            Command::Hsetnx { key, field, value }
        }
        "HSTRLEN" => {
            let key = try_cmd!(require_key(&mut iter, "hstrlen"));
            let field = try_cmd!(require_key(&mut iter, "hstrlen"));
            try_cmd!(ensure_no_more_args(&mut iter, "hstrlen"));
            Command::Hstrlen { key, field }
        }
        "HMSET" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("hmset")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let mut field_values = Vec::new();
            while let Some(field_bytes) = iter.next() {
                let field = match parse_bulk_string(field_bytes) {
                    Ok(f) => f,
                    Err(e) => return Ok(Some(e)),
                };
                let Some(value_bytes) = iter.next() else {
                    return Ok(Some(err_wrong_args("hmset")));
                };
                let value = match parse_bulk_string(value_bytes) {
                    Ok(v) => v,
                    Err(e) => return Ok(Some(e)),
                };
                field_values.push((field, value));
            }
            if field_values.is_empty() {
                return Ok(Some(err_wrong_args("hmset")));
            }
            Command::Hmset { key, field_values }
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
        "EXPIREAT" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("expireat")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(ts_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("expireat")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("expireat")));
            }
            let timestamp = match parse_i64_from_bulk(ts_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(Some(e)),
            };
            Command::Expireat { key, timestamp }
        }
        "PEXPIREAT" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("pexpireat")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            let Some(ts_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("pexpireat")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("pexpireat")));
            }
            let timestamp_ms = match parse_i64_from_bulk(ts_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(Some(e)),
            };
            Command::Pexpireat { key, timestamp_ms }
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
        "EXPIRETIME" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("expiretime")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("expiretime")));
            }
            Command::Expiretime { key }
        }
        "PEXPIRETIME" => {
            let Some(key_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("pexpiretime")));
            };
            let key = match parse_bulk_string(key_bytes) {
                Ok(k) => k,
                Err(e) => return Ok(Some(e)),
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("pexpiretime")));
            }
            Command::Pexpiretime { key }
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
        "EVAL" => {
            // EVAL script numkeys [key ...] [arg ...]
            let Some(script_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("eval")));
            };
            let script = match parse_bulk_string(script_bytes) {
                Ok(s) => s,
                Err(e) => return Ok(Some(e)),
            };
            let Some(numkeys_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("eval")));
            };
            let numkeys = match parse_i64_from_bulk(numkeys_bytes) {
                Ok(n) if n >= 0 => n as usize,
                _ => return Ok(Some(err_not_integer())),
            };
            let mut keys = Vec::with_capacity(numkeys);
            for _ in 0..numkeys {
                let Some(key_bytes) = iter.next() else {
                    return Ok(Some(err_wrong_args("eval")));
                };
                match parse_bulk_string(key_bytes) {
                    Ok(k) => keys.push(k),
                    Err(e) => return Ok(Some(e)),
                }
            }
            let mut args = Vec::new();
            for b in iter {
                match parse_bulk_string(b) {
                    Ok(a) => args.push(a),
                    Err(e) => return Ok(Some(e)),
                }
            }
            Command::Eval { script, keys, args }
        }
        "EVALSHA" => {
            // EVALSHA sha1 numkeys [key ...] [arg ...]
            let Some(sha1_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("evalsha")));
            };
            let sha1 = match parse_bulk_string(sha1_bytes) {
                Ok(s) => s,
                Err(e) => return Ok(Some(e)),
            };
            let Some(numkeys_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("evalsha")));
            };
            let numkeys = match parse_i64_from_bulk(numkeys_bytes) {
                Ok(n) if n >= 0 => n as usize,
                _ => return Ok(Some(err_not_integer())),
            };
            let mut keys = Vec::with_capacity(numkeys);
            for _ in 0..numkeys {
                let Some(key_bytes) = iter.next() else {
                    return Ok(Some(err_wrong_args("evalsha")));
                };
                match parse_bulk_string(key_bytes) {
                    Ok(k) => keys.push(k),
                    Err(e) => return Ok(Some(e)),
                }
            }
            let mut args = Vec::new();
            for b in iter {
                match parse_bulk_string(b) {
                    Ok(a) => args.push(a),
                    Err(e) => return Ok(Some(e)),
                }
            }
            Command::Evalsha { sha1, keys, args }
        }
        "SCRIPT" => {
            // SCRIPT LOAD script | SCRIPT EXISTS sha1 [sha1 ...] | SCRIPT FLUSH
            let Some(subcmd_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("script")));
            };
            let subcmd = match parse_bulk_string(subcmd_bytes) {
                Ok(s) => s.to_uppercase(),
                Err(e) => return Ok(Some(e)),
            };
            match subcmd.as_str() {
                "LOAD" => {
                    let Some(script_bytes) = iter.next() else {
                        return Ok(Some(err_wrong_args("script")));
                    };
                    let script = match parse_bulk_string(script_bytes) {
                        Ok(s) => s,
                        Err(e) => return Ok(Some(e)),
                    };
                    if iter.next().is_some() {
                        return Ok(Some(err_wrong_args("script")));
                    }
                    Command::ScriptLoad { script }
                }
                "EXISTS" => {
                    let mut sha1s = Vec::new();
                    for b in iter {
                        match parse_bulk_string(b) {
                            Ok(s) => sha1s.push(s),
                            Err(e) => return Ok(Some(e)),
                        }
                    }
                    if sha1s.is_empty() {
                        return Ok(Some(err_wrong_args("script")));
                    }
                    Command::ScriptExists { sha1s }
                }
                "FLUSH" => {
                    // 忽略可选的 ASYNC/SYNC 参数
                    Command::ScriptFlush
                }
                _ => Command::Error(format!(
                    "ERR Unknown SCRIPT subcommand or wrong number of arguments for '{}'",
                    subcmd
                )),
            }
        }
        "CONFIG" => {
            let Some(subcmd_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("config")));
            };
            let subcmd = match parse_bulk_string(subcmd_bytes) {
                Ok(s) => s.to_uppercase(),
                Err(e) => return Ok(Some(e)),
            };
            match subcmd.as_str() {
                "GET" => {
                    let Some(pattern_bytes) = iter.next() else {
                        return Ok(Some(err_wrong_args("config|get")));
                    };
                    let pattern = match parse_bulk_string(pattern_bytes) {
                        Ok(s) => s,
                        Err(e) => return Ok(Some(e)),
                    };
                    if iter.next().is_some() {
                        return Ok(Some(err_wrong_args("config|get")));
                    }
                    Command::ConfigGet { pattern }
                }
                "SET" => {
                    let Some(param_bytes) = iter.next() else {
                        return Ok(Some(err_wrong_args("config|set")));
                    };
                    let parameter = match parse_bulk_string(param_bytes) {
                        Ok(s) => s,
                        Err(e) => return Ok(Some(e)),
                    };
                    let Some(value_bytes) = iter.next() else {
                        return Ok(Some(err_wrong_args("config|set")));
                    };
                    let value = match parse_bulk_string(value_bytes) {
                        Ok(s) => s,
                        Err(e) => return Ok(Some(e)),
                    };
                    if iter.next().is_some() {
                        return Ok(Some(err_wrong_args("config|set")));
                    }
                    Command::ConfigSet { parameter, value }
                }
                _ => Command::Error(format!(
                    "ERR Unknown subcommand or wrong number of arguments for 'config|{}'",
                    subcmd.to_lowercase()
                )),
            }
        }
        "CLIENT" => {
            let Some(subcmd_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("client")));
            };
            let subcmd = match parse_bulk_string(subcmd_bytes) {
                Ok(s) => s.to_uppercase(),
                Err(e) => return Ok(Some(e)),
            };
            match subcmd.as_str() {
                "LIST" => {
                    // CLIENT LIST 可能有可选参数，但我们简化处理
                    Command::ClientList
                }
                "ID" => Command::ClientId,
                "SETNAME" => {
                    let Some(name_bytes) = iter.next() else {
                        return Ok(Some(err_wrong_args("client|setname")));
                    };
                    let name = match parse_bulk_string(name_bytes) {
                        Ok(s) => s,
                        Err(e) => return Ok(Some(e)),
                    };
                    Command::ClientSetname { name }
                }
                "GETNAME" => Command::ClientGetname,
                "PAUSE" => {
                    let Some(timeout_bytes) = iter.next() else {
                        return Ok(Some(err_wrong_args("client|pause")));
                    };
                    let timeout_ms = match parse_i64_from_bulk(timeout_bytes) {
                        Ok(v) if v >= 0 => v as u64,
                        _ => {
                            return Ok(Some(Command::Error(
                                "ERR timeout is not an integer or out of range".to_string(),
                            )))
                        }
                    };
                    Command::ClientPause { timeout_ms }
                }
                "UNPAUSE" => Command::ClientUnpause,
                _ => Command::Error(format!(
                    "ERR Unknown subcommand or wrong number of arguments for 'client|{}'",
                    subcmd.to_lowercase()
                )),
            }
        }
        "SLOWLOG" => {
            let Some(subcmd_bytes) = iter.next() else {
                return Ok(Some(err_wrong_args("slowlog")));
            };
            let subcmd = match parse_bulk_string(subcmd_bytes) {
                Ok(s) => s.to_uppercase(),
                Err(e) => return Ok(Some(e)),
            };
            match subcmd.as_str() {
                "GET" => {
                    let count = if let Some(count_bytes) = iter.next() {
                        let count_str = match parse_bulk_string(count_bytes) {
                            Ok(s) => s,
                            Err(e) => return Ok(Some(e)),
                        };
                        match count_str.parse::<usize>() {
                            Ok(n) => Some(n),
                            Err(_) => return Ok(Some(err_not_integer())),
                        }
                    } else {
                        None
                    };
                    Command::SlowlogGet { count }
                }
                "RESET" => Command::SlowlogReset,
                "LEN" => Command::SlowlogLen,
                _ => Command::Error(format!(
                    "ERR Unknown subcommand or wrong number of arguments for 'slowlog|{}'",
                    subcmd.to_lowercase()
                )),
            }
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
