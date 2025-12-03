//! Lua scripting support for Redis-compatible EVAL/EVALSHA commands.

use dashmap::DashMap;
use mlua::{Lua, Value};
use sha1::{Digest, Sha1};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use crate::storage::Storage;

/// Script cache: SHA1 -> script source
pub struct ScriptCache {
    scripts: DashMap<String, String>,
}

impl ScriptCache {
    pub fn new() -> Self {
        Self {
            scripts: DashMap::new(),
        }
    }

    /// Compute SHA1 hash of a script
    pub fn compute_sha1(script: &str) -> String {
        let mut hasher = Sha1::new();
        hasher.update(script.as_bytes());
        let result = hasher.finalize();
        hex::encode(result)
    }

    /// Load a script into the cache, returns its SHA1
    pub fn load(&self, script: &str) -> String {
        let sha1 = Self::compute_sha1(script);
        self.scripts.insert(sha1.clone(), script.to_string());
        sha1
    }

    /// Get a script by SHA1
    pub fn get(&self, sha1: &str) -> Option<String> {
        self.scripts.get(sha1).map(|v| v.clone())
    }

    /// Check if scripts exist
    pub fn exists(&self, sha1s: &[String]) -> Vec<bool> {
        sha1s.iter().map(|s| self.scripts.contains_key(s)).collect()
    }

    /// Flush all scripts
    pub fn flush(&self) {
        self.scripts.clear();
    }
}

impl Default for ScriptCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of script execution
#[derive(Debug, Clone)]
pub enum ScriptResult {
    Nil,
    Integer(i64),
    String(String),
    BulkString(Vec<u8>),
    Array(Vec<ScriptResult>),
    Status(String),
    Error(String),
}

impl ScriptResult {
    /// Convert to RESP format (returns raw bytes to preserve binary data)
    pub fn to_resp_bytes(&self) -> Vec<u8> {
        match self {
            ScriptResult::Nil => b"$-1\r\n".to_vec(),
            ScriptResult::Integer(n) => format!(":{}\r\n", n).into_bytes(),
            ScriptResult::String(s) => format!("${}\r\n{}\r\n", s.len(), s).into_bytes(),
            ScriptResult::BulkString(b) => {
                let mut resp = format!("${}\r\n", b.len()).into_bytes();
                resp.extend_from_slice(b);
                resp.extend_from_slice(b"\r\n");
                resp
            }
            ScriptResult::Array(arr) => {
                let mut resp = format!("*{}\r\n", arr.len()).into_bytes();
                for item in arr {
                    resp.extend(item.to_resp_bytes());
                }
                resp
            }
            ScriptResult::Status(s) => format!("+{}\r\n", s).into_bytes(),
            ScriptResult::Error(e) => format!("-{}\r\n", e).into_bytes(),
        }
    }
}

/// Context for script execution
pub struct ScriptContext {
    pub storage: Arc<Storage>,
    pub current_db: u32,
    pub keys: Vec<String>,
    pub args: Vec<String>,
}

/// Execute a Lua script with redis.call/pcall support
pub fn execute_script(
    script: &str,
    ctx: ScriptContext,
) -> Result<ScriptResult, String> {
    let lua = Lua::new();

    // Set up KEYS and ARGV tables
    let globals = lua.globals();

    // Create KEYS table (1-indexed)
    let keys_table = lua.create_table().map_err(|e| format!("ERR {}", e))?;
    for (i, key) in ctx.keys.iter().enumerate() {
        keys_table.set(i + 1, key.as_str()).map_err(|e| format!("ERR {}", e))?;
    }
    globals.set("KEYS", keys_table).map_err(|e| format!("ERR {}", e))?;

    // Create ARGV table (1-indexed)
    let argv_table = lua.create_table().map_err(|e| format!("ERR {}", e))?;
    for (i, arg) in ctx.args.iter().enumerate() {
        argv_table.set(i + 1, arg.as_str()).map_err(|e| format!("ERR {}", e))?;
    }
    globals.set("ARGV", argv_table).map_err(|e| format!("ERR {}", e))?;

    // Create redis table with call/pcall
    let redis_table = lua.create_table().map_err(|e| format!("ERR {}", e))?;
    
    // Shared context for redis.call/pcall
    let storage = ctx.storage.clone();
    let current_db = ctx.current_db;
    
    // Wrap storage in Rc<RefCell> for sharing between closures
    let storage_rc = Rc::new(RefCell::new(storage));
    
    // redis.call - executes command and raises error on failure
    let storage_call = storage_rc.clone();
    let call_fn = lua.create_function(move |lua, args: mlua::MultiValue| -> mlua::Result<Value> {
        let storage = storage_call.borrow();
        execute_redis_command(lua, &storage, current_db, args, false)
    }).map_err(|e| format!("ERR {}", e))?;
    redis_table.set("call", call_fn).map_err(|e| format!("ERR {}", e))?;
    
    // redis.pcall - executes command and returns error as table on failure
    let storage_pcall = storage_rc.clone();
    let pcall_fn = lua.create_function(move |lua, args: mlua::MultiValue| -> mlua::Result<Value> {
        let storage = storage_pcall.borrow();
        execute_redis_command(lua, &storage, current_db, args, true)
    }).map_err(|e| format!("ERR {}", e))?;
    redis_table.set("pcall", pcall_fn).map_err(|e| format!("ERR {}", e))?;
    
    globals.set("redis", redis_table).map_err(|e| format!("ERR {}", e))?;

    // Execute the script
    let result: Value = lua.load(script).eval().map_err(|e| format!("ERR {}", e))?;

    // Convert Lua value to ScriptResult
    lua_value_to_result(result)
}

/// Convert Lua value to ScriptResult
fn lua_value_to_result(value: Value) -> Result<ScriptResult, String> {
    match value {
        Value::Nil => Ok(ScriptResult::Nil),
        Value::Boolean(b) => {
            // Redis Lua: false -> nil, true -> 1
            if b {
                Ok(ScriptResult::Integer(1))
            } else {
                Ok(ScriptResult::Nil)
            }
        }
        Value::Integer(n) => Ok(ScriptResult::Integer(n)),
        Value::Number(n) => {
            // Redis converts floats to integers by truncation
            Ok(ScriptResult::Integer(n as i64))
        }
        Value::String(s) => {
            let bytes = s.as_bytes().to_vec();
            Ok(ScriptResult::BulkString(bytes))
        }
        Value::Table(t) => {
            // Check if it's an error table
            if let Ok(err) = t.get::<_, String>("err") {
                return Ok(ScriptResult::Error(err));
            }
            // Check if it's a status table
            if let Ok(ok) = t.get::<_, String>("ok") {
                return Ok(ScriptResult::Status(ok));
            }
            // Otherwise treat as array
            let mut arr = Vec::new();
            let mut i = 1i64;
            loop {
                match t.get::<i64, Value>(i) {
                    Ok(Value::Nil) => break,
                    Ok(v) => {
                        arr.push(lua_value_to_result(v)?);
                        i += 1;
                    }
                    Err(_) => break,
                }
            }
            Ok(ScriptResult::Array(arr))
        }
        _ => Ok(ScriptResult::Nil),
    }
}

/// Prefix key with database number
fn prefix_key(db: u32, key: &str) -> String {
    format!("{}:{}", db, key)
}

/// Execute a Redis command from Lua
/// FIX P1: Uses binary-safe byte vectors for arguments instead of String
fn execute_redis_command<'lua>(
    lua: &'lua Lua,
    storage: &Arc<Storage>,
    current_db: u32,
    args: mlua::MultiValue<'lua>,
    is_pcall: bool,
) -> mlua::Result<Value<'lua>> {
    // Convert args to byte strings (binary-safe)
    // Redis scripting is binary-safe: we must preserve raw bytes from Lua strings
    let mut args_vec: Vec<Vec<u8>> = Vec::new();
    for v in args {
        match v {
            Value::String(s) => {
                // Lua strings are binary-safe, get raw bytes
                args_vec.push(s.as_bytes().to_vec());
            }
            Value::Integer(n) => {
                args_vec.push(n.to_string().into_bytes());
            }
            Value::Number(n) => {
                // Format floats consistently
                let s = if n.fract() == 0.0 {
                    format!("{:.0}", n)
                } else {
                    n.to_string()
                };
                args_vec.push(s.into_bytes());
            }
            _ => {
                // FIX P1: Reject unsupported types rather than silently dropping them
                return make_error(
                    lua,
                    "ERR Lua redis() command arguments must be strings or integers",
                    is_pcall,
                );
            }
        }
    }

    if args_vec.is_empty() {
        return make_error(lua, "ERR wrong number of arguments", is_pcall);
    }

    // First argument is the command name (must be valid UTF-8)
    let cmd = match String::from_utf8(args_vec[0].clone()) {
        Ok(s) => s.to_uppercase(),
        Err(_) => {
            return make_error(lua, "ERR command name must be valid UTF-8", is_pcall);
        }
    };
    let cmd_args = &args_vec[1..];

    // Execute the command
    let result = match cmd.as_str() {
        "GET" => cmd_get(storage, current_db, cmd_args),
        "SET" => cmd_set(storage, current_db, cmd_args),
        "DEL" => cmd_del(storage, current_db, cmd_args),
        "EXISTS" => cmd_exists(storage, current_db, cmd_args),
        "INCR" => cmd_incr(storage, current_db, cmd_args),
        "DECR" => cmd_decr(storage, current_db, cmd_args),
        "INCRBY" => cmd_incrby(storage, current_db, cmd_args),
        "DECRBY" => cmd_decrby(storage, current_db, cmd_args),
        "APPEND" => cmd_append(storage, current_db, cmd_args),
        "STRLEN" => cmd_strlen(storage, current_db, cmd_args),
        "MGET" => cmd_mget(storage, current_db, cmd_args),
        "MSET" => cmd_mset(storage, current_db, cmd_args),
        "HGET" => cmd_hget(storage, current_db, cmd_args),
        "HSET" => cmd_hset(storage, current_db, cmd_args),
        "HDEL" => cmd_hdel(storage, current_db, cmd_args),
        "HEXISTS" => cmd_hexists(storage, current_db, cmd_args),
        "HGETALL" => cmd_hgetall(storage, current_db, cmd_args),
        "HKEYS" => cmd_hkeys(storage, current_db, cmd_args),
        "HVALS" => cmd_hvals(storage, current_db, cmd_args),
        "HLEN" => cmd_hlen(storage, current_db, cmd_args),
        "HMGET" => cmd_hmget(storage, current_db, cmd_args),
        "HMSET" => cmd_hmset(storage, current_db, cmd_args),
        "HINCRBY" => cmd_hincrby(storage, current_db, cmd_args),
        "LPUSH" => cmd_lpush(storage, current_db, cmd_args),
        "RPUSH" => cmd_rpush(storage, current_db, cmd_args),
        "LPOP" => cmd_lpop(storage, current_db, cmd_args),
        "RPOP" => cmd_rpop(storage, current_db, cmd_args),
        "LLEN" => cmd_llen(storage, current_db, cmd_args),
        "LRANGE" => cmd_lrange(storage, current_db, cmd_args),
        "LINDEX" => cmd_lindex(storage, current_db, cmd_args),
        "SADD" => cmd_sadd(storage, current_db, cmd_args),
        "SREM" => cmd_srem(storage, current_db, cmd_args),
        "SMEMBERS" => cmd_smembers(storage, current_db, cmd_args),
        "SISMEMBER" => cmd_sismember(storage, current_db, cmd_args),
        "SCARD" => cmd_scard(storage, current_db, cmd_args),
        "ZADD" => cmd_zadd(storage, current_db, cmd_args),
        "ZREM" => cmd_zrem(storage, current_db, cmd_args),
        "ZSCORE" => cmd_zscore(storage, current_db, cmd_args),
        "ZCARD" => cmd_zcard(storage, current_db, cmd_args),
        "ZRANGE" => cmd_zrange(storage, current_db, cmd_args),
        "ZREVRANGE" => cmd_zrevrange(storage, current_db, cmd_args),
        "TYPE" => cmd_type(storage, current_db, cmd_args),
        "TTL" => cmd_ttl(storage, current_db, cmd_args),
        "PTTL" => cmd_pttl(storage, current_db, cmd_args),
        "EXPIRE" => cmd_expire(storage, current_db, cmd_args),
        "PEXPIRE" => cmd_pexpire(storage, current_db, cmd_args),
        "PERSIST" => cmd_persist(storage, current_db, cmd_args),
        _ => Err(format!("ERR unknown command '{}'", cmd)),
    };

    match result {
        Ok(res) => script_result_to_lua(lua, res),
        Err(e) => make_error(lua, &e, is_pcall),
    }
}

/// Create an error value (raises for call, returns table for pcall)
fn make_error<'lua>(lua: &'lua Lua, msg: &str, is_pcall: bool) -> mlua::Result<Value<'lua>> {
    if is_pcall {
        let err_table = lua.create_table()?;
        err_table.set("err", msg)?;
        Ok(Value::Table(err_table))
    } else {
        Err(mlua::Error::RuntimeError(msg.to_string()))
    }
}

/// Convert ScriptResult to Lua Value
/// FIX P2: Redis scripting semantics - nil bulk replies are converted to false (not nil)
/// because Lua tables cannot contain nil values without losing their slots.
fn script_result_to_lua<'lua>(lua: &'lua Lua, result: ScriptResult) -> mlua::Result<Value<'lua>> {
    match result {
        ScriptResult::Nil => Ok(Value::Boolean(false)), // FIX P2: nil -> false
        ScriptResult::Integer(n) => Ok(Value::Integer(n)),
        ScriptResult::String(s) => Ok(Value::String(lua.create_string(&s)?)),
        ScriptResult::BulkString(b) => Ok(Value::String(lua.create_string(&b)?)),
        ScriptResult::Array(arr) => {
            let table = lua.create_table()?;
            for (i, item) in arr.into_iter().enumerate() {
                table.set(i + 1, script_result_to_lua(lua, item)?)?;
            }
            Ok(Value::Table(table))
        }
        ScriptResult::Status(s) => {
            let table = lua.create_table()?;
            table.set("ok", s)?;
            Ok(Value::Table(table))
        }
        ScriptResult::Error(e) => {
            let table = lua.create_table()?;
            table.set("err", e)?;
            Ok(Value::Table(table))
        }
    }
}

// ============================================================================
// Redis command implementations for Lua scripting
// ============================================================================

/// Helper: convert bytes to String (for keys that must be UTF-8)
fn bytes_to_string(bytes: &[u8]) -> Result<String, String> {
    String::from_utf8(bytes.to_vec())
        .map_err(|_| "ERR invalid UTF-8 in key".to_string())
}

/// Helper: convert bytes slice to Vec<String> (for keys)
fn bytes_vec_to_strings(bytes_vec: &[Vec<u8>]) -> Result<Vec<String>, String> {
    bytes_vec
        .iter()
        .map(|b| bytes_to_string(b))
        .collect()
}

fn cmd_get(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.len() != 1 {
        return Err("ERR wrong number of arguments for 'get' command".to_string());
    }
    let key_str = bytes_to_string(&args[0])?;
    let key = prefix_key(db, &key_str);
    match storage.get(&key) {
        Some(v) => Ok(ScriptResult::BulkString(v)),
        None => Ok(ScriptResult::Nil),
    }
}

fn cmd_set(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.len() < 2 {
        return Err("ERR wrong number of arguments for 'set' command".to_string());
    }
    let key_str = bytes_to_string(&args[0])?;
    let key = prefix_key(db, &key_str);
    let value = args[1].clone(); // Value is binary-safe
    storage.set(key, value);
    Ok(ScriptResult::Status("OK".to_string()))
}

fn cmd_del(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.is_empty() {
        return Err("ERR wrong number of arguments for 'del' command".to_string());
    }
    let keys_str = bytes_vec_to_strings(args)?;
    let keys: Vec<String> = keys_str.iter().map(|k| prefix_key(db, k)).collect();
    let count = storage.del(&keys);
    Ok(ScriptResult::Integer(count as i64))
}

fn cmd_exists(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.is_empty() {
        return Err("ERR wrong number of arguments for 'exists' command".to_string());
    }
    let keys_str = bytes_vec_to_strings(args)?;
    let keys: Vec<String> = keys_str.iter().map(|k| prefix_key(db, k)).collect();
    let count = storage.exists(&keys);
    Ok(ScriptResult::Integer(count as i64))
}

fn cmd_incr(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.len() != 1 {
        return Err("ERR wrong number of arguments for 'incr' command".to_string());
    }
    let key_str = bytes_to_string(&args[0])?;
    let key = prefix_key(db, &key_str);
    match storage.incr(&key) {
        Ok(v) => Ok(ScriptResult::Integer(v)),
        Err(()) => Err("ERR value is not an integer or out of range".to_string()),
    }
}

fn cmd_decr(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.len() != 1 {
        return Err("ERR wrong number of arguments for 'decr' command".to_string());
    }
    let key_str = bytes_to_string(&args[0])?;
    let key = prefix_key(db, &key_str);
    match storage.decr(&key) {
        Ok(v) => Ok(ScriptResult::Integer(v)),
        Err(()) => Err("ERR value is not an integer or out of range".to_string()),
    }
}

fn cmd_incrby(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.len() != 2 {
        return Err("ERR wrong number of arguments for 'incrby' command".to_string());
    }
    let key_str = bytes_to_string(&args[0])?;
    let key = prefix_key(db, &key_str);
    let delta_str = bytes_to_string(&args[1])?;
    let delta: i64 = delta_str.parse().map_err(|_| "ERR value is not an integer or out of range".to_string())?;
    match storage.incr_by(&key, delta) {
        Ok(v) => Ok(ScriptResult::Integer(v)),
        Err(()) => Err("ERR value is not an integer or out of range".to_string()),
    }
}

fn cmd_decrby(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.len() != 2 {
        return Err("ERR wrong number of arguments for 'decrby' command".to_string());
    }
    let key_str = bytes_to_string(&args[0])?;
    let key = prefix_key(db, &key_str);
    let delta_str = bytes_to_string(&args[1])?;
    let delta: i64 = delta_str.parse().map_err(|_| "ERR value is not an integer or out of range".to_string())?;
    match storage.incr_by(&key, -delta) {
        Ok(v) => Ok(ScriptResult::Integer(v)),
        Err(()) => Err("ERR value is not an integer or out of range".to_string()),
    }
}

fn cmd_append(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.len() != 2 {
        return Err("ERR wrong number of arguments for 'append' command".to_string());
    }
    let key_str = bytes_to_string(&args[0])?;
    let key = prefix_key(db, &key_str);
    let value = args[1].clone(); // Binary-safe
    match storage.append(&key, &value) {
        Ok(len) => Ok(ScriptResult::Integer(len as i64)),
        Err(()) => Err("ERR WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
    }
}

fn cmd_strlen(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.len() != 1 {
        return Err("ERR wrong number of arguments for 'strlen' command".to_string());
    }
    let key_str = bytes_to_string(&args[0])?;
    let key = prefix_key(db, &key_str);
    match storage.strlen(&key) {
        Ok(len) => Ok(ScriptResult::Integer(len as i64)),
        Err(()) => Err("ERR WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
    }
}

fn cmd_mget(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.is_empty() {
        return Err("ERR wrong number of arguments for 'mget' command".to_string());
    }
    let keys_str = bytes_vec_to_strings(args)?;
    let keys: Vec<String> = keys_str.iter().map(|k| prefix_key(db, k)).collect();
    let mut results = Vec::new();
    for key in keys {
        match storage.get(&key) {
            Some(v) => results.push(ScriptResult::BulkString(v)),
            None => results.push(ScriptResult::Nil),
        }
    }
    Ok(ScriptResult::Array(results))
}

fn cmd_mset(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.len() < 2 || args.len() % 2 != 0 {
        return Err("ERR wrong number of arguments for 'mset' command".to_string());
    }
    for i in (0..args.len()).step_by(2) {
        let key_str = bytes_to_string(&args[i])?;
        let key = prefix_key(db, &key_str);
        let value = args[i + 1].clone(); // Binary-safe
        storage.set(key, value);
    }
    Ok(ScriptResult::Status("OK".to_string()))
}

// Hash commands
fn cmd_hget(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.len() != 2 {
        return Err("ERR wrong number of arguments for 'hget' command".to_string());
    }
    let key_str = bytes_to_string(&args[0])?;
    let key = prefix_key(db, &key_str);
    let field = bytes_to_string(&args[1])?;
    match storage.hget(&key, &field) {
        Ok(Some(v)) => Ok(ScriptResult::String(v)),
        Ok(None) => Ok(ScriptResult::Nil),
        Err(()) => Err("ERR WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
    }
}

fn cmd_hset(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.len() < 3 || (args.len() - 1) % 2 != 0 {
        return Err("ERR wrong number of arguments for 'hset' command".to_string());
    }
    let key_str = bytes_to_string(&args[0])?;
    let key = prefix_key(db, &key_str);
    let mut total_added = 0;
    for i in (1..args.len()).step_by(2) {
        let field = bytes_to_string(&args[i])?;
        let value_bytes = &args[i + 1];
        let value = String::from_utf8_lossy(value_bytes).to_string(); // Convert to String for storage
        match storage.hset(&key, &field, value) {
            Ok(added) => total_added += added as i64,
            Err(()) => return Err("ERR WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    }
    Ok(ScriptResult::Integer(total_added))
}

fn cmd_hdel(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.len() < 2 {
        return Err("ERR wrong number of arguments for 'hdel' command".to_string());
    }
    let key_str = bytes_to_string(&args[0])?;
    let key = prefix_key(db, &key_str);
    let fields = bytes_vec_to_strings(&args[1..])?;
    match storage.hdel(&key, &fields) {
        Ok(count) => Ok(ScriptResult::Integer(count as i64)),
        Err(()) => Err("ERR WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
    }
}

fn cmd_hexists(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.len() != 2 {
        return Err("ERR wrong number of arguments for 'hexists' command".to_string());
    }
    let key_str = bytes_to_string(&args[0])?;
    let key = prefix_key(db, &key_str);
    let field = bytes_to_string(&args[1])?;
    match storage.hexists(&key, &field) {
        Ok(exists) => Ok(ScriptResult::Integer(if exists { 1 } else { 0 })),
        Err(()) => Err("ERR WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
    }
}

fn cmd_hgetall(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.len() != 1 {
        return Err("ERR wrong number of arguments for 'hgetall' command".to_string());
    }
    let key_str = bytes_to_string(&args[0])?;
    let key = prefix_key(db, &key_str);
    match storage.hgetall(&key) {
        Ok(pairs) => {
            let mut results = Vec::new();
            for (field, value) in pairs {
                results.push(ScriptResult::String(field));
                results.push(ScriptResult::String(value));
            }
            Ok(ScriptResult::Array(results))
        }
        Err(()) => Err("ERR WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
    }
}

fn cmd_hkeys(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.len() != 1 {
        return Err("ERR wrong number of arguments for 'hkeys' command".to_string());
    }
    let key_str = bytes_to_string(&args[0])?;
    let key = prefix_key(db, &key_str);
    match storage.hkeys(&key) {
        Ok(keys) => {
            let results: Vec<ScriptResult> = keys
                .into_iter()
                .map(|k| ScriptResult::String(k))
                .collect();
            Ok(ScriptResult::Array(results))
        }
        Err(()) => Err("ERR WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
    }
}

fn cmd_hvals(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.len() != 1 {
        return Err("ERR wrong number of arguments for 'hvals' command".to_string());
    }
    let key_str = bytes_to_string(&args[0])?;
    let key = prefix_key(db, &key_str);
    match storage.hvals(&key) {
        Ok(vals) => {
            let results: Vec<ScriptResult> = vals
                .into_iter()
                .map(|v| ScriptResult::String(v))
                .collect();
            Ok(ScriptResult::Array(results))
        }
        Err(()) => Err("ERR WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
    }
}

fn cmd_hlen(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.len() != 1 {
        return Err("ERR wrong number of arguments for 'hlen' command".to_string());
    }
    let key_str = bytes_to_string(&args[0])?;
    let key = prefix_key(db, &key_str);
    match storage.hlen(&key) {
        Ok(len) => Ok(ScriptResult::Integer(len as i64)),
        Err(()) => Err("ERR WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
    }
}

fn cmd_hmget(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.len() < 2 {
        return Err("ERR wrong number of arguments for 'hmget' command".to_string());
    }
    let key_str = bytes_to_string(&args[0])?;
    let key = prefix_key(db, &key_str);
    let fields = bytes_vec_to_strings(&args[1..])?;
    match storage.hmget(&key, &fields) {
        Ok(values) => {
            let results: Vec<ScriptResult> = values
                .into_iter()
                .map(|v| match v {
                    Some(val) => ScriptResult::String(val),
                    None => ScriptResult::Nil,
                })
                .collect();
            Ok(ScriptResult::Array(results))
        }
        Err(()) => Err("ERR WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
    }
}

fn cmd_hmset(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.len() < 3 || (args.len() - 1) % 2 != 0 {
        return Err("ERR wrong number of arguments for 'hmset' command".to_string());
    }
    let key_str = bytes_to_string(&args[0])?;
    let key = prefix_key(db, &key_str);
    for i in (1..args.len()).step_by(2) {
        let field = bytes_to_string(&args[i])?;
        let value_bytes = &args[i + 1];
        let value = String::from_utf8_lossy(value_bytes).to_string();
        storage.hset(&key, &field, value);
    }
    Ok(ScriptResult::Status("OK".to_string()))
}

fn cmd_hincrby(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.len() != 3 {
        return Err("ERR wrong number of arguments for 'hincrby' command".to_string());
    }
    let key_str = bytes_to_string(&args[0])?;
    let key = prefix_key(db, &key_str);
    let field = bytes_to_string(&args[1])?;
    let delta_str = bytes_to_string(&args[2])?;
    let delta: i64 = delta_str.parse().map_err(|_| "ERR value is not an integer or out of range".to_string())?;
    match storage.hincr_by(&key, &field, delta, None) {
        Ok(new_val) => Ok(ScriptResult::Integer(new_val)),
        Err(_) => Err("ERR hash value is not an integer".to_string()),
    }
}

// List commands
fn cmd_lpush(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.len() < 2 {
        return Err("ERR wrong number of arguments for 'lpush' command".to_string());
    }
    let key_str = bytes_to_string(&args[0])?;
    let key = prefix_key(db, &key_str);
    let values: Vec<String> = args[1..].iter().map(|v| String::from_utf8_lossy(v).to_string()).collect();
    match storage.lpush(&key, &values) {
        Ok(len) => Ok(ScriptResult::Integer(len as i64)),
        Err(()) => Err("ERR WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
    }
}

fn cmd_rpush(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.len() < 2 {
        return Err("ERR wrong number of arguments for 'rpush' command".to_string());
    }
    let key_str = bytes_to_string(&args[0])?;
    let key = prefix_key(db, &key_str);
    let values: Vec<String> = args[1..].iter().map(|v| String::from_utf8_lossy(v).to_string()).collect();
    match storage.rpush(&key, &values) {
        Ok(len) => Ok(ScriptResult::Integer(len as i64)),
        Err(()) => Err("ERR WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
    }
}

fn cmd_lpop(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.is_empty() {
        return Err("ERR wrong number of arguments for 'lpop' command".to_string());
    }
    let key_str = bytes_to_string(&args[0])?;
    let key = prefix_key(db, &key_str);
    match storage.lpop(&key) {
        Ok(Some(v)) => Ok(ScriptResult::String(v)),
        Ok(None) => Ok(ScriptResult::Nil),
        Err(()) => Err("ERR WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
    }
}

fn cmd_rpop(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.is_empty() {
        return Err("ERR wrong number of arguments for 'rpop' command".to_string());
    }
    let key_str = bytes_to_string(&args[0])?;
    let key = prefix_key(db, &key_str);
    match storage.rpop(&key) {
        Ok(Some(v)) => Ok(ScriptResult::String(v)),
        Ok(None) => Ok(ScriptResult::Nil),
        Err(()) => Err("ERR WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
    }
}

fn cmd_llen(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.len() != 1 {
        return Err("ERR wrong number of arguments for 'llen' command".to_string());
    }
    let key_str = bytes_to_string(&args[0])?;
    let key = prefix_key(db, &key_str);
    match storage.llen(&key) {
        Ok(len) => Ok(ScriptResult::Integer(len as i64)),
        Err(()) => Err("ERR WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
    }
}

fn cmd_lrange(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.len() != 3 {
        return Err("ERR wrong number of arguments for 'lrange' command".to_string());
    }
    let key_str = bytes_to_string(&args[0])?;
    let key = prefix_key(db, &key_str);
    let start_str = bytes_to_string(&args[1])?;
    let stop_str = bytes_to_string(&args[2])?;
    let start: isize = start_str.parse().map_err(|_| "ERR value is not an integer or out of range".to_string())?;
    let stop: isize = stop_str.parse().map_err(|_| "ERR value is not an integer or out of range".to_string())?;
    match storage.lrange(&key, start, stop) {
        Ok(values) => {
            let results: Vec<ScriptResult> = values
                .into_iter()
                .map(|v| ScriptResult::String(v))
                .collect();
            Ok(ScriptResult::Array(results))
        }
        Err(()) => Err("ERR WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
    }
}

fn cmd_lindex(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.len() != 2 {
        return Err("ERR wrong number of arguments for 'lindex' command".to_string());
    }
    let key_str = bytes_to_string(&args[0])?;
    let key = prefix_key(db, &key_str);
    let index_str = bytes_to_string(&args[1])?;
    let index: isize = index_str.parse().map_err(|_| "ERR value is not an integer or out of range".to_string())?;
    match storage.lindex(&key, index) {
        Ok(Some(v)) => Ok(ScriptResult::String(v)),
        Ok(None) => Ok(ScriptResult::Nil),
        Err(()) => Err("ERR WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
    }
}

// Set commands
fn cmd_sadd(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.len() < 2 {
        return Err("ERR wrong number of arguments for 'sadd' command".to_string());
    }
    let key_str = bytes_to_string(&args[0])?;
    let key = prefix_key(db, &key_str);
    let members: Vec<String> = args[1..].iter().map(|v| String::from_utf8_lossy(v).to_string()).collect();
    match storage.sadd(&key, &members) {
        Ok(count) => Ok(ScriptResult::Integer(count as i64)),
        Err(()) => Err("ERR WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
    }
}

fn cmd_srem(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.len() < 2 {
        return Err("ERR wrong number of arguments for 'srem' command".to_string());
    }
    let key_str = bytes_to_string(&args[0])?;
    let key = prefix_key(db, &key_str);
    let members: Vec<String> = args[1..].iter().map(|v| String::from_utf8_lossy(v).to_string()).collect();
    match storage.srem(&key, &members) {
        Ok(count) => Ok(ScriptResult::Integer(count as i64)),
        Err(()) => Err("ERR WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
    }
}

fn cmd_smembers(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.len() != 1 {
        return Err("ERR wrong number of arguments for 'smembers' command".to_string());
    }
    let key_str = bytes_to_string(&args[0])?;
    let key = prefix_key(db, &key_str);
    match storage.smembers(&key) {
        Ok(members) => {
            let results: Vec<ScriptResult> = members
                .into_iter()
                .map(|m| ScriptResult::String(m))
                .collect();
            Ok(ScriptResult::Array(results))
        }
        Err(()) => Err("ERR WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
    }
}

fn cmd_sismember(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.len() != 2 {
        return Err("ERR wrong number of arguments for 'sismember' command".to_string());
    }
    let key_str = bytes_to_string(&args[0])?;
    let key = prefix_key(db, &key_str);
    let member = String::from_utf8_lossy(&args[1]).to_string();
    match storage.sismember(&key, &member) {
        Ok(is_member) => Ok(ScriptResult::Integer(if is_member { 1 } else { 0 })),
        Err(()) => Err("ERR WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
    }
}

fn cmd_scard(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.len() != 1 {
        return Err("ERR wrong number of arguments for 'scard' command".to_string());
    }
    let key_str = bytes_to_string(&args[0])?;
    let key = prefix_key(db, &key_str);
    match storage.scard(&key) {
        Ok(count) => Ok(ScriptResult::Integer(count as i64)),
        Err(()) => Err("ERR WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
    }
}

// Sorted Set commands
fn cmd_zadd(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.len() < 3 || (args.len() - 1) % 2 != 0 {
        return Err("ERR wrong number of arguments for 'zadd' command".to_string());
    }
    let key_str = bytes_to_string(&args[0])?;
    let key = prefix_key(db, &key_str);
    let mut members = Vec::new();
    for i in (1..args.len()).step_by(2) {
        let score_str = bytes_to_string(&args[i])?;
        let score: f64 = score_str.parse().map_err(|_| "ERR value is not a valid float".to_string())?;
        let member = String::from_utf8_lossy(&args[i + 1]).to_string();
        members.push((score, member));
    }
    match storage.zadd(&key, &members) {
        Ok(added) => Ok(ScriptResult::Integer(added as i64)),
        Err(_) => Err("ERR WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
    }
}

fn cmd_zrem(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.len() < 2 {
        return Err("ERR wrong number of arguments for 'zrem' command".to_string());
    }
    let key_str = bytes_to_string(&args[0])?;
    let key = prefix_key(db, &key_str);
    let members: Vec<String> = args[1..].iter().map(|v| String::from_utf8_lossy(v).to_string()).collect();
    match storage.zrem(&key, &members) {
        Ok(count) => Ok(ScriptResult::Integer(count as i64)),
        Err(()) => Err("ERR WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
    }
}

fn cmd_zscore(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.len() != 2 {
        return Err("ERR wrong number of arguments for 'zscore' command".to_string());
    }
    let key_str = bytes_to_string(&args[0])?;
    let key = prefix_key(db, &key_str);
    let member = String::from_utf8_lossy(&args[1]).to_string();
    match storage.zscore(&key, &member) {
        Ok(Some(score)) => Ok(ScriptResult::String(score.to_string())),
        Ok(None) => Ok(ScriptResult::Nil),
        Err(()) => Err("ERR WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
    }
}

fn cmd_zcard(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.len() != 1 {
        return Err("ERR wrong number of arguments for 'zcard' command".to_string());
    }
    let key_str = bytes_to_string(&args[0])?;
    let key = prefix_key(db, &key_str);
    match storage.zcard(&key) {
        Ok(count) => Ok(ScriptResult::Integer(count as i64)),
        Err(()) => Err("ERR WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
    }
}

fn cmd_zrange(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.len() < 3 {
        return Err("ERR wrong number of arguments for 'zrange' command".to_string());
    }
    let key_str = bytes_to_string(&args[0])?;
    let key = prefix_key(db, &key_str);
    let start_str = bytes_to_string(&args[1])?;
    let stop_str = bytes_to_string(&args[2])?;
    let start: isize = start_str.parse().map_err(|_| "ERR value is not an integer or out of range".to_string())?;
    let stop: isize = stop_str.parse().map_err(|_| "ERR value is not an integer or out of range".to_string())?;
    let withscores = args.len() > 3 && bytes_to_string(&args[3]).map(|s| s.to_uppercase() == "WITHSCORES").unwrap_or(false);
    
    match storage.zrange(&key, start, stop, false) {
        Ok(results) => {
            let mut script_results = Vec::new();
            for (member, score) in results {
                script_results.push(ScriptResult::String(member));
                if withscores {
                    script_results.push(ScriptResult::String(score.to_string()));
                }
            }
            Ok(ScriptResult::Array(script_results))
        }
        Err(()) => Err("ERR WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
    }
}

fn cmd_zrevrange(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.len() < 3 {
        return Err("ERR wrong number of arguments for 'zrevrange' command".to_string());
    }
    let key_str = bytes_to_string(&args[0])?;
    let key = prefix_key(db, &key_str);
    let start_str = bytes_to_string(&args[1])?;
    let stop_str = bytes_to_string(&args[2])?;
    let start: isize = start_str.parse().map_err(|_| "ERR value is not an integer or out of range".to_string())?;
    let stop: isize = stop_str.parse().map_err(|_| "ERR value is not an integer or out of range".to_string())?;
    let withscores = args.len() > 3 && bytes_to_string(&args[3]).map(|s| s.to_uppercase() == "WITHSCORES").unwrap_or(false);
    
    // Use zrange with rev=true for zrevrange
    match storage.zrange(&key, start, stop, true) {
        Ok(results) => {
            let mut script_results = Vec::new();
            for (member, score) in results {
                script_results.push(ScriptResult::String(member));
                if withscores {
                    script_results.push(ScriptResult::String(score.to_string()));
                }
            }
            Ok(ScriptResult::Array(script_results))
        }
        Err(()) => Err("ERR WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
    }
}

// Key management commands
fn cmd_type(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.len() != 1 {
        return Err("ERR wrong number of arguments for 'type' command".to_string());
    }
    let key_str = bytes_to_string(&args[0])?;
    let key = prefix_key(db, &key_str);
    let type_str = storage.type_of(&key);
    Ok(ScriptResult::Status(type_str))
}

fn cmd_ttl(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.len() != 1 {
        return Err("ERR wrong number of arguments for 'ttl' command".to_string());
    }
    let key_str = bytes_to_string(&args[0])?;
    let key = prefix_key(db, &key_str);
    let ttl = storage.ttl_seconds(&key);
    Ok(ScriptResult::Integer(ttl))
}

fn cmd_pttl(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.len() != 1 {
        return Err("ERR wrong number of arguments for 'pttl' command".to_string());
    }
    let key_str = bytes_to_string(&args[0])?;
    let key = prefix_key(db, &key_str);
    let pttl = storage.pttl_millis(&key);
    Ok(ScriptResult::Integer(pttl))
}

fn cmd_expire(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.len() != 2 {
        return Err("ERR wrong number of arguments for 'expire' command".to_string());
    }
    let key_str = bytes_to_string(&args[0])?;
    let key = prefix_key(db, &key_str);
    let seconds_str = bytes_to_string(&args[1])?;
    let seconds: i64 = seconds_str.parse().map_err(|_| "ERR value is not an integer or out of range".to_string())?;
    let success = storage.expire_seconds(&key, seconds);
    Ok(ScriptResult::Integer(if success { 1 } else { 0 }))
}

fn cmd_pexpire(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.len() != 2 {
        return Err("ERR wrong number of arguments for 'pexpire' command".to_string());
    }
    let key_str = bytes_to_string(&args[0])?;
    let key = prefix_key(db, &key_str);
    let millis_str = bytes_to_string(&args[1])?;
    let millis: i64 = millis_str.parse().map_err(|_| "ERR value is not an integer or out of range".to_string())?;
    let success = storage.expire_millis(&key, millis);
    Ok(ScriptResult::Integer(if success { 1 } else { 0 }))
}

fn cmd_persist(storage: &Arc<Storage>, db: u32, args: &[Vec<u8>]) -> Result<ScriptResult, String> {
    if args.len() != 1 {
        return Err("ERR wrong number of arguments for 'persist' command".to_string());
    }
    let key_str = bytes_to_string(&args[0])?;
    let key = prefix_key(db, &key_str);
    let success = storage.persist(&key);
    Ok(ScriptResult::Integer(if success { 1 } else { 0 }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sha1_computation() {
        let script = "return 1";
        let sha1 = ScriptCache::compute_sha1(script);
        assert_eq!(sha1.len(), 40);
        // SHA1 should be consistent
        assert_eq!(sha1, ScriptCache::compute_sha1(script));
    }

    #[test]
    fn test_script_cache() {
        let cache = ScriptCache::new();
        let script = "return KEYS[1]";
        let sha1 = cache.load(script);
        
        assert!(cache.get(&sha1).is_some());
        assert_eq!(cache.get(&sha1).unwrap(), script);
        assert_eq!(cache.exists(&[sha1.clone()]), vec![true]);
        
        cache.flush();
        assert!(cache.get(&sha1).is_none());
    }

    #[test]
    fn test_simple_script_execution() {
        let storage = Arc::new(Storage::new(None));
        let ctx = ScriptContext {
            storage,
            current_db: 0,
            keys: vec!["key1".to_string()],
            args: vec!["arg1".to_string()],
        };
        
        // Test returning integer
        let result = execute_script("return 42", ctx).unwrap();
        match result {
            ScriptResult::Integer(n) => assert_eq!(n, 42),
            _ => panic!("Expected integer"),
        }
    }

    #[test]
    fn test_script_with_keys_and_args() {
        let storage = Arc::new(Storage::new(None));
        let ctx = ScriptContext {
            storage,
            current_db: 0,
            keys: vec!["mykey".to_string()],
            args: vec!["myarg".to_string()],
        };
        
        // Test accessing KEYS
        let result = execute_script("return KEYS[1]", ctx).unwrap();
        match result {
            ScriptResult::BulkString(b) => assert_eq!(String::from_utf8(b).unwrap(), "mykey"),
            _ => panic!("Expected bulk string"),
        }
    }

    #[test]
    fn test_script_returning_array() {
        let storage = Arc::new(Storage::new(None));
        let ctx = ScriptContext {
            storage,
            current_db: 0,
            keys: vec![],
            args: vec![],
        };
        
        let result = execute_script("return {1, 2, 3}", ctx).unwrap();
        match result {
            ScriptResult::Array(arr) => {
                assert_eq!(arr.len(), 3);
            }
            _ => panic!("Expected array"),
        }
    }
}
