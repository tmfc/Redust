//! Lua scripting support for Redis-compatible EVAL/EVALSHA commands.

use dashmap::DashMap;
use mlua::{Lua, Value};
use sha1::{Digest, Sha1};
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

/// Execute a Lua script (simplified version without redis.call/pcall)
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

    // Create a minimal redis table
    let redis_table = lua.create_table().map_err(|e| format!("ERR {}", e))?;
    
    // redis.call stub - returns error for now
    let call_fn = lua.create_function(|_, _args: mlua::MultiValue| -> mlua::Result<Value> {
        Err(mlua::Error::RuntimeError("redis.call not yet implemented".to_string()))
    }).map_err(|e| format!("ERR {}", e))?;
    redis_table.set("call", call_fn).map_err(|e| format!("ERR {}", e))?;
    
    // redis.pcall stub - returns error table for now
    let pcall_fn = lua.create_function(|lua, _args: mlua::MultiValue| -> mlua::Result<Value> {
        let err_table = lua.create_table()?;
        err_table.set("err", "redis.pcall not yet implemented")?;
        Ok(Value::Table(err_table))
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
