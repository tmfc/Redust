//! ACL (Access Control List) 模块
//!
//! 实现 Redis 兼容的 ACL 访问控制功能，包括：
//! - 用户管理（创建、删除、列出用户）
//! - 命令权限控制（允许/禁止特定命令或命令类别）
//! - Key 权限控制（限制可访问的 key 模式）
//! - 频道权限控制（限制可访问的 Pub/Sub 频道）

use dashmap::DashMap;
use std::collections::HashSet;
use std::sync::Arc;

/// ACL 用户
#[derive(Debug, Clone)]
pub struct AclUser {
    /// 用户名
    pub name: String,
    /// 是否启用
    pub enabled: bool,
    /// 是否无密码用户
    pub nopass: bool,
    /// 密码哈希列表（SHA256 hex）
    pub passwords: HashSet<String>,
    /// 允许的命令（小写）
    pub allowed_commands: HashSet<String>,
    /// 禁止的命令（小写）
    pub denied_commands: HashSet<String>,
    /// 允许的命令类别
    pub allowed_categories: HashSet<String>,
    /// 禁止的命令类别
    pub denied_categories: HashSet<String>,
    /// 是否允许所有命令
    pub all_commands: bool,
    /// 允许的 key 模式
    pub key_patterns: Vec<String>,
    /// 是否允许所有 key
    pub all_keys: bool,
    /// 允许的频道模式
    pub channel_patterns: Vec<String>,
    /// 是否允许所有频道
    pub all_channels: bool,
}

impl Default for AclUser {
    fn default() -> Self {
        Self {
            name: String::new(),
            enabled: false,
            nopass: false,
            passwords: HashSet::new(),
            allowed_commands: HashSet::new(),
            denied_commands: HashSet::new(),
            allowed_categories: HashSet::new(),
            denied_categories: HashSet::new(),
            all_commands: false,
            key_patterns: Vec::new(),
            all_keys: false,
            channel_patterns: Vec::new(),
            all_channels: false,
        }
    }
}

impl AclUser {
    /// 创建新用户
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            ..Default::default()
        }
    }

    /// 创建默认用户（拥有所有权限）
    pub fn default_user() -> Self {
        Self {
            name: "default".to_string(),
            enabled: true,
            nopass: true,
            all_commands: true,
            all_keys: true,
            all_channels: true,
            ..Default::default()
        }
    }

    /// 应用 ACL 规则
    pub fn apply_rule(&mut self, rule: &str) -> Result<(), String> {
        let rule = rule.trim();
        
        match rule {
            // 用户状态
            "on" => self.enabled = true,
            "off" => self.enabled = false,
            "nopass" => {
                self.nopass = true;
                self.passwords.clear();
            }
            "reset" => {
                self.enabled = false;
                self.nopass = false;
                self.passwords.clear();
                self.allowed_commands.clear();
                self.denied_commands.clear();
                self.allowed_categories.clear();
                self.denied_categories.clear();
                self.all_commands = false;
                self.key_patterns.clear();
                self.all_keys = false;
                self.channel_patterns.clear();
                self.all_channels = false;
            }
            "resetkeys" => {
                self.key_patterns.clear();
                self.all_keys = false;
            }
            "resetchannels" => {
                self.channel_patterns.clear();
                self.all_channels = false;
            }
            "allkeys" => self.all_keys = true,
            "allchannels" => self.all_channels = true,
            "allcommands" => self.all_commands = true,
            "nocommands" => {
                self.all_commands = false;
                self.allowed_commands.clear();
                self.allowed_categories.clear();
                self.denied_categories.insert("all".to_string());
            }
            
            // 密码规则
            _ if rule.starts_with('>') => {
                let password = &rule[1..];
                let hash = sha256_hex(password);
                self.passwords.insert(hash);
                self.nopass = false;
            }
            _ if rule.starts_with('#') => {
                let hash = rule[1..].to_lowercase();
                if hash.len() != 64 || !hash.chars().all(|c| c.is_ascii_hexdigit()) {
                    return Err("ERR invalid password hash".to_string());
                }
                self.passwords.insert(hash);
                self.nopass = false;
            }
            _ if rule.starts_with('<') => {
                let password = &rule[1..];
                let hash = sha256_hex(password);
                self.passwords.remove(&hash);
            }
            _ if rule.starts_with('!') => {
                let hash = rule[1..].to_lowercase();
                self.passwords.remove(&hash);
            }
            
            // 命令规则
            _ if rule.starts_with("+@") => {
                let category = rule[2..].to_lowercase();
                if category == "all" {
                    self.all_commands = true;
                } else {
                    self.allowed_categories.insert(category.clone());
                    self.denied_categories.remove(&category);
                }
            }
            _ if rule.starts_with("-@") => {
                let category = rule[2..].to_lowercase();
                if category == "all" {
                    self.all_commands = false;
                    self.allowed_commands.clear();
                    self.allowed_categories.clear();
                }
                self.denied_categories.insert(category.clone());
                self.allowed_categories.remove(&category);
            }
            _ if rule.starts_with('+') => {
                let cmd = rule[1..].to_lowercase();
                self.allowed_commands.insert(cmd.clone());
                self.denied_commands.remove(&cmd);
            }
            _ if rule.starts_with('-') => {
                let cmd = rule[1..].to_lowercase();
                self.denied_commands.insert(cmd.clone());
                self.allowed_commands.remove(&cmd);
            }
            
            // Key 模式
            _ if rule.starts_with('~') => {
                let pattern = rule[1..].to_string();
                if pattern == "*" {
                    self.all_keys = true;
                } else {
                    self.key_patterns.push(pattern);
                }
            }
            
            // 频道模式
            _ if rule.starts_with('&') => {
                let pattern = rule[1..].to_string();
                if pattern == "*" {
                    self.all_channels = true;
                } else {
                    self.channel_patterns.push(pattern);
                }
            }
            
            _ => return Err(format!("ERR unknown ACL rule: {}", rule)),
        }
        
        Ok(())
    }

    /// 检查密码是否匹配
    pub fn check_password(&self, password: &str) -> bool {
        if self.nopass {
            return true;
        }
        let hash = sha256_hex(password);
        self.passwords.contains(&hash)
    }

    /// 检查是否允许执行命令
    pub fn can_execute_command(&self, command: &str) -> bool {
        let cmd = command.to_lowercase();
        
        // 显式禁止的命令
        if self.denied_commands.contains(&cmd) {
            return false;
        }
        
        // 显式允许的命令
        if self.allowed_commands.contains(&cmd) {
            return true;
        }
        
        // 允许所有命令
        if self.all_commands {
            return true;
        }
        
        // 检查命令类别
        let category = get_command_category(&cmd);
        if self.denied_categories.contains(category) || self.denied_categories.contains("all") {
            return false;
        }
        if self.allowed_categories.contains(category) || self.allowed_categories.contains("all") {
            return true;
        }
        
        false
    }

    /// 检查是否允许访问 key
    pub fn can_access_key(&self, key: &str) -> bool {
        if self.all_keys {
            return true;
        }
        
        for pattern in &self.key_patterns {
            if glob_match(pattern, key) {
                return true;
            }
        }
        
        false
    }

    /// 检查是否允许访问频道
    pub fn can_access_channel(&self, channel: &str) -> bool {
        if self.all_channels {
            return true;
        }
        
        for pattern in &self.channel_patterns {
            if glob_match(pattern, channel) {
                return true;
            }
        }
        
        false
    }

    /// 生成 ACL LIST 格式的字符串
    pub fn to_acl_string(&self) -> String {
        let mut parts = vec![format!("user {}", self.name)];
        
        // 状态
        parts.push(if self.enabled { "on".to_string() } else { "off".to_string() });
        
        // 密码
        if self.nopass {
            parts.push("nopass".to_string());
        } else {
            for hash in &self.passwords {
                parts.push(format!("#{}", hash));
            }
        }
        
        // 命令
        if self.all_commands {
            parts.push("+@all".to_string());
        } else {
            for cat in &self.allowed_categories {
                parts.push(format!("+@{}", cat));
            }
            for cat in &self.denied_categories {
                parts.push(format!("-@{}", cat));
            }
            for cmd in &self.allowed_commands {
                parts.push(format!("+{}", cmd));
            }
            for cmd in &self.denied_commands {
                parts.push(format!("-{}", cmd));
            }
        }
        
        // Keys
        if self.all_keys {
            parts.push("~*".to_string());
        } else {
            for pattern in &self.key_patterns {
                parts.push(format!("~{}", pattern));
            }
        }
        
        // Channels
        if self.all_channels {
            parts.push("&*".to_string());
        } else {
            for pattern in &self.channel_patterns {
                parts.push(format!("&{}", pattern));
            }
        }
        
        parts.join(" ")
    }
}

/// ACL 管理器
#[derive(Clone)]
pub struct AclManager {
    users: Arc<DashMap<String, AclUser>>,
}

impl AclManager {
    /// 创建新的 ACL 管理器
    pub fn new() -> Self {
        let manager = Self {
            users: Arc::new(DashMap::new()),
        };
        // 创建默认用户
        manager.users.insert("default".to_string(), AclUser::default_user());
        manager
    }

    /// 获取用户
    pub fn get_user(&self, name: &str) -> Option<AclUser> {
        self.users.get(name).map(|u| u.clone())
    }

    /// 设置用户（创建或更新）
    pub fn set_user(&self, name: &str, rules: &[&str]) -> Result<(), String> {
        let mut user = self.users
            .get(name)
            .map(|u| u.clone())
            .unwrap_or_else(|| AclUser::new(name));
        
        for rule in rules {
            user.apply_rule(rule)?;
        }
        
        self.users.insert(name.to_string(), user);
        Ok(())
    }

    /// 删除用户
    pub fn del_user(&self, name: &str) -> bool {
        if name == "default" {
            return false; // 不能删除默认用户
        }
        self.users.remove(name).is_some()
    }

    /// 列出所有用户
    pub fn list_users(&self) -> Vec<String> {
        self.users.iter().map(|e| e.to_acl_string()).collect()
    }

    /// 获取所有用户名
    pub fn user_names(&self) -> Vec<String> {
        self.users.iter().map(|e| e.key().clone()).collect()
    }

    /// 验证用户
    pub fn authenticate(&self, username: &str, password: &str) -> Result<AclUser, String> {
        let user = self.users.get(username)
            .ok_or_else(|| "WRONGPASS invalid username-password pair or user is disabled.".to_string())?;
        
        if !user.enabled {
            return Err("WRONGPASS invalid username-password pair or user is disabled.".to_string());
        }
        
        if !user.check_password(password) {
            return Err("WRONGPASS invalid username-password pair or user is disabled.".to_string());
        }
        
        Ok(user.clone())
    }
}

impl Default for AclManager {
    fn default() -> Self {
        Self::new()
    }
}

/// 计算 SHA256 哈希并返回十六进制字符串
fn sha256_hex(input: &str) -> String {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(input.as_bytes());
    let result = hasher.finalize();
    hex::encode(result)
}

/// 获取命令所属的类别
fn get_command_category(cmd: &str) -> &'static str {
    match cmd {
        // String 命令
        "set" | "get" | "mset" | "mget" | "incr" | "decr" | "incrby" | "decrby" 
        | "incrbyfloat" | "append" | "strlen" | "getset" | "getrange" | "setrange"
        | "setnx" | "setex" | "psetex" | "msetnx" | "getdel" | "getex" => "string",
        
        // List 命令
        "lpush" | "rpush" | "lpop" | "rpop" | "lrange" | "llen" | "lindex" | "lset"
        | "lrem" | "ltrim" | "linsert" | "rpoplpush" | "blpop" | "brpop" | "lpos" => "list",
        
        // Set 命令
        "sadd" | "srem" | "smembers" | "scard" | "sismember" | "sinter" | "sunion"
        | "sdiff" | "sinterstore" | "sunionstore" | "sdiffstore" | "spop" | "srandmember"
        | "smismember" | "smove" | "sscan" => "set",
        
        // Hash 命令
        "hset" | "hget" | "hmset" | "hmget" | "hgetall" | "hdel" | "hexists" | "hlen"
        | "hkeys" | "hvals" | "hincrby" | "hincrbyfloat" | "hsetnx" | "hstrlen" | "hscan" => "hash",
        
        // Sorted Set 命令
        "zadd" | "zrem" | "zscore" | "zrank" | "zrevrank" | "zrange" | "zrevrange"
        | "zrangebyscore" | "zrevrangebyscore" | "zcard" | "zcount" | "zincrby"
        | "zinter" | "zunion" | "zdiff" | "zinterstore" | "zunionstore" | "zdiffstore"
        | "zpopmin" | "zpopmax" | "zrangebylex" | "zrevrangebylex" | "zlexcount"
        | "zmscore" | "zscan" => "sortedset",
        
        // Pub/Sub 命令
        "publish" | "subscribe" | "unsubscribe" | "psubscribe" | "punsubscribe"
        | "pubsub" | "spublish" | "ssubscribe" | "sunsubscribe" => "pubsub",
        
        // 事务命令
        "multi" | "exec" | "discard" | "watch" | "unwatch" => "transaction",
        
        // 脚本命令
        "eval" | "evalsha" | "script" => "scripting",
        
        // 连接命令
        "ping" | "echo" | "quit" | "auth" | "select" => "connection",
        
        // 服务器命令
        "info" | "dbsize" | "flushdb" | "flushall" | "save" | "bgsave" | "lastsave"
        | "time" | "config" | "slowlog" | "client" | "command" => "server",
        
        // Key 命令
        "del" | "exists" | "expire" | "pexpire" | "expireat" | "pexpireat" | "ttl"
        | "pttl" | "persist" | "type" | "keys" | "scan" | "rename" | "renamenx"
        | "randomkey" | "copy" | "unlink" | "touch" | "object" => "keyspace",
        
        // 危险命令
        "debug" | "shutdown" | "module" => "dangerous",
        
        // ACL 命令
        "acl" => "admin",
        
        _ => "generic",
    }
}

/// 简单的 glob 模式匹配
fn glob_match(pattern: &str, text: &str) -> bool {
    let mut pattern_chars = pattern.chars().peekable();
    let mut text_chars = text.chars().peekable();
    
    glob_match_impl(&mut pattern_chars, &mut text_chars)
}

fn glob_match_impl(
    pattern: &mut std::iter::Peekable<std::str::Chars>,
    text: &mut std::iter::Peekable<std::str::Chars>,
) -> bool {
    while let Some(&p) = pattern.peek() {
        match p {
            '*' => {
                pattern.next();
                if pattern.peek().is_none() {
                    return true;
                }
                while text.peek().is_some() {
                    let mut p_clone = pattern.clone();
                    let mut t_clone = text.clone();
                    if glob_match_impl(&mut p_clone, &mut t_clone) {
                        return true;
                    }
                    text.next();
                }
                return false;
            }
            '?' => {
                pattern.next();
                if text.next().is_none() {
                    return false;
                }
            }
            _ => {
                if text.next() != Some(p) {
                    return false;
                }
                pattern.next();
            }
        }
    }
    text.peek().is_none()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_user() {
        let user = AclUser::default_user();
        assert!(user.enabled);
        assert!(user.nopass);
        assert!(user.all_commands);
        assert!(user.all_keys);
        assert!(user.all_channels);
    }

    #[test]
    fn test_apply_rules() {
        let mut user = AclUser::new("testuser");
        
        user.apply_rule("on").unwrap();
        assert!(user.enabled);
        
        user.apply_rule(">mypassword").unwrap();
        assert!(!user.nopass);
        assert!(user.check_password("mypassword"));
        assert!(!user.check_password("wrongpassword"));
        
        user.apply_rule("+get").unwrap();
        assert!(user.can_execute_command("get"));
        
        user.apply_rule("-set").unwrap();
        assert!(!user.can_execute_command("set"));
        
        user.apply_rule("~mykey:*").unwrap();
        assert!(user.can_access_key("mykey:123"));
        assert!(!user.can_access_key("otherkey"));
        
        user.apply_rule("&mychannel:*").unwrap();
        assert!(user.can_access_channel("mychannel:test"));
        assert!(!user.can_access_channel("otherchannel"));
    }

    #[test]
    fn test_acl_manager() {
        let manager = AclManager::new();
        
        // 默认用户存在
        let default_user = manager.get_user("default").unwrap();
        assert!(default_user.enabled);
        
        // 创建新用户
        manager.set_user("alice", &["on", ">secret", "+@all", "~*"]).unwrap();
        let alice = manager.get_user("alice").unwrap();
        assert!(alice.enabled);
        assert!(alice.check_password("secret"));
        
        // 验证用户
        let auth_result = manager.authenticate("alice", "secret");
        assert!(auth_result.is_ok());
        
        let auth_fail = manager.authenticate("alice", "wrong");
        assert!(auth_fail.is_err());
        
        // 删除用户
        assert!(manager.del_user("alice"));
        assert!(manager.get_user("alice").is_none());
        
        // 不能删除默认用户
        assert!(!manager.del_user("default"));
    }

    #[test]
    fn test_glob_match() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("foo*", "foobar"));
        assert!(glob_match("*bar", "foobar"));
        assert!(glob_match("foo*bar", "fooxyzbar"));
        assert!(glob_match("f?o", "foo"));
        assert!(!glob_match("f?o", "fooo"));
        assert!(glob_match("user:*:data", "user:123:data"));
    }
}
