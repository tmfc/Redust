use crate::hyperloglog::HyperLogLog;
use dashmap::DashMap;
use ordered_float::OrderedFloat;
use rand::prelude::SliceRandom;
use rand::{seq::IteratorRandom, thread_rng};
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::Path;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

type ByteString = Vec<u8>;

#[derive(Debug, Clone)]
struct ZSetInner {
    by_member: HashMap<String, f64>,
    by_score: BTreeSet<(OrderedFloat<f64>, String)>,
}

#[derive(Debug, Clone)]
enum StorageValue {
    String {
        value: ByteString,
        expires_at: Option<Instant>,
    },
    List {
        value: VecDeque<String>,
        expires_at: Option<Instant>,
    },
    Set {
        value: HashSet<String>,
        expires_at: Option<Instant>,
    },
    Hash {
        value: HashMap<String, String>,
        expires_at: Option<Instant>,
    },
    Zset {
        value: ZSetInner,
        expires_at: Option<Instant>,
    },
    HyperLogLog {
        value: HyperLogLog,
        expires_at: Option<Instant>,
    },
}

#[derive(Clone)]
pub struct Storage {
    data: Arc<DashMap<String, StorageValue>>,
    // 0 表示不限制
    maxmemory_bytes: Arc<AtomicU64>,
    last_access: Arc<DashMap<String, u64>>,
    access_counter: Arc<AtomicU64>,
    /// 全局版本计数器，每次写操作递增
    global_version: Arc<AtomicU64>,
    /// 每个 key 的版本号，用于 WATCH 机制
    key_versions: Arc<DashMap<String, u64>>,
}

// HINCRBY 专用错误类型，用于区分 WRONGTYPE / 非整数 / 溢出 / 超过 maxvalue 限制
pub enum HincrError {
    WrongType,
    NotInteger,
    Overflow,
    MaxValueExceeded,
}

pub enum HincrFloatError {
    WrongType,
    NotFloat,
    MaxValueExceeded,
}

pub enum ZsetError {
    WrongType,
    NotFloat,
}

pub enum LsetError {
    WrongType,
    NoSuchKey,
    OutOfRange,
}

/// ZINTER/ZUNION 的聚合方式
#[derive(Debug, Clone, Copy)]
pub enum ZsetAggregate {
    Sum,
    Min,
    Max,
}

impl Default for Storage {
    fn default() -> Self {
        Storage::new(None)
    }
}

impl Storage {
    pub fn new(maxmemory_bytes: Option<u64>) -> Self {
        let mm = maxmemory_bytes.unwrap_or(0);
        Storage {
            data: Arc::new(DashMap::new()),
            maxmemory_bytes: Arc::new(AtomicU64::new(mm)),
            last_access: Arc::new(DashMap::new()),
            access_counter: Arc::new(AtomicU64::new(0)),
            global_version: Arc::new(AtomicU64::new(0)),
            key_versions: Arc::new(DashMap::new()),
        }
    }

    /// 获取指定 key 的当前版本号
    pub fn get_key_version(&self, key: &str) -> u64 {
        self.key_versions.get(key).map(|v| *v).unwrap_or(0)
    }

    /// 递增指定 key 的版本号（在写操作后调用）
    pub fn bump_key_version(&self, key: &str) {
        let new_version = self.global_version.fetch_add(1, Ordering::SeqCst) + 1;
        self.key_versions.insert(key.to_string(), new_version);
    }

    pub fn flushdb(&self, db: u8) {
        let prefix = format!("{}:", db);
        let keys_to_remove: Vec<String> = self
            .data
            .iter()
            .map(|e| e.key().clone())
            .filter(|k| k.starts_with(&prefix))
            .collect();

        for k in &keys_to_remove {
            self.data.remove(k);
            self.last_access.remove(k);
            self.bump_key_version(k);
        }
    }

    pub fn flushall(&self) {
        // 先记录所有 key 并更新版本
        let all_keys: Vec<String> = self.data.iter().map(|e| e.key().clone()).collect();
        for k in &all_keys {
            self.bump_key_version(k);
        }
        self.data.clear();
        self.last_access.clear();
        self.access_counter.store(0, Ordering::Relaxed);
    }

    pub fn hset(&self, key: &str, field: &str, value: String) -> Result<usize, ()> {
        let now = Instant::now();
        self.remove_if_expired(key, now);

        let mut added = 0usize;

        if let Some(mut entry) = self.data.get_mut(key) {
            match entry.value_mut() {
                StorageValue::Hash { value: map, .. } => {
                    let existed = map.insert(field.to_string(), value).is_some();
                    if !existed {
                        added = 1;
                    }
                }
                _ => return Err(()),
            }
        } else {
            let mut map = HashMap::new();
            map.insert(field.to_string(), value);
            added = 1;
            self.data.insert(
                key.to_string(),
                StorageValue::Hash {
                    value: map,
                    expires_at: None,
                },
            );
        }

        // hset 总是修改 key（即使 field 已存在），需要更新版本
        self.touch_key(key);
        self.bump_key_version(key);
        if added > 0 {
            self.maybe_evict_for_write();
        }

        Ok(added)
    }

    pub fn hget(&self, key: &str, field: &str) -> Result<Option<String>, ()> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return Ok(None);
        }

        let result = {
            let Some(entry) = self.data.get(key) else {
                return Ok(None);
            };
            match entry.value() {
                StorageValue::Hash { value: map, .. } => map.get(field).cloned(),
                _ => return Err(()),
            }
        };

        if result.is_some() {
            self.touch_key(key);
        }

        Ok(result)
    }

    pub fn hkeys(&self, key: &str) -> Result<Vec<String>, ()> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return Ok(Vec::new());
        }

        let (mut keys, should_touch) = {
            let entry = self.data.get(key);
            let map = match entry.as_ref().map(|e| e.value()) {
                Some(StorageValue::Hash { value: m, .. }) => m,
                None => return Ok(Vec::new()),
                _ => return Err(()),
            };

            let keys: Vec<String> = map.keys().cloned().collect();
            let touch = !keys.is_empty();
            (keys, touch)
        };

        keys.sort();

        if should_touch {
            self.touch_key(key);
        }

        Ok(keys)
    }

    pub fn hvals(&self, key: &str) -> Result<Vec<String>, ()> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return Ok(Vec::new());
        }

        let (mut vals, should_touch) = {
            let entry = self.data.get(key);
            let map = match entry.as_ref().map(|e| e.value()) {
                Some(StorageValue::Hash { value: m, .. }) => m,
                None => return Ok(Vec::new()),
                _ => return Err(()),
            };

            let vals: Vec<String> = map.values().cloned().collect();
            let touch = !vals.is_empty();
            (vals, touch)
        };

        vals.sort();

        if should_touch {
            self.touch_key(key);
        }

        Ok(vals)
    }

    pub fn hmget(&self, key: &str, fields: &[String]) -> Result<Vec<Option<String>>, ()> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return Ok(vec![None; fields.len()]);
        }

        let entry = self.data.get(key);
        let map = match entry.as_ref().map(|e| e.value()) {
            Some(StorageValue::Hash { value: m, .. }) => m,
            None => return Ok(vec![None; fields.len()]),
            _ => return Err(()),
        };

        let mut result = Vec::with_capacity(fields.len());
        for f in fields {
            result.push(map.get(f).cloned());
        }

        if !result.is_empty() {
            self.touch_key(key);
        }

        Ok(result)
    }

    pub fn hincr_by(
        &self,
        key: &str,
        field: &str,
        delta: i64,
        max_value_bytes: Option<u64>,
    ) -> Result<i64, HincrError> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            // treat as non-existent hash
        }

        // Ensure the key exists and is a Hash
        let mut entry = self
            .data
            .entry(key.to_string())
            .or_insert(StorageValue::Hash {
                value: HashMap::new(),
                expires_at: None,
            });

        let map = match entry.value_mut() {
            StorageValue::Hash { value: map, .. } => map,
            _ => return Err(HincrError::WrongType),
        };

        // Default missing field to 0, then add delta
        let current_s = map.get(field).cloned().unwrap_or_else(|| "0".to_string());
        let current = current_s
            .parse::<i64>()
            .map_err(|_| HincrError::NotInteger)?;
        let new_val = current.checked_add(delta).ok_or(HincrError::Overflow)?;

        let new_str = new_val.to_string();
        if let Some(limit) = max_value_bytes {
            if (new_str.as_bytes().len() as u64) > limit {
                return Err(HincrError::MaxValueExceeded);
            }
        }

        map.insert(field.to_string(), new_str);

        self.touch_key(key);
        self.bump_key_version(key);
        self.maybe_evict_for_write();

        Ok(new_val)
    }

    pub fn hincr_by_float(
        &self,
        key: &str,
        field: &str,
        delta: f64,
        max_value_bytes: Option<u64>,
    ) -> Result<f64, HincrFloatError> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            // treat as non-existent hash
        }

        let mut entry = self
            .data
            .entry(key.to_string())
            .or_insert(StorageValue::Hash {
                value: HashMap::new(),
                expires_at: None,
            });

        let map = match entry.value_mut() {
            StorageValue::Hash { value: map, .. } => map,
            _ => return Err(HincrFloatError::WrongType),
        };

        let current_s = map.get(field).cloned().unwrap_or_else(|| "0".to_string());
        let current = current_s
            .parse::<f64>()
            .map_err(|_| HincrFloatError::NotFloat)?;
        let new_val = current + delta;
        if !new_val.is_finite() {
            return Err(HincrFloatError::NotFloat);
        }

        // 生成紧凑十进制表示，逻辑与 incr_by_float 保持一致
        let mut s = new_val.to_string();
        if s.contains('.') {
            while s.ends_with('0') {
                s.pop();
            }
            if s.ends_with('.') {
                s.push('0');
            }
        }

        if let Some(limit) = max_value_bytes {
            if (s.as_bytes().len() as u64) > limit {
                return Err(HincrFloatError::MaxValueExceeded);
            }
        }

        map.insert(field.to_string(), s);

        self.touch_key(key);
        self.bump_key_version(key);
        self.maybe_evict_for_write();

        Ok(new_val)
    }

    pub fn hlen(&self, key: &str) -> Result<usize, ()> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return Ok(0);
        }

        let Some(entry) = self.data.get(key) else {
            return Ok(0);
        };

        let len = if let StorageValue::Hash { value: map, .. } = entry.value() {
            map.len()
        } else {
            return Err(());
        };

        Ok(len)
    }

    pub fn hdel(&self, key: &str, fields: &[String]) -> Result<usize, ()> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return Ok(0);
        }

        let Some(mut entry) = self.data.get_mut(key) else {
            return Ok(0);
        };

        let removed = match entry.value_mut() {
            StorageValue::Hash { value: map, .. } => {
                let mut removed = 0;
                for f in fields {
                    if map.remove(f).is_some() {
                        removed += 1;
                    }
                }
                removed
            }
            _ => return Err(()),
        };

        if removed > 0 {
            self.touch_key(key);
            self.bump_key_version(key);
        }

        Ok(removed)
    }

    /// HSETNX: 仅当字段不存在时设置值
    /// 返回 Ok(1) 如果字段被设置，Ok(0) 如果字段已存在
    pub fn hsetnx(&self, key: &str, field: &str, value: String) -> Result<usize, ()> {
        let now = Instant::now();
        self.remove_if_expired(key, now);

        if let Some(mut entry) = self.data.get_mut(key) {
            match entry.value_mut() {
                StorageValue::Hash { value: map, .. } => {
                    if map.contains_key(field) {
                        return Ok(0); // 字段已存在，不设置
                    }
                    map.insert(field.to_string(), value);
                    self.touch_key(key);
                    self.bump_key_version(key);
                    self.maybe_evict_for_write();
                    Ok(1)
                }
                _ => Err(()), // WRONGTYPE
            }
        } else {
            // key 不存在，创建新的 Hash
            let mut map = HashMap::new();
            map.insert(field.to_string(), value);
            self.data.insert(
                key.to_string(),
                StorageValue::Hash {
                    value: map,
                    expires_at: None,
                },
            );
            self.touch_key(key);
            self.bump_key_version(key);
            self.maybe_evict_for_write();
            Ok(1)
        }
    }

    /// HSTRLEN: 获取字段值的字符串长度
    pub fn hstrlen(&self, key: &str, field: &str) -> Result<usize, ()> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return Ok(0);
        }

        let Some(entry) = self.data.get(key) else {
            return Ok(0);
        };

        match entry.value() {
            StorageValue::Hash { value: map, .. } => {
                let len = map.get(field).map(|v| v.len()).unwrap_or(0);
                if len > 0 {
                    self.touch_key(key);
                }
                Ok(len)
            }
            _ => Err(()), // WRONGTYPE
        }
    }

    /// HMSET: 批量设置多个字段
    pub fn hmset(&self, key: &str, field_values: &[(String, String)]) -> Result<(), ()> {
        let now = Instant::now();
        self.remove_if_expired(key, now);

        if let Some(mut entry) = self.data.get_mut(key) {
            match entry.value_mut() {
                StorageValue::Hash { value: map, .. } => {
                    for (field, value) in field_values {
                        map.insert(field.clone(), value.clone());
                    }
                }
                _ => return Err(()), // WRONGTYPE
            }
        } else {
            let mut map = HashMap::new();
            for (field, value) in field_values {
                map.insert(field.clone(), value.clone());
            }
            self.data.insert(
                key.to_string(),
                StorageValue::Hash {
                    value: map,
                    expires_at: None,
                },
            );
        }

        self.touch_key(key);
        self.bump_key_version(key);
        self.maybe_evict_for_write();
        Ok(())
    }

    pub fn spop(&self, key: &str, count: Option<i64>) -> Result<Vec<String>, ()> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return Ok(Vec::new());
        }

        let Some(mut entry) = self.data.get_mut(key) else {
            return Ok(Vec::new());
        };

        let set = match entry.value_mut() {
            StorageValue::Set { value: set, .. } => set,
            _ => return Err(()),
        };

        let mut result = Vec::new();
        let n = match count {
            None => 1,
            Some(c) => {
                if c <= 0 {
                    return Ok(Vec::new());
                }
                c as usize
            }
        };

        if n == 0 || set.is_empty() {
            return Ok(Vec::new());
        }

        let mut rng = thread_rng();
        for _ in 0..n {
            if let Some(chosen) = set.iter().choose(&mut rng).cloned() {
                set.remove(&chosen);
                result.push(chosen);
                if set.is_empty() {
                    break;
                }
            } else {
                break;
            }
        }

        if !result.is_empty() {
            self.touch_key(key);
            self.bump_key_version(key);
        }

        Ok(result)
    }

    pub fn srandmember(&self, key: &str, count: Option<i64>) -> Result<Vec<String>, ()> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return Ok(Vec::new());
        }

        let entry = self.data.get(key);
        let set = match entry.as_ref().map(|e| e.value()) {
            Some(StorageValue::Set { value: set, .. }) => set,
            None => return Ok(Vec::new()),
            _ => return Err(()),
        };

        if set.is_empty() {
            return Ok(Vec::new());
        }

        let mut rng = thread_rng();
        let mut result = Vec::new();

        match count {
            None => {
                if let Some(chosen) = set.iter().choose(&mut rng).cloned() {
                    result.push(chosen);
                }
            }
            Some(c) if c == 0 => {}
            Some(c) if c > 0 => {
                let n = c as usize;
                let mut candidates: Vec<String> = set.iter().cloned().collect();
                if n >= candidates.len() {
                    result = candidates;
                } else {
                    candidates.shuffle(&mut rng);
                    result.extend(candidates.into_iter().take(n));
                }
            }
            Some(c) => {
                let n = (-c) as usize;
                let elems: Vec<&String> = set.iter().collect();
                for _ in 0..n {
                    if let Some(chosen) = elems.iter().choose(&mut rng) {
                        result.push((*chosen).to_string());
                    }
                }
            }
        }

        Ok(result)
    }

    pub fn hexists(&self, key: &str, field: &str) -> Result<bool, ()> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return Ok(false);
        }

        let Some(entry) = self.data.get(key) else {
            return Ok(false);
        };

        let exists = if let StorageValue::Hash { value: map, .. } = entry.value() {
            map.contains_key(field)
        } else {
            return Err(());
        };

        Ok(exists)
    }

    pub fn hgetall(&self, key: &str) -> Result<Vec<(String, String)>, ()> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return Ok(Vec::new());
        }

        let (result, should_touch) = {
            let entry = self.data.get(key);
            let map = match entry.as_ref().map(|e| e.value()) {
                Some(StorageValue::Hash { value: m, .. }) => m,
                None => return Ok(Vec::new()),
                _ => return Err(()),
            };

            let vec: Vec<(String, String)> =
                map.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            let touch = !vec.is_empty();
            (vec, touch)
        };

        if should_touch {
            self.touch_key(key);
        }

        Ok(result)
    }

    pub fn set(&self, key: String, value: ByteString) {
        let now = Instant::now();
        self.remove_if_expired(&key, now);

        self.data
            .entry(key.clone())
            .and_modify(|existing| {
                if let StorageValue::String { value: v, expires_at } = existing {
                    *v = value.clone();
                    *expires_at = None;
                } else {
                    *existing = StorageValue::String {
                        value: value.clone(),
                        expires_at: None,
                    };
                }
            })
            .or_insert(StorageValue::String {
                value,
                expires_at: None,
            });
        self.touch_key(&key);
        self.bump_key_version(&key);
        self.maybe_evict_for_write();
    }

    pub fn get(&self, key: &str) -> Option<ByteString> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return None;
        }

        let result = {
            self.data.get(key).and_then(|entry| {
                if let StorageValue::String { value, .. } = entry.value() {
                    Some(value.clone())
                } else {
                    None // Key exists but is not a string
                }
            })
        };

        if result.is_some() {
            self.touch_key(key);
        }

        result
    }

    pub fn getdel(&self, key: &str) -> Result<Option<ByteString>, ()> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return Ok(None);
        }

        // 直接从 DashMap 中移除 key，避免在持有引用的同时再调用 remove
        let removed = self.data.remove(key);

        let Some((_k, value)) = removed else {
            return Ok(None);
        };

        let result = match value {
            StorageValue::String { value, .. } => Ok(Some(value)),
            _ => Err(()),
        }?;

        // 删除访问记录
        self.last_access.remove(key);
        self.bump_key_version(key);

        Ok(result)
    }

    pub fn getrange(&self, key: &str, start: isize, end: isize) -> Result<ByteString, ()> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return Ok(ByteString::new());
        }

        let Some(entry) = self.data.get(key) else {
            return Ok(ByteString::new());
        };

        let s = match entry.value() {
            StorageValue::String { value, .. } => value,
            _ => return Err(()),
        };

        let len = s.len() as isize;
        if len == 0 {
            return Ok(ByteString::new());
        }

        let mut start_idx = start;
        let mut end_idx = end;

        if start_idx < 0 {
            start_idx += len;
        }
        if end_idx < 0 {
            end_idx += len;
        }

        if start_idx < 0 {
            start_idx = 0;
        }
        if end_idx >= len {
            end_idx = len - 1;
        }

        if start_idx > end_idx || start_idx >= len {
            return Ok(ByteString::new());
        }

        let start_u = start_idx as usize;
        let end_u = end_idx as usize;
        let slice = &s[start_u..=end_u];

        Ok(slice.to_vec())
    }

    pub fn append(&self, key: &str, suffix: &[u8]) -> Result<usize, ()> {
        let now = Instant::now();
        self.remove_if_expired(key, now);
        let len = if let Some(mut entry) = self.data.get_mut(key) {
            match entry.value_mut() {
                StorageValue::String { value, .. } => {
                    value.extend_from_slice(suffix);
                    value.len()
                }
                _ => return Err(()),
            }
        } else {
            let value = suffix.to_vec();
            let len = value.len();
            self.data.insert(
                key.to_string(),
                StorageValue::String {
                    value,
                    expires_at: None,
                },
            );
            len
        };
        self.touch_key(key);
        self.bump_key_version(key);
        if len > 0 {
            self.maybe_evict_for_write();
        }

        Ok(len)
    }

    pub fn strlen(&self, key: &str) -> Result<usize, ()> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return Ok(0);
        }

        let Some(entry) = self.data.get(key) else {
            return Ok(0);
        };

        let len = match entry.value() {
            StorageValue::String { value, .. } => value.len(),
            _ => return Err(()),
        };

        Ok(len)
    }

    pub fn getset(&self, key: &str, value: ByteString) -> Result<Option<ByteString>, ()> {
        let now = Instant::now();
        self.remove_if_expired(key, now);

        let mut old: Option<ByteString> = None;

        if let Some(mut entry) = self.data.get_mut(key) {
            match entry.value_mut() {
                StorageValue::String { value: v, .. } => {
                    old = Some(v.clone());
                    *v = value;
                }
                _ => return Err(()),
            }
        } else {
            self.data.insert(
                key.to_string(),
                StorageValue::String {
                    value,
                    expires_at: None,
                },
            );
        }

        self.touch_key(key);
        self.bump_key_version(key);
        self.maybe_evict_for_write();

        Ok(old)
    }

    pub fn setrange(&self, key: &str, offset: usize, value: &[u8]) -> Result<usize, ()> {
        let now = Instant::now();
        self.remove_if_expired(key, now);

        let new_len = if let Some(mut entry) = self.data.get_mut(key) {
            match entry.value_mut() {
                StorageValue::String { value: s, .. } => {
                    let cur_len = s.len();
                    if offset > cur_len {
                        let pad_len = offset - cur_len;
                        s.resize(cur_len + pad_len, 0);
                    }
                    let target_len = offset + value.len();
                    if target_len > s.len() {
                        s.resize(target_len, 0);
                    }
                    s[offset..offset + value.len()].copy_from_slice(value);
                    s.len()
                }
                _ => return Err(()),
            }
        } else {
            let mut s = vec![0u8; offset];
            s.extend_from_slice(value);
            let len = s.len();
            self.data.insert(
                key.to_string(),
                StorageValue::String {
                    value: s,
                    expires_at: None,
                },
            );
            len
        };

        self.touch_key(key);
        self.bump_key_version(key);
        if new_len > 0 {
            self.maybe_evict_for_write();
        }

        Ok(new_len)
    }

    pub fn mget(&self, keys: &[String]) -> Vec<Option<ByteString>> {
        keys.iter().map(|k| self.get(k)).collect()
    }

    pub fn mset(&self, pairs: &[(String, ByteString)]) {
        for (k, v) in pairs {
            self.set(k.clone(), v.clone());
        }
    }

    pub fn msetnx(&self, pairs: &[(String, ByteString)]) -> bool {
        if pairs.is_empty() {
            return false;
        }

        let now = Instant::now();

        // 先检查是否有任意 key 已存在且未过期
        for (k, _v) in pairs.iter() {
            if self.remove_if_expired(k, now) {
                continue;
            }
            if self.data.contains_key(k) {
                return false;
            }
        }

        // 所有 key 均不存在/已过期，执行写入
        for (k, v) in pairs {
            self.set(k.clone(), v.clone());
        }

        true
    }

    pub fn setnx(&self, key: &str, value: ByteString) -> bool {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            // treat as non-existent
        }

        let mut inserted = false;
        self.data
            .entry(key.to_string())
            .and_modify(|_existing| {
                // key exists, do nothing
            })
            .or_insert_with(|| {
                inserted = true;
                StorageValue::String {
                    value,
                    expires_at: None,
                }
            });
        if inserted {
            self.touch_key(key);
            self.bump_key_version(key);
            self.maybe_evict_for_write();
        }
        inserted
    }

    pub fn set_with_expire_seconds(&self, key: String, value: ByteString, seconds: i64) {
        self.set(key.clone(), value);
        self.expire_seconds(&key, seconds);
    }

    pub fn set_with_expire_millis(&self, key: String, value: ByteString, millis: i64) {
        self.set(key.clone(), value);
        self.expire_millis(&key, millis);
    }

    pub fn del(&self, keys: &[String]) -> usize {
        let mut removed = 0;
        for key in keys {
            if self.data.remove(key).is_some() {
                self.last_access.remove(key);
                self.bump_key_version(key);
                removed += 1;
            }
        }
        removed
    }

    /// UNLINK: 与 DEL 相同（简化实现，实际 Redis 中是异步删除）
    pub fn unlink(&self, keys: &[String]) -> usize {
        self.del(keys)
    }

    /// COPY: 复制 key 到新 key
    /// 返回 Err(()) 表示 source 和 destination 相同（非法操作）
    pub fn copy(&self, source: &str, destination: &str, replace: bool) -> Result<bool, ()> {
        // Redis 语义：source 和 destination 相同时返回错误
        if source == destination {
            return Err(());
        }

        let now = Instant::now();
        if self.remove_if_expired(source, now) {
            return Ok(false); // source 不存在
        }

        let entry = match self.data.get(source) {
            Some(e) => e,
            None => return Ok(false),
        };

        // 检查 destination 是否存在
        self.remove_if_expired(destination, now);
        if self.data.contains_key(destination) && !replace {
            return Ok(false);
        }

        // 克隆值
        let cloned_value = entry.value().clone();
        drop(entry);

        // 如果 replace，先删除
        if replace {
            self.data.remove(destination);
        }

        self.data.insert(destination.to_string(), cloned_value);
        self.touch_key(destination);
        self.bump_key_version(destination);
        Ok(true)
    }

    /// TOUCH: 更新 key 的访问时间，返回存在的 key 数量
    pub fn touch(&self, keys: &[String]) -> usize {
        let now = Instant::now();
        let mut count = 0;
        for key in keys {
            if !self.remove_if_expired(key, now) && self.data.contains_key(key) {
                self.touch_key(key);
                count += 1;
            }
        }
        count
    }

    /// OBJECT ENCODING: 返回 key 的编码类型
    pub fn object_encoding(&self, key: &str) -> Option<&'static str> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return None;
        }

        let entry = self.data.get(key)?;
        let encoding = match entry.value() {
            StorageValue::String { .. } => "embstr",
            StorageValue::List { .. } => "quicklist",
            StorageValue::Set { .. } => "hashtable",
            StorageValue::Hash { .. } => "hashtable",
            StorageValue::Zset { .. } => "skiplist",
            StorageValue::HyperLogLog { .. } => "raw",
        };
        Some(encoding)
    }

    pub fn exists(&self, keys: &[String]) -> usize {
        let now = Instant::now();
        keys.iter()
            .filter(|k| !self.remove_if_expired(k, now) && self.data.contains_key(*k))
            .count()
    }

    pub fn rename(&self, from: &str, to: &str) -> Result<(), ()> {
        if from == to {
            // Redis: RENAME key key 也是 OK，不做任何修改
            if !self.data.contains_key(from) {
                return Err(());
            }
            return Ok(());
        }

        let now = Instant::now();
        if self.remove_if_expired(from, now) {
            return Err(());
        }

        let removed = self.data.remove(from);
        let Some((_k, value)) = removed else {
            return Err(());
        };

        // 删除旧的访问记录
        self.last_access.remove(from);
        self.bump_key_version(from);

        // 插入新 key，保留 TTL 信息
        self.data.insert(to.to_string(), value);
        self.touch_key(to);
        self.bump_key_version(to);

        Ok(())
    }

    pub fn renamenx(&self, from: &str, to: &str) -> Result<bool, ()> {
        let now = Instant::now();
        if from == to {
            // RENAME NX key key：如果 key 存在（且未过期），视为目标存在，返回 0；不存在则报错
            if self.remove_if_expired(from, now) {
                return Err(());
            }
            return if self.data.contains_key(from) {
                Ok(false)
            } else {
                Err(())
            };
        }

        if self.remove_if_expired(from, now) {
            return Err(());
        }

        // 目标 key 也需要尊重过期语义：已过期的 key 视为不存在
        self.remove_if_expired(to, now);

        if self.data.contains_key(to) {
            return Ok(false);
        }

        let removed = self.data.remove(from);
        let Some((_k, value)) = removed else {
            return Err(());
        };

        self.last_access.remove(from);
        self.bump_key_version(from);
        self.data.insert(to.to_string(), value);
        self.touch_key(to);
        self.bump_key_version(to);

        Ok(true)
    }

    pub fn incr(&self, key: &str) -> Result<i64, ()> {
        self.incr_by(key, 1)
    }

    pub fn decr(&self, key: &str) -> Result<i64, ()> {
        self.incr_by(key, -1)
    }

    pub fn incr_by(&self, key: &str, delta: i64) -> Result<i64, ()> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            // Treat as non-existent and start from 0
            self.data.insert(
                key.to_string(),
                StorageValue::String {
                    value: b"0".to_vec(),
                    expires_at: None,
                },
            );
        }

        let mut entry = self
            .data
            .entry(key.to_string())
            .or_insert(StorageValue::String {
                value: b"0".to_vec(),
                expires_at: None,
            });

        let current_val = match entry.value_mut() {
            StorageValue::String { value, .. } => value,
            _ => return Err(()), // Key exists but is not a string
        };

        let current_str = std::str::from_utf8(current_val).map_err(|_| ())?;
        let value: i64 = current_str.parse().map_err(|_| ())?;
        let new_val = value.checked_add(delta).ok_or(())?;
        *current_val = new_val.to_string().into_bytes();
        self.touch_key(key);
        self.bump_key_version(key);
        self.maybe_evict_for_write();
        Ok(new_val)
    }

    pub fn incr_by_float(&self, key: &str, delta: f64) -> Result<f64, ()> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            self.data.insert(
                key.to_string(),
                StorageValue::String {
                    value: b"0".to_vec(),
                    expires_at: None,
                },
            );
        }

        let mut entry = self
            .data
            .entry(key.to_string())
            .or_insert(StorageValue::String {
                value: b"0".to_vec(),
                expires_at: None,
            });

        let current_val = match entry.value_mut() {
            StorageValue::String { value, .. } => value,
            _ => return Err(()),
        };

        let current_str = std::str::from_utf8(current_val).map_err(|_| ())?;
        let current: f64 = current_str.parse().map_err(|_| ())?;
        let new_val = current + delta;
        if !new_val.is_finite() {
            return Err(());
        }

        // 尽量生成紧凑的人类可读十进制表示，接近 Redis 行为
        let mut s = new_val.to_string();
        if s.contains('.') {
            while s.ends_with('0') {
                s.pop();
            }
            if s.ends_with('.') {
                s.push('0');
            }
        }

        *current_val = s.into_bytes();
        self.touch_key(key);
        self.bump_key_version(key);
        self.maybe_evict_for_write();
        Ok(new_val)
    }

    pub fn type_of(&self, key: &str) -> String {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return "none".to_string();
        }

        self.data.get(key).map_or_else(
            || "none".to_string(),
            |entry| match entry.value() {
                StorageValue::String { .. } => "string".to_string(),
                StorageValue::List { .. } => "list".to_string(),
                StorageValue::Set { .. } => "set".to_string(),
                StorageValue::Hash { .. } => "hash".to_string(),
                StorageValue::Zset { .. } => "zset".to_string(),
                StorageValue::HyperLogLog { .. } => "string".to_string(), // Redis 中 HLL 的类型显示为 string
            },
        )
    }

    pub fn keys(&self, _pattern: &str) -> Vec<String> {
        // 当前实现：忽略 pattern，仅负责返回所有未过期的物理 key，排序后交给上层做模式匹配。
        // 这样可以复用同一 API 支撑 KEYS/SCAN/INFO 等调用场景。
        let now = Instant::now();
        let mut all: Vec<String> = self
            .data
            .iter()
            .map(|entry| entry.key().clone())
            .filter(|k| !self.remove_if_expired(k, now))
            .collect();
        all.sort();
        all
    }

    pub fn lpush(&self, key: &str, values: &[String]) -> Result<usize, ()> {
        self.push_internal(key, values, true)
    }

    pub fn rpush(&self, key: &str, values: &[String]) -> Result<usize, ()> {
        self.push_internal(key, values, false)
    }

    fn push_internal(&self, key: &str, values: &[String], left: bool) -> Result<usize, ()> {
        let now = Instant::now();
        self.remove_if_expired(key, now);

        let len;

        if let Some(mut entry) = self.data.get_mut(key) {
            match entry.value_mut() {
                StorageValue::List { value: list, .. } => {
                    for v in values {
                        if left {
                            list.push_front(v.clone());
                        } else {
                            list.push_back(v.clone());
                        }
                    }
                    len = list.len();
                }
                _ => return Err(()),
            }
        } else {
            let mut list = VecDeque::new();
            for v in values {
                if left {
                    list.push_front(v.clone());
                } else {
                    list.push_back(v.clone());
                }
            }
            len = list.len();
            self.data.insert(
                key.to_string(),
                StorageValue::List {
                    value: list,
                    expires_at: None,
                },
            );
        }

        self.touch_key(key);
        self.bump_key_version(key);
        if len > 0 {
            self.maybe_evict_for_write();
        }

        Ok(len)
    }

    pub fn lrange(&self, key: &str, start: isize, stop: isize) -> Result<Vec<String>, ()> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return Ok(Vec::new());
        }

        let entry = self.data.get(key);
        let list = match entry.as_ref().map(|e| e.value()) {
            Some(StorageValue::List { value, .. }) => value,
            None => return Ok(Vec::new()),
            _ => return Err(()),
        };

        let len = list.len() as isize;
        if len == 0 {
            return Ok(Vec::new());
        }

        let mut s = start;
        let mut e = stop;

        if s < 0 {
            s += len;
        }
        if e < 0 {
            e += len;
        }

        if s < 0 {
            s = 0;
        }
        if e >= len {
            e = len - 1;
        }

        if s > e || s >= len {
            return Ok(Vec::new());
        }

        let start_idx = s as usize;
        let end_idx = e as usize;

        let result: Vec<String> = list
            .iter()
            .skip(start_idx)
            .take(end_idx - start_idx + 1)
            .cloned()
            .collect();

        if !result.is_empty() {
            self.touch_key(key);
        }

        Ok(result)
    }

    pub fn lpop(&self, key: &str) -> Result<Option<String>, ()> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return Ok(None);
        }

        let Some(mut entry) = self.data.get_mut(key) else {
            return Ok(None);
        };

        let result = match entry.value_mut() {
            StorageValue::List { value: list, .. } => list.pop_front(),
            _ => return Err(()),
        };

        if result.is_some() {
            self.bump_key_version(key);
        }

        Ok(result)
    }

    pub fn rpop(&self, key: &str) -> Result<Option<String>, ()> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return Ok(None);
        }

        let Some(mut entry) = self.data.get_mut(key) else {
            return Ok(None);
        };

        let result = match entry.value_mut() {
            StorageValue::List { value: list, .. } => list.pop_back(),
            _ => return Err(()),
        };

        if result.is_some() {
            self.bump_key_version(key);
        }

        Ok(result)
    }

    pub fn llen(&self, key: &str) -> Result<usize, ()> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return Ok(0);
        }

        let Some(entry) = self.data.get(key) else {
            return Ok(0);
        };

        let len = if let StorageValue::List { value: list, .. } = entry.value() {
            list.len()
        } else {
            return Err(());
        };

        Ok(len)
    }

    pub fn lindex(&self, key: &str, index: isize) -> Result<Option<String>, ()> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return Ok(None);
        }

        let Some(entry) = self.data.get(key) else {
            return Ok(None);
        };

        let list = match entry.value() {
            StorageValue::List { value: list, .. } => list,
            _ => return Err(()),
        };

        let len = list.len() as isize;
        if len == 0 {
            return Ok(None);
        }

        let mut idx = index;
        if idx < 0 {
            idx += len;
        }
        if idx < 0 || idx >= len {
            return Ok(None);
        }

        Ok(list.get(idx as usize).cloned())
    }

    pub fn lrem(&self, key: &str, count: isize, value: &str) -> Result<usize, ()> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return Ok(0);
        }

        let Some(mut entry) = self.data.get_mut(key) else {
            return Ok(0);
        };

        let list = match entry.value_mut() {
            StorageValue::List { value: list, .. } => list,
            _ => return Err(()),
        };

        if list.is_empty() {
            return Ok(0);
        }

        // Implement Redis-like LREM semantics.
        let mut removed = 0usize;

        if count == 0 {
            // remove all occurrences
            let original_len = list.len();
            list.retain(|v| v != value);
            removed = original_len - list.len();
        } else if count > 0 {
            // from head to tail, remove up to count
            let mut remaining = count as usize;
            let mut i = 0;
            while i < list.len() && remaining > 0 {
                if list[i] == value {
                    list.remove(i);
                    removed += 1;
                    remaining -= 1;
                    // do not advance i; list has shifted
                } else {
                    i += 1;
                }
            }
        } else {
            // count < 0: from tail to head, remove up to |count|
            let mut remaining = (-count) as usize;
            let mut i = list.len();
            while i > 0 && remaining > 0 {
                i -= 1;
                if list[i] == value {
                    list.remove(i);
                    removed += 1;
                    remaining -= 1;
                }
            }
        }

        if removed > 0 {
            self.touch_key(key);
            self.bump_key_version(key);
        }

        Ok(removed)
    }

    pub fn ltrim(&self, key: &str, start: isize, stop: isize) -> Result<(), ()> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return Ok(());
        }

        let Some(mut entry) = self.data.get_mut(key) else {
            // key does not exist: Redis treats as no-op
            return Ok(());
        };

        let list = match entry.value_mut() {
            StorageValue::List { value: list, .. } => list,
            _ => return Err(()), // WRONGTYPE
        };

        if list.is_empty() {
            return Ok(());
        }

        let len = list.len() as isize;
        let mut s = start;
        let mut e = stop;

        if s < 0 {
            s += len;
        }
        if e < 0 {
            e += len;
        }

        if s < 0 {
            s = 0;
        }
        if e >= len {
            e = len - 1;
        }

        if s > e || s >= len {
            // Result is empty list
            list.clear();
            self.touch_key(key);
            self.bump_key_version(key);
            return Ok(());
        }

        let start_idx = s as usize;
        let end_idx = e as usize;

        // Keep only [start_idx, end_idx]
        // First, drop elements after end_idx
        if end_idx + 1 < list.len() {
            list.truncate(end_idx + 1);
        }
        // Then, drop elements before start_idx by draining
        if start_idx > 0 {
            list.drain(0..start_idx);
        }

        self.touch_key(key);
        self.bump_key_version(key);
        // ltrim 只会收缩列表，不会增加内存，这里不触发淘汰

        Ok(())
    }

    /// LSET: 设置指定索引位置的元素
    /// 返回 Ok(()) 成功，Err(LsetError) 失败
    pub fn lset(&self, key: &str, index: isize, value: String) -> Result<(), LsetError> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return Err(LsetError::NoSuchKey);
        }

        let Some(mut entry) = self.data.get_mut(key) else {
            return Err(LsetError::NoSuchKey);
        };

        let list = match entry.value_mut() {
            StorageValue::List { value: list, .. } => list,
            _ => return Err(LsetError::WrongType),
        };

        if list.is_empty() {
            return Err(LsetError::OutOfRange);
        }

        let len = list.len() as isize;
        let idx = if index < 0 { len + index } else { index };

        if idx < 0 || idx >= len {
            return Err(LsetError::OutOfRange);
        }

        list[idx as usize] = value;
        self.touch_key(key);
        self.bump_key_version(key);
        Ok(())
    }

    /// LINSERT: 在 pivot 元素前/后插入新元素
    /// 返回插入后列表长度，-1 表示 pivot 未找到，Err 表示类型错误
    pub fn linsert(&self, key: &str, before: bool, pivot: &str, value: String) -> Result<isize, ()> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return Ok(0); // key 不存在返回 0
        }

        let Some(mut entry) = self.data.get_mut(key) else {
            return Ok(0);
        };

        let list = match entry.value_mut() {
            StorageValue::List { value: list, .. } => list,
            _ => return Err(()),
        };

        // 查找 pivot 位置
        let pos = list.iter().position(|v| v == pivot);
        match pos {
            Some(idx) => {
                let insert_idx = if before { idx } else { idx + 1 };
                list.insert(insert_idx, value);
                self.touch_key(key);
                self.bump_key_version(key);
                self.maybe_evict_for_write();
                Ok(list.len() as isize)
            }
            None => Ok(-1), // pivot 未找到
        }
    }

    /// RPOPLPUSH: 从 source 右侧弹出并推入 destination 左侧
    pub fn rpoplpush(&self, source: &str, destination: &str) -> Result<Option<String>, ()> {
        let now = Instant::now();
        self.remove_if_expired(source, now);
        self.remove_if_expired(destination, now);

        // 先从 source 弹出
        let popped = {
            let Some(mut entry) = self.data.get_mut(source) else {
                return Ok(None);
            };
            let list = match entry.value_mut() {
                StorageValue::List { value: list, .. } => list,
                _ => return Err(()),
            };
            list.pop_back()
        };

        let Some(value) = popped else {
            return Ok(None);
        };

        self.touch_key(source);
        self.bump_key_version(source);

        // 推入 destination 左侧
        if let Some(mut entry) = self.data.get_mut(destination) {
            match entry.value_mut() {
                StorageValue::List { value: list, .. } => {
                    list.push_front(value.clone());
                }
                _ => return Err(()),
            }
        } else {
            let mut list = VecDeque::new();
            list.push_front(value.clone());
            self.data.insert(
                destination.to_string(),
                StorageValue::List {
                    value: list,
                    expires_at: None,
                },
            );
        }

        self.touch_key(destination);
        self.bump_key_version(destination);
        self.maybe_evict_for_write();

        Ok(Some(value))
    }

    /// LPOS: 查找元素位置
    /// 返回匹配的索引列表（如果 count 为 None 则返回第一个匹配）
    pub fn lpos(
        &self,
        key: &str,
        element: &str,
        rank: Option<isize>,
        count: Option<isize>,
        maxlen: Option<isize>,
    ) -> Result<Vec<isize>, ()> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return Ok(Vec::new());
        }

        let Some(entry) = self.data.get(key) else {
            return Ok(Vec::new());
        };

        let list = match entry.value() {
            StorageValue::List { value: list, .. } => list,
            _ => return Err(()),
        };

        if list.is_empty() {
            return Ok(Vec::new());
        }

        let len = list.len();
        let max = maxlen.map(|m| m as usize).unwrap_or(len).min(len);
        let rank_val = rank.unwrap_or(1);
        let want_count = count.map(|c| c as usize).unwrap_or(1);

        let mut results = Vec::new();
        let mut matches_found = 0isize;

        if rank_val > 0 {
            // 从头到尾搜索
            for (i, v) in list.iter().enumerate().take(max) {
                if v == element {
                    matches_found += 1;
                    if matches_found >= rank_val {
                        results.push(i as isize);
                        if count.is_some() && results.len() >= want_count {
                            break;
                        }
                        if count.is_none() {
                            break; // 只返回第一个
                        }
                    }
                }
            }
        } else {
            // 从尾到头搜索
            let skip = if max < len { len - max } else { 0 };
            for i in (skip..len).rev() {
                if list[i] == element {
                    matches_found += 1;
                    if matches_found >= -rank_val {
                        results.push(i as isize);
                        if count.is_some() && results.len() >= want_count {
                            break;
                        }
                        if count.is_none() {
                            break;
                        }
                    }
                }
            }
        }

        if !results.is_empty() {
            self.touch_key(key);
        }

        Ok(results)
    }

    pub fn sadd(&self, key: &str, members: &[String]) -> Result<usize, ()> {
        let now = Instant::now();
        self.remove_if_expired(key, now);

        let mut added = 0usize;

        if let Some(mut entry) = self.data.get_mut(key) {
            match entry.value_mut() {
                StorageValue::Set { value: set, .. } => {
                    for m in members {
                        if set.insert(m.clone()) {
                            added += 1;
                        }
                    }
                }
                _ => return Err(()),
            }
        } else {
            let mut set = HashSet::new();
            for m in members {
                if set.insert(m.clone()) {
                    added += 1;
                }
            }
            self.data.insert(
                key.to_string(),
                StorageValue::Set {
                    value: set,
                    expires_at: None,
                },
            );
        }

        if added > 0 {
            self.touch_key(key);
            self.bump_key_version(key);
            self.maybe_evict_for_write();
        }

        Ok(added)
    }

    pub fn srem(&self, key: &str, members: &[String]) -> Result<usize, ()> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return Ok(0);
        }

        let removed = if let Some(mut entry) = self.data.get_mut(key) {
            match entry.value_mut() {
                StorageValue::Set { value: set, .. } => {
                    let mut removed = 0;
                    for m in members {
                        if set.remove(m) {
                            removed += 1;
                        }
                    }
                    removed
                }
                _ => return Err(()),
            }
        } else {
            0
        };

        if removed > 0 {
            self.touch_key(key);
            self.bump_key_version(key);
        }

        Ok(removed)
    }

    pub fn smembers(&self, key: &str) -> Result<Vec<String>, ()> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return Ok(Vec::new());
        }

        let (mut members, should_touch) = {
            let entry = self.data.get(key);
            let set = match entry.as_ref().map(|e| e.value()) {
                Some(StorageValue::Set { value, .. }) => value,
                None => return Ok(Vec::new()),
                _ => return Err(()),
            };

            let members: Vec<String> = set.iter().cloned().collect();
            let touch = !members.is_empty();
            (members, touch)
        };

        members.sort();

        if should_touch {
            self.touch_key(key);
        }

        Ok(members)
    }

    pub fn dbsize(&self) -> usize {
        let now = Instant::now();
        let keys: Vec<String> = self.data.iter().map(|entry| entry.key().clone()).collect();

        keys.into_iter()
            .filter(|k| !self.remove_if_expired(k, now))
            .count()
    }

    pub fn scard(&self, key: &str) -> Result<usize, ()> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return Ok(0);
        }

        let Some(entry) = self.data.get(key) else {
            return Ok(0);
        };

        let len = if let StorageValue::Set { value: set, .. } = entry.value() {
            set.len()
        } else {
            return Err(());
        };

        Ok(len)
    }

    pub fn sismember(&self, key: &str, member: &str) -> Result<bool, ()> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return Ok(false);
        }

        let Some(entry) = self.data.get(key) else {
            return Ok(false);
        };

        let exists = if let StorageValue::Set { value: set, .. } = entry.value() {
            set.contains(member)
        } else {
            return Err(());
        };

        Ok(exists)
    }

    pub fn sunion(&self, keys: &[String]) -> Result<Vec<String>, ()> {
        let mut result: HashSet<String> = HashSet::new();
        for key in keys {
            let now = Instant::now();
            if self.remove_if_expired(key, now) {
                continue;
            }

            if let Some(entry) = self.data.get(key) {
                match entry.value() {
                    StorageValue::Set { value: set, .. } => {
                        for m in set {
                            result.insert(m.clone());
                        }
                    }
                    _ => return Err(()),
                }
            }
        }

        let mut members: Vec<String> = result.into_iter().collect();
        members.sort();
        Ok(members)
    }

    fn set_store_result(&self, dest: &str, members: HashSet<String>) -> usize {
        let len = members.len();
        self.data.insert(
            dest.to_string(),
            StorageValue::Set {
                value: members,
                expires_at: None,
            },
        );
        self.touch_key(dest);
        self.bump_key_version(dest);
        if len > 0 {
            self.maybe_evict_for_write();
        }
        len
    }

    pub fn sunionstore(&self, dest: &str, keys: &[String]) -> Result<usize, ()> {
        let now = Instant::now();
        let mut result: HashSet<String> = HashSet::new();
        for key in keys {
            if self.remove_if_expired(key, now) {
                continue;
            }

            if let Some(entry) = self.data.get(key) {
                match entry.value() {
                    StorageValue::Set { value: set, .. } => {
                        for m in set {
                            result.insert(m.clone());
                        }
                    }
                    _ => return Err(()),
                }
            }
        }

        Ok(self.set_store_result(dest, result))
    }

    pub fn sinterstore(&self, dest: &str, keys: &[String]) -> Result<usize, ()> {
        let now = Instant::now();
        let mut iter = keys.iter();
        let Some(first_key) = iter.next() else {
            return Ok(0);
        };

        if self.remove_if_expired(first_key, now) {
            return Ok(self.set_store_result(dest, HashSet::new()));
        }

        let mut result: HashSet<String> = match self.data.get(first_key) {
            Some(entry) => match entry.value() {
                StorageValue::Set { value: set, .. } => set.iter().cloned().collect(),
                _ => return Err(()),
            },
            None => HashSet::new(),
        };

        for key in iter {
            if self.remove_if_expired(key, now) {
                result.clear();
                break;
            }

            if let Some(entry) = self.data.get(key) {
                match entry.value() {
                    StorageValue::Set { value: set, .. } => {
                        result.retain(|m| set.contains(m));
                    }
                    _ => return Err(()),
                }
            } else {
                result.clear();
                break;
            }
        }

        Ok(self.set_store_result(dest, result))
    }

    pub fn sdiffstore(&self, dest: &str, keys: &[String]) -> Result<usize, ()> {
        let now = Instant::now();
        let mut iter = keys.iter();
        let Some(first_key) = iter.next() else {
            return Ok(0);
        };

        if self.remove_if_expired(first_key, now) {
            return Ok(self.set_store_result(dest, HashSet::new()));
        }

        let mut result: HashSet<String> = match self.data.get(first_key) {
            Some(entry) => match entry.value() {
                StorageValue::Set { value: set, .. } => set.iter().cloned().collect(),
                _ => return Err(()),
            },
            None => HashSet::new(),
        };

        for key in iter {
            if self.remove_if_expired(key, now) {
                continue;
            }

            if let Some(entry) = self.data.get(key) {
                match entry.value() {
                    StorageValue::Set { value: set, .. } => {
                        result.retain(|m| !set.contains(m));
                    }
                    _ => return Err(()),
                }
            }
        }

        Ok(self.set_store_result(dest, result))
    }

    /// SMOVE: 将 member 从 source 移动到 destination
    /// 返回 Ok(true) 表示移动成功，Ok(false) 表示 member 不在 source 中
    /// 返回 Err(()) 表示类型错误
    pub fn smove(&self, source: &str, destination: &str, member: &str) -> Result<bool, ()> {
        let now = Instant::now();
        self.remove_if_expired(source, now);
        self.remove_if_expired(destination, now);

        // 检查 source 是否存在且包含 member
        let removed = {
            let Some(mut entry) = self.data.get_mut(source) else {
                return Ok(false);
            };
            match entry.value_mut() {
                StorageValue::Set { value: set, .. } => {
                    if !set.remove(member) {
                        return Ok(false);
                    }
                    true
                }
                _ => return Err(()),
            }
        };

        if !removed {
            return Ok(false);
        }

        self.bump_key_version(source);

        // 添加到 destination
        match self.data.get_mut(destination) {
            Some(mut entry) => match entry.value_mut() {
                StorageValue::Set { value: set, .. } => {
                    set.insert(member.to_string());
                }
                _ => return Err(()),
            },
            None => {
                let mut set = HashSet::new();
                set.insert(member.to_string());
                self.data.insert(
                    destination.to_string(),
                    StorageValue::Set {
                        value: set,
                        expires_at: None,
                    },
                );
            }
        }

        self.touch_key(destination);
        self.bump_key_version(destination);
        Ok(true)
    }

    pub fn zadd(&self, key: &str, entries: &[(f64, String)]) -> Result<usize, ZsetError> {
        let now = Instant::now();
        self.remove_if_expired(key, now);

        for (score, _) in entries {
            if !score.is_finite() {
                return Err(ZsetError::NotFloat);
            }
        }

        let mut added = 0usize;

        if let Some(mut entry) = self.data.get_mut(key) {
            match entry.value_mut() {
                StorageValue::Zset { value, .. } => {
                    for (score, member) in entries {
                        let ord_score = OrderedFloat(*score);
                        if let Some(old_score) = value.by_member.insert(member.clone(), *score) {
                            let _ = value
                                .by_score
                                .remove(&(OrderedFloat(old_score), member.clone()));
                        } else {
                            added += 1;
                        }
                        value.by_score.insert((ord_score, member.clone()));
                    }
                }
                _ => return Err(ZsetError::WrongType),
            }
        } else {
            let mut inner = ZSetInner {
                by_member: HashMap::new(),
                by_score: BTreeSet::new(),
            };
            for (score, member) in entries {
                let ord_score = OrderedFloat(*score);
                inner.by_member.insert(member.clone(), *score);
                inner.by_score.insert((ord_score, member.clone()));
                added += 1;
            }
            self.data.insert(
                key.to_string(),
                StorageValue::Zset {
                    value: inner,
                    expires_at: None,
                },
            );
        }

        if !entries.is_empty() {
            self.touch_key(key);
            self.bump_key_version(key);
            self.maybe_evict_for_write();
        }

        Ok(added)
    }

    pub fn zcard(&self, key: &str) -> Result<usize, ()> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return Ok(0);
        }

        let Some(entry) = self.data.get(key) else {
            return Ok(0);
        };

        match entry.value() {
            StorageValue::Zset { value, .. } => Ok(value.by_member.len()),
            _ => Err(()),
        }
    }

    pub fn zrem(&self, key: &str, members: &[String]) -> Result<usize, ()> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return Ok(0);
        }

        let removed = if let Some(mut entry) = self.data.get_mut(key) {
            match entry.value_mut() {
                StorageValue::Zset { value, .. } => {
                    let mut removed = 0;
                    for member in members {
                        if let Some(old_score) = value.by_member.remove(member) {
                            value
                                .by_score
                                .remove(&(OrderedFloat(old_score), member.clone()));
                            removed += 1;
                        }
                    }
                    removed
                }
                _ => return Err(()),
            }
        } else {
            0
        };

        if removed > 0 {
            self.touch_key(key);
            self.bump_key_version(key);
        }

        Ok(removed)
    }

    pub fn zscore(&self, key: &str, member: &str) -> Result<Option<f64>, ()> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return Ok(None);
        }

        let Some(entry) = self.data.get(key) else {
            return Ok(None);
        };

        match entry.value() {
            StorageValue::Zset { value, .. } => Ok(value.by_member.get(member).cloned()),
            _ => Err(()),
        }
    }

    pub fn zincrby(&self, key: &str, increment: f64, member: &str) -> Result<f64, ZsetError> {
        if !increment.is_finite() {
            return Err(ZsetError::NotFloat);
        }

        let now = Instant::now();
        self.remove_if_expired(key, now);

        let new_score: f64;

        if let Some(mut entry) = self.data.get_mut(key) {
            match entry.value_mut() {
                StorageValue::Zset { value, .. } => {
                    let old = value.by_member.get(member).cloned().unwrap_or(0.0);
                    new_score = old + increment;
                    if !new_score.is_finite() {
                        return Err(ZsetError::NotFloat);
                    }
                    let _ = value
                        .by_score
                        .remove(&(OrderedFloat(old), member.to_string()));
                    value
                        .by_score
                        .insert((OrderedFloat(new_score), member.to_string()));
                    value
                        .by_member
                        .insert(member.to_string(), new_score);
                }
                _ => return Err(ZsetError::WrongType),
            }
        } else {
            new_score = increment;
            let mut inner = ZSetInner {
                by_member: HashMap::new(),
                by_score: BTreeSet::new(),
            };
            inner
                .by_member
                .insert(member.to_string(), new_score);
            inner
                .by_score
                .insert((OrderedFloat(new_score), member.to_string()));
            self.data.insert(
                key.to_string(),
                StorageValue::Zset {
                    value: inner,
                    expires_at: None,
                },
            );
        }

        self.touch_key(key);
        self.bump_key_version(key);
        self.maybe_evict_for_write();

        Ok(new_score)
    }

    pub fn zrange(
        &self,
        key: &str,
        start: isize,
        stop: isize,
        rev: bool,
    ) -> Result<Vec<(String, f64)>, ()> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return Ok(Vec::new());
        }

        let entry = match self.data.get(key) {
            Some(e) => e,
            None => return Ok(Vec::new()),
        };

        let zset = match entry.value() {
            StorageValue::Zset { value, .. } => value,
            _ => return Err(()),
        };

        let mut items: Vec<(String, f64)> = zset
            .by_score
            .iter()
            .map(|(score, member)| (member.clone(), score.0))
            .collect();

        if rev {
            items.reverse();
        }

        let len = items.len() as isize;
        if len == 0 {
            return Ok(Vec::new());
        }

        let normalize = |idx: isize| -> isize {
            if idx < 0 {
                len + idx
            } else {
                idx
            }
        };

        let mut s = normalize(start);
        let mut e = normalize(stop);

        if s < 0 {
            s = 0;
        }
        if e < 0 {
            return Ok(Vec::new());
        }
        if s >= len {
            return Ok(Vec::new());
        }
        if e >= len {
            e = len - 1;
        }
        if s > e {
            return Ok(Vec::new());
        }

        let take_len = (e - s + 1) as usize;
        let result = items
            .into_iter()
            .skip(s as usize)
            .take(take_len)
            .collect();

        Ok(result)
    }

    pub fn zscan_entries(&self, key: &str) -> Result<Vec<(String, f64)>, ()> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return Ok(Vec::new());
        }

        let entry = match self.data.get(key) {
            Some(e) => e,
            None => return Ok(Vec::new()),
        };

        let zset = match entry.value() {
            StorageValue::Zset { value, .. } => value,
            _ => return Err(()),
        };

        let items: Vec<(String, f64)> = zset
            .by_score
            .iter()
            .map(|(score, member)| (member.clone(), score.0))
            .collect();

        Ok(items)
    }

    /// ZCOUNT: 统计分数在 [min, max] 范围内的成员数量
    pub fn zcount(&self, key: &str, min: f64, min_exclusive: bool, max: f64, max_exclusive: bool) -> Result<usize, ()> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return Ok(0);
        }

        let entry = match self.data.get(key) {
            Some(e) => e,
            None => return Ok(0),
        };

        let zset = match entry.value() {
            StorageValue::Zset { value, .. } => value,
            _ => return Err(()),
        };

        let count = zset.by_score.iter().filter(|(score, _)| {
            let s = score.0;
            let min_ok = if min_exclusive { s > min } else { s >= min };
            let max_ok = if max_exclusive { s < max } else { s <= max };
            min_ok && max_ok
        }).count();

        Ok(count)
    }

    /// ZRANK: 获取成员的排名（从 0 开始，按分数升序）
    pub fn zrank(&self, key: &str, member: &str) -> Result<Option<usize>, ()> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return Ok(None);
        }

        let entry = match self.data.get(key) {
            Some(e) => e,
            None => return Ok(None),
        };

        let zset = match entry.value() {
            StorageValue::Zset { value, .. } => value,
            _ => return Err(()),
        };

        // 获取成员的分数
        let score = match zset.by_member.get(member) {
            Some(s) => *s,
            None => return Ok(None),
        };

        // 计算排名
        let rank = zset.by_score.iter()
            .position(|(s, m)| s.0 == score && m == member);

        Ok(rank)
    }

    /// ZREVRANK: 获取成员的排名（从 0 开始，按分数降序）
    pub fn zrevrank(&self, key: &str, member: &str) -> Result<Option<usize>, ()> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return Ok(None);
        }

        let entry = match self.data.get(key) {
            Some(e) => e,
            None => return Ok(None),
        };

        let zset = match entry.value() {
            StorageValue::Zset { value, .. } => value,
            _ => return Err(()),
        };

        // 获取成员的分数
        let score = match zset.by_member.get(member) {
            Some(s) => *s,
            None => return Ok(None),
        };

        // 计算逆序排名
        let total = zset.by_score.len();
        let rank = zset.by_score.iter()
            .position(|(s, m)| s.0 == score && m == member);

        Ok(rank.map(|r| total - 1 - r))
    }

    /// ZPOPMIN: 弹出分数最小的成员
    pub fn zpopmin(&self, key: &str, count: usize) -> Result<Vec<(String, f64)>, ()> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return Ok(Vec::new());
        }

        let Some(mut entry) = self.data.get_mut(key) else {
            return Ok(Vec::new());
        };

        let zset = match entry.value_mut() {
            StorageValue::Zset { value, .. } => value,
            _ => return Err(()),
        };

        let mut result = Vec::new();
        for _ in 0..count {
            if let Some((score, member)) = zset.by_score.pop_first() {
                zset.by_member.remove(&member);
                result.push((member, score.0));
            } else {
                break;
            }
        }

        if !result.is_empty() {
            self.touch_key(key);
            self.bump_key_version(key);
        }

        Ok(result)
    }

    /// ZPOPMAX: 弹出分数最大的成员
    pub fn zpopmax(&self, key: &str, count: usize) -> Result<Vec<(String, f64)>, ()> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return Ok(Vec::new());
        }

        let Some(mut entry) = self.data.get_mut(key) else {
            return Ok(Vec::new());
        };

        let zset = match entry.value_mut() {
            StorageValue::Zset { value, .. } => value,
            _ => return Err(()),
        };

        let mut result = Vec::new();
        for _ in 0..count {
            if let Some((score, member)) = zset.by_score.pop_last() {
                zset.by_member.remove(&member);
                result.push((member, score.0));
            } else {
                break;
            }
        }

        if !result.is_empty() {
            self.touch_key(key);
            self.bump_key_version(key);
        }

        Ok(result)
    }

    /// ZINTER: 计算多个 sorted set 的交集
    pub fn zinter(
        &self,
        keys: &[String],
        weights: Option<&[f64]>,
        aggregate: ZsetAggregate,
    ) -> Result<Vec<(String, f64)>, ()> {
        let now = Instant::now();
        
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        
        // 获取第一个 set 作为基础
        for key in keys {
            self.remove_if_expired(key, now);
        }
        
        let first_key = &keys[0];
        let first_entry = match self.data.get(first_key) {
            Some(e) => e,
            None => return Ok(Vec::new()), // 任一 key 不存在，交集为空
        };
        
        let first_zset = match first_entry.value() {
            StorageValue::Zset { value, .. } => value,
            _ => return Err(()),
        };
        
        // 收集第一个 set 的所有成员
        let mut result: HashMap<String, f64> = HashMap::new();
        let w0 = weights.map(|w| w[0]).unwrap_or(1.0);
        for (member, score) in &first_zset.by_member {
            result.insert(member.clone(), *score * w0);
        }
        drop(first_entry);
        
        // 与其他 set 求交集
        for (i, key) in keys.iter().enumerate().skip(1) {
            let entry = match self.data.get(key) {
                Some(e) => e,
                None => return Ok(Vec::new()),
            };
            
            let zset = match entry.value() {
                StorageValue::Zset { value, .. } => value,
                _ => return Err(()),
            };
            
            let weight = weights.map(|w| w[i]).unwrap_or(1.0);
            
            // 保留交集
            result.retain(|member, score| {
                if let Some(&other_score) = zset.by_member.get(member) {
                    let weighted = other_score * weight;
                    *score = match aggregate {
                        ZsetAggregate::Sum => *score + weighted,
                        ZsetAggregate::Min => score.min(weighted),
                        ZsetAggregate::Max => score.max(weighted),
                    };
                    true
                } else {
                    false
                }
            });
        }
        
        // 按分数排序
        let mut items: Vec<(String, f64)> = result.into_iter().collect();
        items.sort_by(|a, b| {
            a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });
        
        Ok(items)
    }

    /// ZUNION: 计算多个 sorted set 的并集
    pub fn zunion(
        &self,
        keys: &[String],
        weights: Option<&[f64]>,
        aggregate: ZsetAggregate,
    ) -> Result<Vec<(String, f64)>, ()> {
        let now = Instant::now();
        
        let mut result: HashMap<String, f64> = HashMap::new();
        
        for (i, key) in keys.iter().enumerate() {
            self.remove_if_expired(key, now);
            
            let entry = match self.data.get(key) {
                Some(e) => e,
                None => continue,
            };
            
            let zset = match entry.value() {
                StorageValue::Zset { value, .. } => value,
                _ => return Err(()),
            };
            
            let weight = weights.map(|w| w[i]).unwrap_or(1.0);
            
            for (member, score) in &zset.by_member {
                let weighted = *score * weight;
                result.entry(member.clone())
                    .and_modify(|s| {
                        *s = match aggregate {
                            ZsetAggregate::Sum => *s + weighted,
                            ZsetAggregate::Min => s.min(weighted),
                            ZsetAggregate::Max => s.max(weighted),
                        };
                    })
                    .or_insert(weighted);
            }
        }
        
        // 按分数排序
        let mut items: Vec<(String, f64)> = result.into_iter().collect();
        items.sort_by(|a, b| {
            a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });
        
        Ok(items)
    }

    /// ZDIFF: 计算第一个 sorted set 与其他 set 的差集
    pub fn zdiff(&self, keys: &[String]) -> Result<Vec<(String, f64)>, ()> {
        let now = Instant::now();
        
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        
        for key in keys {
            self.remove_if_expired(key, now);
        }
        
        let first_key = &keys[0];
        let first_entry = match self.data.get(first_key) {
            Some(e) => e,
            None => return Ok(Vec::new()),
        };
        
        let first_zset = match first_entry.value() {
            StorageValue::Zset { value, .. } => value,
            _ => return Err(()),
        };
        
        // 收集第一个 set 的所有成员
        let mut result: HashMap<String, f64> = first_zset.by_member.clone();
        drop(first_entry);
        
        // 移除在其他 set 中出现的成员
        for key in keys.iter().skip(1) {
            let entry = match self.data.get(key) {
                Some(e) => e,
                None => continue,
            };
            
            let zset = match entry.value() {
                StorageValue::Zset { value, .. } => value,
                _ => return Err(()),
            };
            
            for member in zset.by_member.keys() {
                result.remove(member);
            }
        }
        
        // 按分数排序
        let mut items: Vec<(String, f64)> = result.into_iter().collect();
        items.sort_by(|a, b| {
            a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });
        
        Ok(items)
    }

    /// ZINTERSTORE: 计算交集并存储到目标 key
    pub fn zinterstore(
        &self,
        destination: &str,
        keys: &[String],
        weights: Option<&[f64]>,
        aggregate: ZsetAggregate,
    ) -> Result<usize, ()> {
        let items = self.zinter(keys, weights, aggregate)?;
        let count = items.len();
        
        // 删除旧的目标 key
        self.data.remove(destination);
        
        if !items.is_empty() {
            let mut inner = ZSetInner {
                by_member: HashMap::new(),
                by_score: BTreeSet::new(),
            };
            for (member, score) in items {
                inner.by_member.insert(member.clone(), score);
                inner.by_score.insert((OrderedFloat(score), member));
            }
            self.data.insert(
                destination.to_string(),
                StorageValue::Zset {
                    value: inner,
                    expires_at: None,
                },
            );
            self.touch_key(destination);
            self.bump_key_version(destination);
        }
        
        Ok(count)
    }

    /// ZUNIONSTORE: 计算并集并存储到目标 key
    pub fn zunionstore(
        &self,
        destination: &str,
        keys: &[String],
        weights: Option<&[f64]>,
        aggregate: ZsetAggregate,
    ) -> Result<usize, ()> {
        let items = self.zunion(keys, weights, aggregate)?;
        let count = items.len();
        
        // 删除旧的目标 key
        self.data.remove(destination);
        
        if !items.is_empty() {
            let mut inner = ZSetInner {
                by_member: HashMap::new(),
                by_score: BTreeSet::new(),
            };
            for (member, score) in items {
                inner.by_member.insert(member.clone(), score);
                inner.by_score.insert((OrderedFloat(score), member));
            }
            self.data.insert(
                destination.to_string(),
                StorageValue::Zset {
                    value: inner,
                    expires_at: None,
                },
            );
            self.touch_key(destination);
            self.bump_key_version(destination);
        }
        
        Ok(count)
    }

    /// ZDIFFSTORE: 计算差集并存储到目标 key
    pub fn zdiffstore(&self, destination: &str, keys: &[String]) -> Result<usize, ()> {
        let items = self.zdiff(keys)?;
        let count = items.len();
        
        // 删除旧的目标 key
        self.data.remove(destination);
        
        if !items.is_empty() {
            let mut inner = ZSetInner {
                by_member: HashMap::new(),
                by_score: BTreeSet::new(),
            };
            for (member, score) in items {
                inner.by_member.insert(member.clone(), score);
                inner.by_score.insert((OrderedFloat(score), member));
            }
            self.data.insert(
                destination.to_string(),
                StorageValue::Zset {
                    value: inner,
                    expires_at: None,
                },
            );
            self.touch_key(destination);
            self.bump_key_version(destination);
        }
        
        Ok(count)
    }

    /// ZLEXCOUNT: 统计字典序在 [min, max] 范围内的成员数量
    /// 注意：ZLEXCOUNT 要求所有成员的分数相同，但这里我们简化实现，只按字典序比较
    pub fn zlexcount(
        &self,
        key: &str,
        min: &str,
        min_inclusive: bool,
        max: &str,
        max_inclusive: bool,
        min_unbounded: bool,
        max_unbounded: bool,
    ) -> Result<usize, ()> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return Ok(0);
        }

        let entry = match self.data.get(key) {
            Some(e) => e,
            None => return Ok(0),
        };

        let zset = match entry.value() {
            StorageValue::Zset { value, .. } => value,
            _ => return Err(()),
        };

        let count = zset.by_member.keys().filter(|member| {
            let min_ok = if min_unbounded {
                true
            } else if min_inclusive {
                member.as_str() >= min
            } else {
                member.as_str() > min
            };
            
            let max_ok = if max_unbounded {
                true
            } else if max_inclusive {
                member.as_str() <= max
            } else {
                member.as_str() < max
            };
            
            min_ok && max_ok
        }).count();

        Ok(count)
    }

    pub fn save_rdb<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        let mut file = File::create(path)?;

        file.write_all(b"REDUSTDB")?;
        file.write_all(&1u32.to_le_bytes())?;

        let now = Instant::now();

        for entry in self.data.iter() {
            let key = entry.key();
            let value = entry.value();

            if Storage::value_is_expired(value, now) {
                continue;
            }

            let (type_byte, expires_millis) = match value {
                StorageValue::String { expires_at, .. } => {
                    (0u8, Self::remaining_millis(*expires_at, now))
                }
                StorageValue::List { expires_at, .. } => {
                    (1u8, Self::remaining_millis(*expires_at, now))
                }
                StorageValue::Set { expires_at, .. } => {
                    (2u8, Self::remaining_millis(*expires_at, now))
                }
                StorageValue::Hash { expires_at, .. } => {
                    (3u8, Self::remaining_millis(*expires_at, now))
                }
                StorageValue::Zset { expires_at, .. } => {
                    (4u8, Self::remaining_millis(*expires_at, now))
                }
                StorageValue::HyperLogLog { expires_at, .. } => {
                    (5u8, Self::remaining_millis(*expires_at, now))
                }
            };

            file.write_all(&[type_byte])?;
            file.write_all(&expires_millis.to_le_bytes())?;

            let key_bytes = key.as_bytes();
            let key_len = key_bytes.len() as u32;
            file.write_all(&key_len.to_le_bytes())?;
            file.write_all(key_bytes)?;

            match value {
                StorageValue::String { value, .. } => {
                    let v_len = value.len() as u32;
                    file.write_all(&v_len.to_le_bytes())?;
                    file.write_all(value)?;
                }
                StorageValue::List { value: list, .. } => {
                    let len = list.len() as u32;
                    file.write_all(&len.to_le_bytes())?;
                    for item in list.iter() {
                        let b = item.as_bytes();
                        let l = b.len() as u32;
                        file.write_all(&l.to_le_bytes())?;
                        file.write_all(b)?;
                    }
                }
                StorageValue::Set { value: set, .. } => {
                    let len = set.len() as u32;
                    file.write_all(&len.to_le_bytes())?;
                    for member in set.iter() {
                        let b = member.as_bytes();
                        let l = b.len() as u32;
                        file.write_all(&l.to_le_bytes())?;
                        file.write_all(b)?;
                    }
                }
                StorageValue::Hash { value: map, .. } => {
                    let len = map.len() as u32;
                    file.write_all(&len.to_le_bytes())?;
                    for (field, val) in map.iter() {
                        let f_bytes = field.as_bytes();
                        let f_len = f_bytes.len() as u32;
                        file.write_all(&f_len.to_le_bytes())?;
                        file.write_all(f_bytes)?;

                        let v_bytes = val.as_bytes();
                        let v_len = v_bytes.len() as u32;
                        file.write_all(&v_len.to_le_bytes())?;
                        file.write_all(v_bytes)?;
                    }
                }
                StorageValue::Zset { value: zset, .. } => {
                    // 序列化 ZSET: 元素数量 + (score, member) 对
                    let len = zset.by_member.len() as u32;
                    file.write_all(&len.to_le_bytes())?;
                    for (member, score) in zset.by_member.iter() {
                        // 写入 score (f64, 8 bytes)
                        file.write_all(&score.to_le_bytes())?;
                        // 写入 member
                        let m_bytes = member.as_bytes();
                        let m_len = m_bytes.len() as u32;
                        file.write_all(&m_len.to_le_bytes())?;
                        file.write_all(m_bytes)?;
                    }
                }
                StorageValue::HyperLogLog { value: hll, .. } => {
                    // 序列化 HyperLogLog: 直接写入 16384 个寄存器
                    let registers = hll.registers();
                    file.write_all(&registers)?;
                }
            }
        }

        Ok(())
    }

    pub fn load_rdb<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        let path_ref = path.as_ref();
        if !path_ref.exists() {
            return Ok(());
        }

        let mut file = File::open(path_ref)?;

        let mut magic = [0u8; 8];
        if file.read_exact(&mut magic).is_err() {
            return Ok(());
        }
        if &magic != b"REDUSTDB" {
            return Ok(());
        }

        let mut version_bytes = [0u8; 4];
        if file.read_exact(&mut version_bytes).is_err() {
            return Ok(());
        }
        let version = u32::from_le_bytes(version_bytes);
        if version != 1 {
            return Ok(());
        }

        self.data.clear();
        self.last_access.clear();

        loop {
            let mut type_buf = [0u8; 1];
            match file.read_exact(&mut type_buf) {
                Ok(()) => {}
                Err(e) => {
                    if e.kind() == io::ErrorKind::UnexpectedEof {
                        break;
                    } else {
                        return Err(e);
                    }
                }
            }

            let mut expires_buf = [0u8; 8];
            if file.read_exact(&mut expires_buf).is_err() {
                break;
            }
            let expires_millis = i64::from_le_bytes(expires_buf);

            let mut key_len_buf = [0u8; 4];
            if file.read_exact(&mut key_len_buf).is_err() {
                break;
            }
            let key_len = u32::from_le_bytes(key_len_buf) as usize;
            let mut key_bytes = vec![0u8; key_len];
            if file.read_exact(&mut key_bytes).is_err() {
                break;
            }
            let key = match String::from_utf8(key_bytes) {
                Ok(s) => s,
                Err(_) => {
                    return Ok(());
                }
            };

            let now = Instant::now();
            let expires_at = if expires_millis < 0 {
                None
            } else if expires_millis == 0 {
                continue;
            } else {
                Some(now + Duration::from_millis(expires_millis as u64))
            };

            let t = type_buf[0];
            let value = match t {
                0 => {
                    let mut len_buf = [0u8; 4];
                    if file.read_exact(&mut len_buf).is_err() {
                        break;
                    }
                    let len = u32::from_le_bytes(len_buf) as usize;
                    let mut v = vec![0u8; len];
                    if file.read_exact(&mut v).is_err() {
                        break;
                    }
                    StorageValue::String {
                        value: v,
                        expires_at,
                    }
                }
                1 => {
                    let mut len_buf = [0u8; 4];
                    if file.read_exact(&mut len_buf).is_err() {
                        break;
                    }
                    let len = u32::from_le_bytes(len_buf) as usize;
                    let mut list = VecDeque::with_capacity(len);
                    for _ in 0..len {
                        let mut ilen_buf = [0u8; 4];
                        if file.read_exact(&mut ilen_buf).is_err() {
                            break;
                        }
                        let ilen = u32::from_le_bytes(ilen_buf) as usize;
                        let mut item = vec![0u8; ilen];
                        if file.read_exact(&mut item).is_err() {
                            break;
                        }
                        let s = match String::from_utf8(item) {
                            Ok(s) => s,
                            Err(_) => {
                                return Ok(());
                            }
                        };
                        list.push_back(s);
                    }
                    StorageValue::List {
                        value: list,
                        expires_at,
                    }
                }
                2 => {
                    let mut len_buf = [0u8; 4];
                    if file.read_exact(&mut len_buf).is_err() {
                        break;
                    }
                    let len = u32::from_le_bytes(len_buf) as usize;
                    let mut set = HashSet::with_capacity(len);
                    for _ in 0..len {
                        let mut mlen_buf = [0u8; 4];
                        if file.read_exact(&mut mlen_buf).is_err() {
                            break;
                        }
                        let mlen = u32::from_le_bytes(mlen_buf) as usize;
                        let mut member = vec![0u8; mlen];
                        if file.read_exact(&mut member).is_err() {
                            break;
                        }
                        let s = match String::from_utf8(member) {
                            Ok(s) => s,
                            Err(_) => {
                                return Ok(());
                            }
                        };
                        set.insert(s);
                    }
                    StorageValue::Set {
                        value: set,
                        expires_at,
                    }
                }
                3 => {
                    let mut len_buf = [0u8; 4];
                    if file.read_exact(&mut len_buf).is_err() {
                        break;
                    }
                    let len = u32::from_le_bytes(len_buf) as usize;
                    let mut map = HashMap::with_capacity(len);
                    for _ in 0..len {
                        let mut flen_buf = [0u8; 4];
                        if file.read_exact(&mut flen_buf).is_err() {
                            break;
                        }
                        let flen = u32::from_le_bytes(flen_buf) as usize;
                        let mut field = vec![0u8; flen];
                        if file.read_exact(&mut field).is_err() {
                            break;
                        }
                        let field_str = match String::from_utf8(field) {
                            Ok(s) => s,
                            Err(_) => {
                                return Ok(());
                            }
                        };

                        let mut vlen_buf = [0u8; 4];
                        if file.read_exact(&mut vlen_buf).is_err() {
                            break;
                        }
                        let vlen = u32::from_le_bytes(vlen_buf) as usize;
                        let mut val = vec![0u8; vlen];
                        if file.read_exact(&mut val).is_err() {
                            break;
                        }
                        let val_str = match String::from_utf8(val) {
                            Ok(s) => s,
                            Err(_) => {
                                return Ok(());
                            }
                        };

                        map.insert(field_str, val_str);
                    }
                    StorageValue::Hash {
                        value: map,
                        expires_at,
                    }
                }
                4 => {
                    // ZSET 类型
                    let mut len_buf = [0u8; 4];
                    if file.read_exact(&mut len_buf).is_err() {
                        break;
                    }
                    let len = u32::from_le_bytes(len_buf) as usize;
                    let mut by_member = HashMap::with_capacity(len);
                    let mut by_score = BTreeSet::new();
                    for _ in 0..len {
                        // 读取 score (f64, 8 bytes)
                        let mut score_buf = [0u8; 8];
                        if file.read_exact(&mut score_buf).is_err() {
                            break;
                        }
                        let score = f64::from_le_bytes(score_buf);

                        // 读取 member
                        let mut mlen_buf = [0u8; 4];
                        if file.read_exact(&mut mlen_buf).is_err() {
                            break;
                        }
                        let mlen = u32::from_le_bytes(mlen_buf) as usize;
                        let mut member = vec![0u8; mlen];
                        if file.read_exact(&mut member).is_err() {
                            break;
                        }
                        let member_str = match String::from_utf8(member) {
                            Ok(s) => s,
                            Err(_) => {
                                return Ok(());
                            }
                        };

                        by_member.insert(member_str.clone(), score);
                        by_score.insert((OrderedFloat(score), member_str));
                    }
                    StorageValue::Zset {
                        value: ZSetInner { by_member, by_score },
                        expires_at,
                    }
                }
                5 => {
                    // 反序列化 HyperLogLog: 读取 16384 个寄存器
                    let mut registers = vec![0u8; 16384];
                    if file.read_exact(&mut registers).is_err() {
                        break;
                    }
                    let hll = match HyperLogLog::from_registers(registers) {
                        Some(h) => h,
                        None => {
                            return Ok(());
                        }
                    };
                    StorageValue::HyperLogLog {
                        value: hll,
                        expires_at,
                    }
                }
                _ => {
                    return Ok(());
                }
            };

            self.data.insert(key, value);
        }

        Ok(())
    }

    pub fn expire_seconds(&self, key: &str, seconds: i64) -> bool {
        // Redis 语义：seconds <= 0 视为立刻过期并删除，若 key 存在返回 1
        if seconds <= 0 {
            let existed = self.data.remove(key).is_some();
            if existed {
                self.last_access.remove(key);
                self.bump_key_version(key);
            }
            return existed;
        }

        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return false;
        }

        let Some(mut entry) = self.data.get_mut(key) else {
            return false;
        };

        let deadline = now + Duration::from_secs(seconds as u64);

        match entry.value_mut() {
            StorageValue::String { expires_at, .. }
            | StorageValue::List { expires_at, .. }
            | StorageValue::Set { expires_at, .. }
            | StorageValue::Hash { expires_at, .. }
            | StorageValue::Zset { expires_at, .. }
            | StorageValue::HyperLogLog { expires_at, .. } => {
                *expires_at = Some(deadline);
                self.bump_key_version(key);
                true
            }
        }
    }

    pub fn expire_millis(&self, key: &str, millis: i64) -> bool {
        if millis <= 0 {
            let existed = self.data.remove(key).is_some();
            if existed {
                self.last_access.remove(key);
                self.bump_key_version(key);
            }
            return existed;
        }

        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return false;
        }

        let Some(mut entry) = self.data.get_mut(key) else {
            return false;
        };

        let deadline = now + Duration::from_millis(millis as u64);

        match entry.value_mut() {
            StorageValue::String { expires_at, .. }
            | StorageValue::List { expires_at, .. }
            | StorageValue::Set { expires_at, .. }
            | StorageValue::Hash { expires_at, .. }
            | StorageValue::Zset { expires_at, .. }
            | StorageValue::HyperLogLog { expires_at, .. } => {
                *expires_at = Some(deadline);
                self.bump_key_version(key);
                true
            }
        }
    }

    pub fn ttl_seconds(&self, key: &str) -> i64 {
        // Redis 语义：
        // - key 不存在 -> -2
        // - 存在但无过期时间 -> -1
        // - 否则返回剩余秒数，向上取整
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return -2;
        }

        let Some(entry) = self.data.get(key) else {
            return -2;
        };

        let expires_at = match entry.value() {
            StorageValue::String { expires_at, .. }
            | StorageValue::List { expires_at, .. }
            | StorageValue::Set { expires_at, .. }
            | StorageValue::Hash { expires_at, .. }
            | StorageValue::Zset { expires_at, .. }
            | StorageValue::HyperLogLog { expires_at, .. } => expires_at,
        };

        let Some(deadline) = expires_at else {
            return -1;
        };

        if *deadline <= now {
            // 过期键交给懒删除/定期删除，这里视为不存在
            if self.data.remove(key).is_some() {
                self.last_access.remove(key);
            }
            return -2;
        }

        let remaining = deadline.duration_since(now);
        let millis = remaining.as_millis() as i64;
        // 向上取整到秒
        (millis + 999) / 1000
    }

    pub fn pttl_millis(&self, key: &str) -> i64 {
        // Redis 语义同 TTL，但单位为毫秒
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return -2;
        }

        let Some(entry) = self.data.get(key) else {
            return -2;
        };

        let expires_at = match entry.value() {
            StorageValue::String { expires_at, .. }
            | StorageValue::List { expires_at, .. }
            | StorageValue::Set { expires_at, .. }
            | StorageValue::Hash { expires_at, .. }
            | StorageValue::Zset { expires_at, .. }
            | StorageValue::HyperLogLog { expires_at, .. } => expires_at,
        };

        let Some(deadline) = expires_at else {
            return -1;
        };

        if *deadline <= now {
            if self.data.remove(key).is_some() {
                self.last_access.remove(key);
            }
            return -2;
        }

        let remaining = deadline.duration_since(now);
        remaining.as_millis() as i64
    }

    pub fn persist(&self, key: &str) -> bool {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return false;
        }

        let Some(mut entry) = self.data.get_mut(key) else {
            return false;
        };

        match entry.value_mut() {
            StorageValue::String { expires_at, .. }
            | StorageValue::List { expires_at, .. }
            | StorageValue::Set { expires_at, .. }
            | StorageValue::Hash { expires_at, .. }
            | StorageValue::Zset { expires_at, .. }
            | StorageValue::HyperLogLog { expires_at, .. } => {
                if expires_at.is_some() {
                    *expires_at = None;
                    self.bump_key_version(key);
                    true
                } else {
                    false
                }
            }
        }
    }

    /// EXPIREAT: 设置绝对过期时间（Unix 秒）
    pub fn expireat(&self, key: &str, timestamp: i64) -> bool {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return false;
        }

        let Some(mut entry) = self.data.get_mut(key) else {
            return false;
        };

        // 计算从现在到目标时间的 Duration
        let now_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        
        let diff_secs = timestamp - now_unix;
        if diff_secs <= 0 {
            // 已过期，删除 key
            drop(entry);
            self.data.remove(key);
            self.last_access.remove(key);
            self.bump_key_version(key);
            return true;
        }

        let deadline = now + Duration::from_secs(diff_secs as u64);

        match entry.value_mut() {
            StorageValue::String { expires_at, .. }
            | StorageValue::List { expires_at, .. }
            | StorageValue::Set { expires_at, .. }
            | StorageValue::Hash { expires_at, .. }
            | StorageValue::Zset { expires_at, .. }
            | StorageValue::HyperLogLog { expires_at, .. } => {
                *expires_at = Some(deadline);
                self.bump_key_version(key);
                true
            }
        }
    }

    /// PEXPIREAT: 设置绝对过期时间（Unix 毫秒）
    pub fn pexpireat(&self, key: &str, timestamp_ms: i64) -> bool {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return false;
        }

        let Some(mut entry) = self.data.get_mut(key) else {
            return false;
        };

        let now_unix_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        
        let diff_ms = timestamp_ms - now_unix_ms;
        if diff_ms <= 0 {
            drop(entry);
            self.data.remove(key);
            self.last_access.remove(key);
            self.bump_key_version(key);
            return true;
        }

        let deadline = now + Duration::from_millis(diff_ms as u64);

        match entry.value_mut() {
            StorageValue::String { expires_at, .. }
            | StorageValue::List { expires_at, .. }
            | StorageValue::Set { expires_at, .. }
            | StorageValue::Hash { expires_at, .. }
            | StorageValue::Zset { expires_at, .. }
            | StorageValue::HyperLogLog { expires_at, .. } => {
                *expires_at = Some(deadline);
                self.bump_key_version(key);
                true
            }
        }
    }

    /// EXPIRETIME: 返回 key 的绝对过期时间（Unix 秒）
    /// -2: key 不存在, -1: 无过期时间
    pub fn expiretime(&self, key: &str) -> i64 {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return -2;
        }

        let Some(entry) = self.data.get(key) else {
            return -2;
        };

        let expires_at = match entry.value() {
            StorageValue::String { expires_at, .. }
            | StorageValue::List { expires_at, .. }
            | StorageValue::Set { expires_at, .. }
            | StorageValue::Hash { expires_at, .. }
            | StorageValue::Zset { expires_at, .. }
            | StorageValue::HyperLogLog { expires_at, .. } => expires_at,
        };

        let Some(deadline) = expires_at else {
            return -1;
        };

        // 计算绝对时间戳
        let now_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        
        if *deadline <= now {
            return -2;
        }

        let remaining = deadline.duration_since(now);
        now_unix + remaining.as_secs() as i64
    }

    /// PEXPIRETIME: 返回 key 的绝对过期时间（Unix 毫秒）
    /// -2: key 不存在, -1: 无过期时间
    pub fn pexpiretime(&self, key: &str) -> i64 {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return -2;
        }

        let Some(entry) = self.data.get(key) else {
            return -2;
        };

        let expires_at = match entry.value() {
            StorageValue::String { expires_at, .. }
            | StorageValue::List { expires_at, .. }
            | StorageValue::Set { expires_at, .. }
            | StorageValue::Hash { expires_at, .. }
            | StorageValue::Zset { expires_at, .. }
            | StorageValue::HyperLogLog { expires_at, .. } => expires_at,
        };

        let Some(deadline) = expires_at else {
            return -1;
        };

        let now_unix_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        
        if *deadline <= now {
            return -2;
        }

        let remaining = deadline.duration_since(now);
        now_unix_ms + remaining.as_millis() as i64
    }

    pub fn sinter(&self, keys: &[String]) -> Result<Vec<String>, ()> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        // 先扫描一遍，找到最小的集合（按元素个数），同时检查类型
        let mut smallest_index: Option<usize> = None;
        let mut smallest_len = usize::MAX;

        for (i, key) in keys.iter().enumerate() {
            let now = Instant::now();
            if self.remove_if_expired(key, now) {
                return Ok(Vec::new());
            }

            let Some(entry) = self.data.get(key) else {
                return Ok(Vec::new()); // Key does not exist -> empty
            };

            if let StorageValue::Set { value: set, .. } = entry.value() {
                let len = set.len();
                if len < smallest_len {
                    smallest_len = len;
                    smallest_index = Some(i);
                }
            } else {
                return Err(()); // WRONGTYPE
            }
        }

        let Some(smallest_idx) = smallest_index else {
            return Ok(Vec::new());
        };

        // 再次获取最小集合，遍历其元素，并对其他集合做 contains 检查
        let smallest_key = &keys[smallest_idx];
        let now = Instant::now();
        if self.remove_if_expired(smallest_key, now) {
            return Ok(Vec::new());
        }

        let Some(entry) = self.data.get(smallest_key) else {
            return Ok(Vec::new());
        };
        let StorageValue::Set {
            value: smallest_set,
            ..
        } = entry.value()
        else {
            return Err(());
        };

        let mut result: HashSet<String> = HashSet::new();

        'outer: for member in smallest_set.iter() {
            for (j, key) in keys.iter().enumerate() {
                if j == smallest_idx {
                    continue;
                }

                let now = Instant::now();
                if self.remove_if_expired(key, now) {
                    return Ok(Vec::new());
                }

                let Some(other_entry) = self.data.get(key) else {
                    return Ok(Vec::new());
                };
                let StorageValue::Set {
                    value: other_set, ..
                } = other_entry.value()
                else {
                    return Err(());
                };

                if !other_set.contains(member) {
                    continue 'outer;
                }
            }

            result.insert(member.clone());
        }

        let mut members: Vec<String> = result.into_iter().collect();
        members.sort();
        Ok(members)
    }

    pub fn sdiff(&self, keys: &[String]) -> Result<Vec<String>, ()> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let first_key = &keys[0];
        let now = Instant::now();
        if self.remove_if_expired(first_key, now) {
            return Ok(Vec::new());
        }

        let first_set_entry = self.data.get(first_key);

        let first_set = match first_set_entry.as_ref().map(|e| e.value()) {
            Some(StorageValue::Set { value: s, .. }) => s,
            None => return Ok(Vec::new()), // missing -> empty
            _ => return Err(()),           // WRONGTYPE
        };

        let mut result: HashSet<String> = first_set.iter().cloned().collect();

        for key in &keys[1..] {
            let now = Instant::now();
            if self.remove_if_expired(key, now) {
                continue;
            }

            if let Some(entry) = self.data.get(key) {
                match entry.value() {
                    StorageValue::Set { value: set, .. } => {
                        for member in set.iter() {
                            result.remove(member);
                        }
                    }
                    _ => return Err(()), // WRONGTYPE
                }
            }
        }

        let mut members: Vec<String> = result.into_iter().collect();
        members.sort();
        Ok(members)
    }

    fn value_is_expired(value: &StorageValue, now: Instant) -> bool {
        let expires_at = match value {
            StorageValue::String { expires_at, .. } => expires_at,
            StorageValue::List { expires_at, .. } => expires_at,
            StorageValue::Set { expires_at, .. } => expires_at,
            StorageValue::Hash { expires_at, .. } => expires_at,
            StorageValue::Zset { expires_at, .. } => expires_at,
            StorageValue::HyperLogLog { expires_at, .. } => expires_at,
        };

        match expires_at {
            Some(when) => now >= *when,
            None => false,
        }
    }

    fn remaining_millis(expires_at: Option<Instant>, now: Instant) -> i64 {
        match expires_at {
            None => -1,
            Some(deadline) => {
                if deadline <= now {
                    0
                } else {
                    let dur = deadline.duration_since(now);
                    dur.as_millis() as i64
                }
            }
        }
    }

    fn remove_if_expired(&self, key: &str, now: Instant) -> bool {
        let mut should_remove = false;

        if let Some(entry) = self.data.get(key) {
            if Storage::value_is_expired(entry.value(), now) {
                should_remove = true;
            }
        }

        if should_remove {
            if self.data.remove(key).is_some() {
                self.last_access.remove(key);
                self.bump_key_version(key);
            }
            true
        } else {
            false
        }
    }

    pub fn spawn_expiration_task(&self) {
        let storage = self.clone();
        tokio::spawn(async move {
            let sample_size: usize = 20;
            let interval = Duration::from_millis(100);

            loop {
                tokio::time::sleep(interval).await;
                let now = Instant::now();

                let keys: Vec<String> = storage
                    .data
                    .iter()
                    .map(|entry| entry.key().clone())
                    .take(sample_size)
                    .collect();

                if keys.is_empty() {
                    continue;
                }

                for key in keys {
                    storage.remove_if_expired(&key, now);
                }
            }
        });
    }

    fn touch_key(&self, key: &str) {
        let ts = self.access_counter.fetch_add(1, Ordering::Relaxed);
        self.last_access.insert(key.to_string(), ts);
    }

    fn evict_one_sampled_key(&self) -> bool {
        let sample_size: usize = 5;

        let mut rng = thread_rng();

        // 均匀随机从所有 key 中选择 sample_size 个候选
        let candidates: Vec<String> = self
            .data
            .iter()
            .map(|e| e.key().clone())
            .choose_multiple(&mut rng, sample_size);

        if candidates.is_empty() {
            return false;
        }

        let mut oldest_key: Option<String> = None;
        let mut oldest_ts: u64 = u64::MAX;

        for k in candidates.into_iter() {
            let ts = self.last_access.get(&k).map(|v| *v.value()).unwrap_or(0);

            if ts < oldest_ts {
                oldest_ts = ts;
                oldest_key = Some(k);
            }
        }

        let Some(key) = oldest_key else {
            return false;
        };

        self.data.remove(&key);
        self.last_access.remove(&key);
        self.bump_key_version(&key);
        true
    }

    pub fn maybe_evict_for_write(&self) {
        let limit = self.maxmemory_bytes.load(Ordering::Relaxed);
        if limit == 0 {
            return;
        }

        // 简单实现：反复检查 approximate_used_memory，直到不再超限或没有可淘汰的键
        loop {
            let used = self.approximate_used_memory();
            if used <= limit {
                break;
            }

            if !self.evict_one_sampled_key() {
                break;
            }
        }
    }

    pub fn approximate_used_memory(&self) -> u64 {
        let mut total: u64 = 0;

        for entry in self.data.iter() {
            let key_size = entry.key().len() as u64;
            let value = entry.value();

            let value_size: u64 = match value {
                StorageValue::String { value, .. } => value.len() as u64,
                StorageValue::List { value: list, .. } => list.iter().map(|v| v.len() as u64).sum(),
                StorageValue::Set { value: set, .. } => set.iter().map(|v| v.len() as u64).sum(),
                StorageValue::Hash { value: map, .. } => {
                    map.iter().map(|(k, v)| (k.len() + v.len()) as u64).sum()
                }
                StorageValue::Zset { value, .. } => {
                    value
                        .by_member
                        .iter()
                        .map(|(member, _)| member.len() as u64 + std::mem::size_of::<f64>() as u64)
                        .sum()
                }
                StorageValue::HyperLogLog { .. } => {
                    // HyperLogLog 固定占用约 12KB (16384 个 6-bit 寄存器)
                    12288
                }
            };

            total = total.saturating_add(key_size.saturating_add(value_size));
        }

        total
    }

    pub fn maxmemory_bytes(&self) -> Option<u64> {
        let v = self.maxmemory_bytes.load(Ordering::Relaxed);
        if v == 0 {
            None
        } else {
            Some(v)
        }
    }

    pub fn set_maxmemory_bytes(&self, value: Option<u64>) {
        let v = value.unwrap_or(0);
        self.maxmemory_bytes.store(v, Ordering::Relaxed);
    }

    // ========== HyperLogLog 操作 ==========

    /// PFADD: 添加元素到 HyperLogLog
    ///
    /// 返回 1 如果 HLL 被修改（基数可能改变）
    /// 返回 0 如果 HLL 未被修改
    /// 返回 Err 如果 key 存在但不是 HyperLogLog 类型
    pub fn pfadd(&self, key: &str, elements: &[Vec<u8>]) -> Result<i64, ()> {
        let now = Instant::now();
        self.remove_if_expired(key, now);
        self.touch_key(key);

        let mut modified = false;

        if let Some(mut entry) = self.data.get_mut(key) {
            match entry.value_mut() {
                StorageValue::HyperLogLog { value: hll, .. } => {
                    for element in elements {
                        if hll.add(element) {
                            modified = true;
                        }
                    }
                }
                _ => return Err(()), // WRONGTYPE
            }
        } else {
            // 创建新的 HyperLogLog
            let mut hll = HyperLogLog::new();
            for element in elements {
                if hll.add(element) {
                    modified = true;
                }
            }
            self.data.insert(
                key.to_string(),
                StorageValue::HyperLogLog {
                    value: hll,
                    expires_at: None,
                },
            );
        }

        if modified {
            self.bump_key_version(key);
        }

        Ok(if modified { 1 } else { 0 })
    }

    /// PFCOUNT: 估算一个或多个 HyperLogLog 的基数
    ///
    /// 如果提供多个 key，会临时合并它们并返回并集的基数
    /// 返回 Err 如果任何 key 存在但不是 HyperLogLog 类型
    pub fn pfcount(&self, keys: &[String]) -> Result<u64, ()> {
        let now = Instant::now();

        if keys.is_empty() {
            return Ok(0);
        }

        if keys.len() == 1 {
            // 单个 key 的情况
            let key = &keys[0];
            self.remove_if_expired(key, now);

            if let Some(entry) = self.data.get(key) {
                match entry.value() {
                    StorageValue::HyperLogLog { value: hll, .. } => Ok(hll.count()),
                    _ => Err(()), // WRONGTYPE
                }
            } else {
                Ok(0) // key 不存在，返回 0
            }
        } else {
            // 多个 key 的情况：临时合并
            let mut merged = HyperLogLog::new();

            for key in keys {
                self.remove_if_expired(key, now);

                if let Some(entry) = self.data.get(key) {
                    match entry.value() {
                        StorageValue::HyperLogLog { value: hll, .. } => {
                            merged.merge(hll);
                        }
                        _ => return Err(()), // WRONGTYPE
                    }
                }
                // key 不存在时跳过
            }

            Ok(merged.count())
        }
    }

    /// PFMERGE: 合并多个 HyperLogLog 到目标 key
    ///
    /// 如果目标 key 已存在，必须是 HyperLogLog 类型
    /// 源 key 不存在时会被跳过
    /// 返回 Err 如果任何涉及的 key 类型不匹配
    pub fn pfmerge(&self, destkey: &str, sourcekeys: &[String]) -> Result<(), ()> {
        let now = Instant::now();

        // 收集所有源 HLL
        let mut merged = HyperLogLog::new();

        for key in sourcekeys {
            self.remove_if_expired(key, now);

            if let Some(entry) = self.data.get(key) {
                match entry.value() {
                    StorageValue::HyperLogLog { value: hll, .. } => {
                        merged.merge(hll);
                    }
                    _ => return Err(()), // WRONGTYPE
                }
            }
            // key 不存在时跳过
        }

        // 检查目标 key
        self.remove_if_expired(destkey, now);

        if let Some(mut entry) = self.data.get_mut(destkey) {
            match entry.value_mut() {
                StorageValue::HyperLogLog { value: dest_hll, .. } => {
                    // 合并到现有 HLL
                    dest_hll.merge(&merged);
                }
                _ => return Err(()), // WRONGTYPE
            }
        } else {
            // 创建新的 HLL
            self.data.insert(
                destkey.to_string(),
                StorageValue::HyperLogLog {
                    value: merged,
                    expires_at: None,
                },
            );
        }

        self.bump_key_version(destkey);
        Ok(())
    }
}
