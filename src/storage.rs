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
}

#[derive(Clone)]
pub struct Storage {
    data: Arc<DashMap<String, StorageValue>>,
    maxmemory_bytes: Option<u64>,
    last_access: Arc<DashMap<String, u64>>,
    access_counter: Arc<AtomicU64>,
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

impl Default for Storage {
    fn default() -> Self {
        Storage::new(None)
    }
}

impl Storage {
    pub fn new(maxmemory_bytes: Option<u64>) -> Self {
        Storage {
            data: Arc::new(DashMap::new()),
            maxmemory_bytes,
            last_access: Arc::new(DashMap::new()),
            access_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn flushdb(&self, db: u8) {
        let prefix = format!("{}:", db);
        let keys_to_remove: Vec<String> = self
            .data
            .iter()
            .map(|e| e.key().clone())
            .filter(|k| k.starts_with(&prefix))
            .collect();

        for k in keys_to_remove {
            self.data.remove(&k);
            self.last_access.remove(&k);
        }
    }

    pub fn flushall(&self) {
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

        if added > 0 {
            self.touch_key(key);
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
        }

        Ok(removed)
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
        if len > 0 {
            self.touch_key(key);
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

        if new_len > 0 {
            self.touch_key(key);
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
                removed += 1;
            }
        }
        removed
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

        // 插入新 key，保留 TTL 信息
        self.data.insert(to.to_string(), value);
        self.touch_key(to);

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
        self.data.insert(to.to_string(), value);
        self.touch_key(to);

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

        if len > 0 {
            self.touch_key(key);
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
        // ltrim 只会收缩列表，不会增加内存，这里不触发淘汰

        Ok(())
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
        if len > 0 {
            self.touch_key(dest);
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
                StorageValue::Zset { .. } => {
                    // 当前 RDB v1 不支持 ZSET，直接跳过该 key
                    continue;
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
                StorageValue::Zset { .. } => {
                    // 上面 match 已经 continue，这里理论上不会到达
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
                    // RDB v1 尚未定义 ZSET 类型，遇到未知类型时直接结束加载，视为旧版本文件
                    return Ok(());
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
            | StorageValue::Zset { expires_at, .. } => {
                *expires_at = Some(deadline);
                true
            }
        }
    }

    pub fn expire_millis(&self, key: &str, millis: i64) -> bool {
        if millis <= 0 {
            let existed = self.data.remove(key).is_some();
            if existed {
                self.last_access.remove(key);
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
            | StorageValue::Zset { expires_at, .. } => {
                *expires_at = Some(deadline);
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
            | StorageValue::Zset { expires_at, .. } => expires_at,
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
            | StorageValue::Zset { expires_at, .. } => expires_at,
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
            | StorageValue::Zset { expires_at, .. } => {
                if expires_at.is_some() {
                    *expires_at = None;
                    true
                } else {
                    false
                }
            }
        }
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
        true
    }

    pub fn maybe_evict_for_write(&self) {
        let Some(limit) = self.maxmemory_bytes else {
            return;
        };

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
            };

            total = total.saturating_add(key_size.saturating_add(value_size));
        }

        total
    }

    pub fn maxmemory_bytes(&self) -> Option<u64> {
        self.maxmemory_bytes
    }
}
