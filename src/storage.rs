use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::Path;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};
use dashmap::DashMap;
use rand::{seq::IteratorRandom, thread_rng};

#[derive(Debug, Clone)]
enum StorageValue {
    String {
        value: String,
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
}

#[derive(Clone)]
pub struct Storage {
    data: Arc<DashMap<String, StorageValue>>,
    maxmemory_bytes: Option<u64>,
    last_access: Arc<DashMap<String, u64>>,
    access_counter: Arc<AtomicU64>,
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

    pub fn set(&self, key: String, value: String) {
        self.data.insert(
            key.clone(),
            StorageValue::String {
                value,
                expires_at: None,
            },
        );
        self.touch_key(&key);
        self.maybe_evict_for_write();
    }

    pub fn get(&self, key: &str) -> Option<String> {
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

    pub fn mget(&self, keys: &[String]) -> Vec<Option<String>> {
        keys.iter().map(|k| self.get(k)).collect()
    }

    pub fn mset(&self, pairs: &[(String, String)]) {
        for (k, v) in pairs {
            self.set(k.clone(), v.clone());
        }
    }

    pub fn setnx(&self, key: &str, value: String) -> bool {
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
                StorageValue::String { value, expires_at: None }
            });
        if inserted {
            self.touch_key(key);
            self.maybe_evict_for_write();
        }
        inserted
    }

    pub fn set_with_expire_seconds(&self, key: String, value: String, seconds: i64) {
        self.set(key.clone(), value);
        self.expire_seconds(&key, seconds);
    }

    pub fn set_with_expire_millis(&self, key: String, value: String, millis: i64) {
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
            .filter(|k| {
                !self.remove_if_expired(k, now) && self.data.contains_key(*k)
            })
            .count()
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
                    value: "0".to_string(),
                    expires_at: None,
                },
            );
        }

        let mut entry = self
            .data
            .entry(key.to_string())
            .or_insert(StorageValue::String {
                value: "0".to_string(),
                expires_at: None,
            });

        let current_val = match entry.value_mut() {
            StorageValue::String { value, .. } => value,
            _ => return Err(()), // Key exists but is not a string
        };

        let value: i64 = current_val.parse().map_err(|_| ())?;
        let new_val = value.checked_add(delta).ok_or(())?;
        *current_val = new_val.to_string();
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
            },
        )
    }

    pub fn keys(&self, pattern: &str) -> Vec<String> {
        // Very simple implementation: only support "*" (all keys) and exact match.
        if pattern == "*" {
            let now = Instant::now();
            let mut all: Vec<String> = self
                .data
                .iter()
                .map(|entry| entry.key().clone())
                .filter(|k| !self.remove_if_expired(k, now))
                .collect();
            all.sort();
            all
        } else {
            let mut result = Vec::new();
            let now = Instant::now();
            if !self.remove_if_expired(pattern, now) && self.data.contains_key(pattern) {
                result.push(pattern.to_string());
            }
            result
        }
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
        let keys: Vec<String> = self
            .data
            .iter()
            .map(|entry| entry.key().clone())
            .collect();

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
            };

            file.write_all(&[type_byte])?;
            file.write_all(&expires_millis.to_le_bytes())?;

            let key_bytes = key.as_bytes();
            let key_len = key_bytes.len() as u32;
            file.write_all(&key_len.to_le_bytes())?;
            file.write_all(key_bytes)?;

            match value {
                StorageValue::String { value, .. } => {
                    let v_bytes = value.as_bytes();
                    let v_len = v_bytes.len() as u32;
                    file.write_all(&v_len.to_le_bytes())?;
                    file.write_all(v_bytes)?;
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
                    let s = match String::from_utf8(v) {
                        Ok(s) => s,
                        Err(_) => {
                            return Ok(());
                        }
                    };
                    StorageValue::String { value: s, expires_at }
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
                    StorageValue::List { value: list, expires_at }
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
                    StorageValue::Set { value: set, expires_at }
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
                    StorageValue::Hash { value: map, expires_at }
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
            | StorageValue::Hash { expires_at, .. } => {
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
            | StorageValue::Hash { expires_at, .. } => {
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
            | StorageValue::Hash { expires_at, .. } => expires_at,
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
            | StorageValue::Hash { expires_at, .. } => expires_at,
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
            | StorageValue::Hash { expires_at, .. } => {
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
        let StorageValue::Set { value: smallest_set, .. } = entry.value() else {
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
                let StorageValue::Set { value: other_set, .. } = other_entry.value() else {
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
            let ts = self
                .last_access
                .get(&k)
                .map(|v| *v.value())
                .unwrap_or(0);

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
                StorageValue::List { value: list, .. } => {
                    list.iter().map(|v| v.len() as u64).sum()
                }
                StorageValue::Set { value: set, .. } => {
                    set.iter().map(|v| v.len() as u64).sum()
                }
                StorageValue::Hash { value: map, .. } => {
                    map.iter().map(|(k, v)| (k.len() + v.len()) as u64).sum()
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
