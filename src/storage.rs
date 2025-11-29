use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use dashmap::DashMap;

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
}

#[derive(Clone)]
pub struct Storage {
    data: Arc<DashMap<String, StorageValue>>,
}

impl Default for Storage {
    fn default() -> Self {
        Storage {
            data: Arc::new(DashMap::new()),
        }
    }
}

impl Storage {
    pub fn set(&self, key: String, value: String) {
        self.data.insert(
            key,
            StorageValue::String {
                value,
                expires_at: None,
            },
        );
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return None;
        }

        self.data.get(key).and_then(|entry| {
            if let StorageValue::String { value, .. } = entry.value() {
                Some(value.clone())
            } else {
                None // Key exists but is not a string
            }
        })
    }

    pub fn del(&self, keys: &[String]) -> usize {
        let mut removed = 0;
        for key in keys {
            if self.data.remove(key).is_some() {
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

    fn incr_by(&self, key: &str, delta: i64) -> Result<i64, ()> {
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

    pub fn lpush(&self, key: &str, values: &[String]) -> usize {
        self.push_internal(key, values, true)
    }

    pub fn rpush(&self, key: &str, values: &[String]) -> usize {
        self.push_internal(key, values, false)
    }

    fn push_internal(&self, key: &str, values: &[String], left: bool) -> usize {
        let now = Instant::now();
        self.remove_if_expired(key, now);

        let mut entry = self
            .data
            .entry(key.to_string())
            .or_insert_with(|| StorageValue::List {
                value: VecDeque::new(),
                expires_at: None,
            });

        match entry.value_mut() {
            StorageValue::List { value: list, .. } => {
                for v in values {
                    if left {
                        list.push_front(v.clone());
                    } else {
                        list.push_back(v.clone());
                    }
                }
                list.len()
            }
            _ => 0, // Key exists but is not a list
        }
    }

    pub fn lrange(&self, key: &str, start: isize, stop: isize) -> Vec<String> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return Vec::new();
        }

        let entry = self.data.get(key);
        let list = match entry.as_ref().map(|e| e.value()) {
            Some(StorageValue::List { value, .. }) => value,
            _ => return Vec::new(), // Key does not exist or is not a list
        };

        let len = list.len() as isize;
        if len == 0 {
            return Vec::new();
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
            return Vec::new();
        }

        let start_idx = s as usize;
        let end_idx = e as usize;

        list.iter()
            .skip(start_idx)
            .take(end_idx - start_idx + 1)
            .cloned()
            .collect()
    }

    pub fn lpop(&self, key: &str) -> Option<String> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return None;
        }

        let mut entry = self.data.get_mut(key)?;
        match entry.value_mut() {
            StorageValue::List { value: list, .. } => list.pop_front(),
            _ => None, // Key exists but is not a list
        }
    }

    pub fn rpop(&self, key: &str) -> Option<String> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return None;
        }

        let mut entry = self.data.get_mut(key)?;
        match entry.value_mut() {
            StorageValue::List { value: list, .. } => list.pop_back(),
            _ => None, // Key exists but is not a list
        }
    }

    pub fn sadd(&self, key: &str, members: &[String]) -> usize {
        let now = Instant::now();
        self.remove_if_expired(key, now);

        let mut entry = self
            .data
            .entry(key.to_string())
            .or_insert_with(|| StorageValue::Set {
                value: HashSet::new(),
                expires_at: None,
            });

        match entry.value_mut() {
            StorageValue::Set { value: set, .. } => {
                let mut added = 0;
                for m in members {
                    if set.insert(m.clone()) {
                        added += 1;
                    }
                }
                added
            }
            _ => 0, // Key exists but is not a set
        }
    }

    pub fn srem(&self, key: &str, members: &[String]) -> usize {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return 0;
        }

        if let Some(mut entry) = self.data.get_mut(key) {

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
                _ => 0, // Key exists but is not a set
            }
        } else {
            0 // Key does not exist
        }
    }

    pub fn smembers(&self, key: &str) -> Vec<String> {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return Vec::new();
        }

        let entry = self.data.get(key);
        let set = match entry.as_ref().map(|e| e.value()) {
            Some(StorageValue::Set { value, .. }) => value,
            _ => return Vec::new(), // Key does not exist or is not a set
        };

        let mut members: Vec<String> = set.iter().cloned().collect();
        members.sort();
        members
    }

    pub fn scard(&self, key: &str) -> usize {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return 0;
        }

        self.data.get(key).map_or(0, |entry| {
            if let StorageValue::Set { value: set, .. } = entry.value() {
                set.len()
            } else {
                0 // Key exists but is not a set
            }
        })
    }

    pub fn sismember(&self, key: &str, member: &str) -> bool {
        let now = Instant::now();
        if self.remove_if_expired(key, now) {
            return false;
        }

        self.data.get(key).map_or(false, |entry| {
            if let StorageValue::Set { value: set, .. } = entry.value() {
                set.contains(member)
            } else {
                false // Key exists but is not a set
            }
        })
    }

    pub fn sunion(&self, keys: &[String]) -> Vec<String> {
        let mut result: HashSet<String> = HashSet::new();
        for key in keys {
            let now = Instant::now();
            if self.remove_if_expired(key, now) {
                continue;
            }

            if let Some(entry) = self.data.get(key) {
                if let StorageValue::Set { value: set, .. } = entry.value() {
                    for m in set {
                        result.insert(m.clone());
                    }
                }
            }
        }

        let mut members: Vec<String> = result.into_iter().collect();
        members.sort();
        members
    }

    pub fn expire_seconds(&self, key: &str, seconds: i64) -> bool {
        // Redis 语义：seconds <= 0 视为立刻过期并删除，若 key 存在返回 1
        if seconds <= 0 {
            let existed = self.data.remove(key).is_some();
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
            | StorageValue::Set { expires_at, .. } => {
                *expires_at = Some(deadline);
                true
            }
        }
    }

    pub fn expire_millis(&self, key: &str, millis: i64) -> bool {
        if millis <= 0 {
            let existed = self.data.remove(key).is_some();
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
            | StorageValue::Set { expires_at, .. } => {
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
            | StorageValue::Set { expires_at, .. } => expires_at,
        };

        let Some(deadline) = expires_at else {
            return -1;
        };

        if *deadline <= now {
            // 过期键交给懒删除/定期删除，这里视为不存在
            self.data.remove(key);
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
            | StorageValue::Set { expires_at, .. } => expires_at,
        };

        let Some(deadline) = expires_at else {
            return -1;
        };

        if *deadline <= now {
            self.data.remove(key);
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
            | StorageValue::Set { expires_at, .. } => {
                if expires_at.is_some() {
                    *expires_at = None;
                    true
                } else {
                    false
                }
            }
        }
    }

    pub fn sinter(&self, keys: &[String]) -> Vec<String> {
        if keys.is_empty() {
            return Vec::new();
        }

        // 先扫描一遍，找到最小的集合（按元素个数），同时保证所有 key 都存在且类型为 Set
        let mut smallest_index: Option<usize> = None;
        let mut smallest_len = usize::MAX;

        for (i, key) in keys.iter().enumerate() {
            let now = Instant::now();
            if self.remove_if_expired(key, now) {
                return Vec::new();
            }

            let Some(entry) = self.data.get(key) else {
                return Vec::new(); // Key does not exist
            };

            if let StorageValue::Set { value: set, .. } = entry.value() {
                let len = set.len();
                if len < smallest_len {
                    smallest_len = len;
                    smallest_index = Some(i);
                }
            } else {
                return Vec::new(); // Key exists but is not a set
            }
        }

        let Some(smallest_idx) = smallest_index else {
            return Vec::new();
        };

        // 再次获取最小集合，遍历其元素，并对其他集合做 contains 检查
        let smallest_key = &keys[smallest_idx];
        let now = Instant::now();
        if self.remove_if_expired(smallest_key, now) {
            return Vec::new();
        }

        let Some(entry) = self.data.get(smallest_key) else {
            return Vec::new();
        };
        let StorageValue::Set { value: smallest_set, .. } = entry.value() else {
            return Vec::new();
        };

        let mut result: HashSet<String> = HashSet::new();

        'outer: for member in smallest_set.iter() {
            // 检查该元素是否出现在其他所有集合中
            for (j, key) in keys.iter().enumerate() {
                if j == smallest_idx {
                    continue;
                }

                let now = Instant::now();
                if self.remove_if_expired(key, now) {
                    return Vec::new();
                }

                let Some(other_entry) = self.data.get(key) else {
                    return Vec::new();
                };
                let StorageValue::Set { value: other_set, .. } = other_entry.value() else {
                    return Vec::new();
                };

                if !other_set.contains(member) {
                    continue 'outer;
                }
            }

            result.insert(member.clone());
        }

        let mut members: Vec<String> = result.into_iter().collect();
        members.sort();
        members
    }

    pub fn sdiff(&self, keys: &[String]) -> Vec<String> {
        if keys.is_empty() {
            return Vec::new();
        }

        let first_key = &keys[0];
        let now = Instant::now();
        if self.remove_if_expired(first_key, now) {
            return Vec::new();
        }

        let first_set_entry = self.data.get(first_key);

        let first_set = match first_set_entry.as_ref().map(|e| e.value()) {
            Some(StorageValue::Set { value: s, .. }) => s,
            _ => return Vec::new(), // Key does not exist or is not a set
        };

        let mut result: HashSet<String> = first_set.iter().cloned().collect();

        for key in &keys[1..] {
            let now = Instant::now();
            if self.remove_if_expired(key, now) {
                continue;
            }

            if let Some(entry) = self.data.get(key) {
                if let StorageValue::Set { value: set, .. } = entry.value() {
                    for member in set.iter() {
                        result.remove(member);
                    }
                }
            }
        }

        let mut members: Vec<String> = result.into_iter().collect();
        members.sort();
        members
    }

    fn value_is_expired(value: &StorageValue, now: Instant) -> bool {
        let expires_at = match value {
            StorageValue::String { expires_at, .. } => expires_at,
            StorageValue::List { expires_at, .. } => expires_at,
            StorageValue::Set { expires_at, .. } => expires_at,
        };

        match expires_at {
            Some(when) => now >= *when,
            None => false,
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
            self.data.remove(key);
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
}
