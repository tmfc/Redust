use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};

#[derive(Default)]
struct Inner {
    strings: HashMap<String, String>,
    lists: HashMap<String, VecDeque<String>>,
}

#[derive(Clone, Default)]
pub struct Storage {
    inner: Arc<RwLock<Inner>>,
}

impl Storage {
    pub fn set(&self, key: String, value: String) {
        if let Ok(mut inner) = self.inner.write() {
            // 覆盖字符串值时，保留列表键空间独立存在
            inner.strings.insert(key, value);
        }
    }

    pub fn get(&self, key: &str) -> Option<String> {
        match self.inner.read() {
            Ok(inner) => inner.strings.get(key).cloned(),
            Err(_) => None,
        }
    }

    pub fn del(&self, keys: &[String]) -> usize {
        match self.inner.write() {
            Ok(mut inner) => {
                let mut removed = 0;
                for key in keys {
                    let mut deleted = false;
                    if inner.strings.remove(key).is_some() {
                        deleted = true;
                    }
                    if inner.lists.remove(key).is_some() {
                        deleted = true;
                    }
                    if deleted {
                        removed += 1;
                    }
                }
                removed
            }
            Err(_) => 0,
        }
    }

    pub fn exists(&self, keys: &[String]) -> usize {
        match self.inner.read() {
            Ok(inner) => keys
                .iter()
                .filter(|k| {
                    let k = k.as_str();
                    inner.strings.contains_key(k) || inner.lists.contains_key(k)
                })
                .count(),
            Err(_) => 0,
        }
    }

    pub fn incr(&self, key: &str) -> Result<i64, ()> {
        self.incr_by(key, 1)
    }

    pub fn decr(&self, key: &str) -> Result<i64, ()> {
        self.incr_by(key, -1)
    }

    fn incr_by(&self, key: &str, delta: i64) -> Result<i64, ()> {
        let mut guard = self.inner.write().map_err(|_| ())?;
        let current = guard
            .strings
            .get(key)
            .map(String::as_str)
            .unwrap_or("0");
        let value: i64 = current.parse().map_err(|_| ())?;
        let new = value
            .checked_add(delta)
            .ok_or(())?;
        guard.strings.insert(key.to_string(), new.to_string());
        Ok(new)
    }

    pub fn type_of(&self, key: &str) -> String {
        match self.inner.read() {
            Ok(inner) => {
                if inner.strings.contains_key(key) {
                    "string".to_string()
                } else if inner.lists.contains_key(key) {
                    "list".to_string()
                } else {
                    "none".to_string()
                }
            }
            Err(_) => "none".to_string(),
        }
    }

    pub fn keys(&self, pattern: &str) -> Vec<String> {
        // Very simple implementation: only support "*" (all keys) and exact match.
        match self.inner.read() {
            Ok(inner) => {
                if pattern == "*" {
                    let mut all: Vec<String> = inner
                        .strings
                        .keys()
                        .chain(inner.lists.keys())
                        .cloned()
                        .collect();
                    all.sort();
                    all.dedup();
                    all
                } else {
                    let mut result = Vec::new();
                    if inner.strings.contains_key(pattern) || inner.lists.contains_key(pattern) {
                        result.push(pattern.to_string());
                    }
                    result
                }
            }
            Err(_) => Vec::new(),
        }
    }

    pub fn lpush(&self, key: &str, values: &[String]) -> usize {
        self.push_internal(key, values, true)
    }

    pub fn rpush(&self, key: &str, values: &[String]) -> usize {
        self.push_internal(key, values, false)
    }

    fn push_internal(&self, key: &str, values: &[String], left: bool) -> usize {
        let mut guard = match self.inner.write() {
            Ok(g) => g,
            Err(_) => return 0,
        };

        let list = guard.lists.entry(key.to_string()).or_insert_with(VecDeque::new);
        for v in values {
            if left {
                list.push_front(v.clone());
            } else {
                list.push_back(v.clone());
            }
        }
        list.len()
    }

    pub fn lrange(&self, key: &str, start: isize, stop: isize) -> Vec<String> {
        let guard = match self.inner.read() {
            Ok(g) => g,
            Err(_) => return Vec::new(),
        };

        let Some(list) = guard.lists.get(key) else {
            return Vec::new();
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

        list
            .iter()
            .skip(start_idx)
            .take(end_idx - start_idx + 1)
            .cloned()
            .collect()
    }
}
