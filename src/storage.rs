use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Clone, Default)]
pub struct Storage {
    inner: Arc<RwLock<HashMap<String, String>>>,
}

impl Storage {
    pub fn set(&self, key: String, value: String) {
        if let Ok(mut map) = self.inner.write() {
            map.insert(key, value);
        }
    }

    pub fn get(&self, key: &str) -> Option<String> {
        match self.inner.read() {
            Ok(map) => map.get(key).cloned(),
            Err(_) => None,
        }
    }

    pub fn del(&self, keys: &[String]) -> usize {
        match self.inner.write() {
            Ok(mut map) => {
                let mut removed = 0;
                for key in keys {
                    if map.remove(key).is_some() {
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
            Ok(map) => keys.iter().filter(|k| map.contains_key(k.as_str())).count(),
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
        let current = guard.get(key).map(String::as_str).unwrap_or("0");
        let value: i64 = current.parse().map_err(|_| ())?;
        let new = value
            .checked_add(delta)
            .ok_or(())?;
        guard.insert(key.to_string(), new.to_string());
        Ok(new)
    }

    pub fn type_of(&self, key: &str) -> String {
        match self.inner.read() {
            Ok(map) => {
                if map.contains_key(key) {
                    "string".to_string()
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
            Ok(map) => {
                if pattern == "*" {
                    map.keys().cloned().collect()
                } else {
                    map.keys()
                        .filter(|k| k.as_str() == pattern)
                        .cloned()
                        .collect()
                }
            }
            Err(_) => Vec::new(),
        }
    }
}
