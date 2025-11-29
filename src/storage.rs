use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use dashmap::DashMap;

#[derive(Debug, Clone)]
enum StorageValue {
    String(String),
    List(VecDeque<String>),
    Set(HashSet<String>),
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
        self.data.insert(key, StorageValue::String(value));
    }

    pub fn get(&self, key: &str) -> Option<String> {
        self.data.get(key).and_then(|entry| {
            if let StorageValue::String(s) = entry.value() {
                Some(s.clone())
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
        keys.iter()
            .filter(|k| self.data.contains_key(*k))
            .count()
    }

    pub fn incr(&self, key: &str) -> Result<i64, ()> {
        self.incr_by(key, 1)
    }

    pub fn decr(&self, key: &str) -> Result<i64, ()> {
        self.incr_by(key, -1)
    }

    fn incr_by(&self, key: &str, delta: i64) -> Result<i64, ()> {
        let mut entry = self.data.entry(key.to_string()).or_insert(StorageValue::String("0".to_string()));

        let current_val = match entry.value_mut() {
            StorageValue::String(s) => s,
            _ => return Err(()), // Key exists but is not a string
        };

        let value: i64 = current_val.parse().map_err(|_| ())?;
        let new_val = value.checked_add(delta).ok_or(())?;
        *current_val = new_val.to_string();
        Ok(new_val)
    }

    pub fn type_of(&self, key: &str) -> String {
        self.data.get(key).map_or_else(
            || "none".to_string(),
            |entry| match entry.value() {
                StorageValue::String(_) => "string".to_string(),
                StorageValue::List(_) => "list".to_string(),
                StorageValue::Set(_) => "set".to_string(),
            },
        )
    }

    pub fn keys(&self, pattern: &str) -> Vec<String> {
        // Very simple implementation: only support "*" (all keys) and exact match.
        if pattern == "*" {
            let mut all: Vec<String> = self.data.iter().map(|entry| entry.key().clone()).collect();
            all.sort();
            all
        } else {
            let mut result = Vec::new();
            if self.data.contains_key(pattern) {
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
        let mut entry = self.data.entry(key.to_string()).or_insert_with(|| StorageValue::List(VecDeque::new()));

        match entry.value_mut() {
            StorageValue::List(list) => {
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
        let entry = self.data.get(key);
        let list = match entry.as_ref().map(|e| e.value()) {
            Some(StorageValue::List(l)) => l,
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
        let mut entry = self.data.get_mut(key)?;
        match entry.value_mut() {
            StorageValue::List(list) => list.pop_front(),
            _ => None, // Key exists but is not a list
        }
    }

    pub fn rpop(&self, key: &str) -> Option<String> {
        let mut entry = self.data.get_mut(key)?;
        match entry.value_mut() {
            StorageValue::List(list) => list.pop_back(),
            _ => None, // Key exists but is not a list
        }
    }

    pub fn sadd(&self, key: &str, members: &[String]) -> usize {
        let mut entry = self.data.entry(key.to_string()).or_insert_with(|| StorageValue::Set(HashSet::new()));

        match entry.value_mut() {
            StorageValue::Set(set) => {
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
        if let Some(mut entry) = self.data.get_mut(key) {

            match entry.value_mut() {
                StorageValue::Set(set) => {
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
        let entry = self.data.get(key);
        let set = match entry.as_ref().map(|e| e.value()) {
            Some(StorageValue::Set(s)) => s,
            _ => return Vec::new(), // Key does not exist or is not a set
        };

        let mut members: Vec<String> = set.iter().cloned().collect();
        members.sort();
        members
    }

    pub fn scard(&self, key: &str) -> usize {
        self.data.get(key).map_or(0, |entry| {
            if let StorageValue::Set(set) = entry.value() {
                set.len()
            } else {
                0 // Key exists but is not a set
            }
        })
    }

    pub fn sismember(&self, key: &str, member: &str) -> bool {
        self.data.get(key).map_or(false, |entry| {
            if let StorageValue::Set(set) = entry.value() {
                set.contains(member)
            } else {
                false // Key exists but is not a set
            }
        })
    }

    pub fn sunion(&self, keys: &[String]) -> Vec<String> {
        let mut result: HashSet<String> = HashSet::new();
        for key in keys {
            if let Some(entry) = self.data.get(key) {
                if let StorageValue::Set(set) = entry.value() {
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

    pub fn sinter(&self, keys: &[String]) -> Vec<String> {
        if keys.is_empty() {
            return Vec::new();
        }

        let mut existing_sets_data: Vec<HashSet<String>> = Vec::new();
        for key in keys {
            if let Some(entry) = self.data.get(key) {
                if let StorageValue::Set(set) = entry.value() {
                    existing_sets_data.push(set.iter().cloned().collect());
                } else {
                    return Vec::new(); // Key exists but is not a set
                }
            } else {
                return Vec::new(); // Key does not exist
            }
        }

        if existing_sets_data.is_empty() {
            return Vec::new();
        }

        // We already own the sets; start from the first and intersect with the rest
        let mut result: HashSet<String> = existing_sets_data[0].clone();

        for i in 1..existing_sets_data.len() {
            result = result
                .intersection(&existing_sets_data[i])
                .cloned()
                .collect();
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
        let first_set_entry = self.data.get(first_key);

        let first_set = match first_set_entry.as_ref().map(|e| e.value()) {
            Some(StorageValue::Set(s)) => s,
            _ => return Vec::new(), // Key does not exist or is not a set
        };

        let mut result: HashSet<String> = first_set.iter().cloned().collect();

        for key in &keys[1..] {
            if let Some(entry) = self.data.get(key) {
                if let StorageValue::Set(set) = entry.value() {
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
}
