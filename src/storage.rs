use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, RwLock};

#[derive(Default)]
struct Inner {
    strings: HashMap<String, String>,
    lists: HashMap<String, VecDeque<String>>,
    sets: HashMap<String, HashSet<String>>,
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
                    if inner.sets.remove(key).is_some() {
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
                    inner.strings.contains_key(k)
                        || inner.lists.contains_key(k)
                        || inner.sets.contains_key(k)
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
                } else if inner.sets.contains_key(key) {
                    "set".to_string()
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
                        .chain(inner.sets.keys())
                        .cloned()
                        .collect();
                    all.sort();
                    all.dedup();
                    all
                } else {
                    let mut result = Vec::new();
                    if inner.strings.contains_key(pattern)
                        || inner.lists.contains_key(pattern)
                        || inner.sets.contains_key(pattern)
                    {
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

    pub fn lpop(&self, key: &str) -> Option<String> {
        let mut guard = match self.inner.write() {
            Ok(g) => g,
            Err(_) => return None,
        };

        let list = guard.lists.get_mut(key)?;
        list.pop_front()
    }

    pub fn rpop(&self, key: &str) -> Option<String> {
        let mut guard = match self.inner.write() {
            Ok(g) => g,
            Err(_) => return None,
        };

        let list = guard.lists.get_mut(key)?;
        list.pop_back()
    }

    pub fn sadd(&self, key: &str, members: &[String]) -> usize {
        let mut guard = match self.inner.write() {
            Ok(g) => g,
            Err(_) => return 0,
        };

        let set = guard.sets.entry(key.to_string()).or_insert_with(HashSet::new);
        let mut added = 0;
        for m in members {
            if set.insert(m.clone()) {
                added += 1;
            }
        }
        added
    }

    pub fn srem(&self, key: &str, members: &[String]) -> usize {
        let mut guard = match self.inner.write() {
            Ok(g) => g,
            Err(_) => return 0,
        };

        let Some(set) = guard.sets.get_mut(key) else {
            return 0;
        };

        let mut removed = 0;
        for m in members {
            if set.remove(m) {
                removed += 1;
            }
        }
        removed
    }

    pub fn smembers(&self, key: &str) -> Vec<String> {
        let guard = match self.inner.read() {
            Ok(g) => g,
            Err(_) => return Vec::new(),
        };

        let Some(set) = guard.sets.get(key) else {
            return Vec::new();
        };

        let mut members: Vec<String> = set.iter().cloned().collect();
        members.sort();
        members
    }

    pub fn scard(&self, key: &str) -> usize {
        let guard = match self.inner.read() {
            Ok(g) => g,
            Err(_) => return 0,
        };
        guard.sets.get(key).map(|s| s.len()).unwrap_or(0)
    }

    pub fn sismember(&self, key: &str, member: &str) -> bool {
        let guard = match self.inner.read() {
            Ok(g) => g,
            Err(_) => return false,
        };
        guard
            .sets
            .get(key)
            .map(|s| s.contains(member))
            .unwrap_or(false)
    }

    pub fn sunion(&self, keys: &[String]) -> Vec<String> {
        let guard = match self.inner.read() {
            Ok(g) => g,
            Err(_) => return Vec::new(),
        };

        let mut result: HashSet<String> = HashSet::new();
        for key in keys {
            if let Some(set) = guard.sets.get(key) {
                for m in set {
                    result.insert(m.clone());
                }
            }
        }

        let mut members: Vec<String> = result.into_iter().collect();
        members.sort();
        members
    }

    pub fn sinter(&self, keys: &[String]) -> Vec<String> {
        let guard = match self.inner.read() {
            Ok(g) => g,
            Err(_) => return Vec::new(),
        };

        if keys.is_empty() {
            return Vec::new();
        }

        // 收集所有存在的集合引用，如果有任意一个 key 不存在，则交集为空。
        let mut existing_sets = Vec::with_capacity(keys.len());
        for key in keys {
            match guard.sets.get(key) {
                Some(set) => existing_sets.push(set),
                None => return Vec::new(),
            }
        }

        // 选择元素最少的集合作为基准集合，降低整体扫描量。
        let (min_index, min_set) = existing_sets
            .iter()
            .enumerate()
            .min_by_key(|(_, set)| set.len())
            .unwrap();

        let mut result: HashSet<String> = HashSet::new();
        'outer: for member in min_set.iter() {
            for (idx, set) in existing_sets.iter().enumerate() {
                if idx == min_index {
                    continue;
                }
                if !set.contains(member) {
                    continue 'outer;
                }
            }
            result.insert(member.clone());
        }

        let mut members: Vec<String> = result.into_iter().collect();
        members.sort();
        members
    }
}
