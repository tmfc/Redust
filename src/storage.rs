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
}
