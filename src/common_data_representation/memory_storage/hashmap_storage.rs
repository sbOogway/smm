use std::collections::HashMap;
use std::sync::RwLock;

use super::MemoryStorage;

pub struct HashMapStorage<V> {
    inner: RwLock<HashMap<String, V>>,
}

impl<V> HashMapStorage<V> {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }
}

impl<V: Clone + Send + Sync> MemoryStorage<V> for HashMapStorage<V> {
    fn set(&self, key: String, value: V) {
        self.inner.write().unwrap().insert(key, value);
    }

    fn get(&self, key: &str) -> Option<V> {
        self.inner.read().unwrap().get(key).cloned()
    }
}
