use std::collections::{HashMap, VecDeque};
use std::sync::RwLock;
use std::time::{Duration, Instant};

use super::TtlBufferStorage;

pub struct TtlBufferHashMapStorage<V> {
    ttl: Option<Duration>,
    inner: RwLock<HashMap<String, VecDeque<(Instant, V)>>>,
}

impl<V> TtlBufferHashMapStorage<V> {
    pub fn new(ttl: Option<Duration>) -> Self {
        Self {
            ttl,
            inner: RwLock::new(HashMap::new()),
        }
    }
}

impl<V: Clone + Send + Sync> TtlBufferStorage<V> for TtlBufferHashMapStorage<V> {
    fn set(&self, key: String, value: V) {
        let mut map = self.inner.write().unwrap();
        let deque = map.entry(key).or_insert_with(VecDeque::new);
        deque.push_back((Instant::now(), value));
    }

    fn get(&self, key: &str) -> Option<Vec<V>> {
        let mut map = self.inner.write().unwrap();
        let deque = map.get_mut(key)?;

        while let Some(front) = deque.front() {
            match self.ttl {
                Some(ttl) if front.0.elapsed() >= ttl => {
                    deque.pop_front();
                }
                _ => break,
            }
        }

        if deque.is_empty() {
            return None;
        }

        let values: Vec<V> = deque.iter().map(|(_, v)| v.clone()).collect();
        Some(values)
    }
}
