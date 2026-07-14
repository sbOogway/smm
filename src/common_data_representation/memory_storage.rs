pub mod hashmap_storage;
pub mod redis_storage;

use self::{hashmap_storage::HashMapStorage, redis_storage::RedisStorage};

pub trait MemoryStorage<V>: Send + Sync {
    fn set(&self, key: String, value: V);
    fn get(&self, key: &str) -> Option<V>;
}

use std::fmt::Display;

pub fn new_storage<V: Clone + Display + Send + Sync + 'static>(
    cfg: &crate::config::MemoryStorageConfig,
) -> Box<dyn MemoryStorage<V>> {
    match cfg.backend.as_str() {
        "hashmap" => Box::new(HashMapStorage::new()),
        "redis" => Box::new(RedisStorage::new(
            &cfg.redis
                .as_ref()
                .expect("redis config required for redis backend")
                .socket_path,
        )),
        other => panic!("unknown memory_storage backend: {other}"),
    }
}
