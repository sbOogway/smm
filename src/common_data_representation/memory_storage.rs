pub mod hashmap_storage;
pub mod redis_storage;
pub mod ttl_buffer_hashmap_storage;

use std::fmt::Display;
use std::time::Duration;

use self::{
    hashmap_storage::HashMapStorage, redis_storage::RedisStorage,
    ttl_buffer_hashmap_storage::TtlBufferHashMapStorage,
};

pub trait MemoryStorage<V>: Send + Sync {
    fn set(&self, key: String, value: V);
    fn get(&self, key: &str) -> Option<V>;
}

pub trait TtlBufferStorage<V>: Send + Sync {
    fn set(&self, key: String, value: V);
    fn get(&self, key: &str) -> Option<Vec<V>>;
}

pub fn new<V: Clone + Display + Send + Sync + 'static>(
    cfg: &crate::config::MemoryStorageConfig,
    ttl: Option<Duration>,
) -> Box<dyn MemoryStorage<V>> {
    match cfg.backend.as_str() {
        "hashmap" => Box::new(HashMapStorage::new()),
        "redis" => Box::new(RedisStorage::new(
            ttl,
            &cfg.redis
                .as_ref()
                .expect("redis config required for redis backend")
                .socket_path,
        )),
        other => panic!("unknown memory_storage backend: {other}"),
    }
}

pub fn new_ttl_buffer<V: Clone + Send + Sync + 'static>(
    cfg: &crate::config::MemoryStorageConfig,
    ttl: Option<Duration>,
) -> Box<dyn TtlBufferStorage<V>> {
    match cfg.backend.as_str() {
        "ttl_buffer_hashmap" => Box::new(TtlBufferHashMapStorage::new(ttl)),
        other => panic!("unknown ttl buffer backend: {other}"),
    }
}
