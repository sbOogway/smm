use std::collections::HashMap;
use std::fmt::Display;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use tokio::sync::mpsc;

use super::MemoryStorage;

pub struct RedisStorage<V> {
    ttl: Option<Duration>,
    cache: Arc<RwLock<HashMap<String, (V, Option<Instant>)>>>,
    _tx: mpsc::UnboundedSender<(String, V, Option<Duration>)>,
}

impl<V: Display + Send + Sync + 'static> RedisStorage<V> {
    pub fn new(ttl: Option<Duration>, socket_path: &str) -> Self {
        let cache = Arc::new(RwLock::new(HashMap::new()));
        let (tx, rx) = mpsc::unbounded_channel();
        let cache_clone = cache.clone();
        let path = socket_path.to_string();
        tokio::spawn(async move {
            Self::background_worker(path, rx, cache_clone).await;
        });
        Self { ttl, cache, _tx: tx }
    }

    async fn background_worker(
        socket_path: String,
        mut rx: mpsc::UnboundedReceiver<(String, V, Option<Duration>)>,
        _cache: Arc<RwLock<HashMap<String, (V, Option<Instant>)>>>,
    ) {
        let client = match redis::Client::open(format!("redis+unix://{}", socket_path)) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(error = %e, "invalid redis url");
                return;
            }
        };

        let mut conn = loop {
            match client.get_connection_manager().await {
                Ok(cm) => break cm,
                Err(e) => {
                    tracing::warn!(error = %e, "redis connection failed, retrying in 1s");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        };

        while let Some((key, value, ttl)) = rx.recv().await {
            let value_str = value.to_string();
            let result = if let Some(ttl) = ttl {
                let secs = ttl.as_secs();
                redis::cmd("SET")
                    .arg(&[&key, &value_str, "EX", &secs.to_string()])
                    .query_async::<()>(&mut conn)
                    .await
            } else {
                redis::cmd("SET")
                    .arg(&[&key, &value_str])
                    .query_async::<()>(&mut conn)
                    .await
            };
            if let Err(e) = result {
                tracing::warn!(error = %e, key = %key, "redis set failed");
                match client.get_connection_manager().await {
                    Ok(new_conn) => conn = new_conn,
                    Err(e) => tracing::warn!(error = %e, "redis reconnection failed"),
                }
            }
        }
    }
}

impl<V: Display + Clone + Send + Sync + 'static> MemoryStorage<V> for RedisStorage<V> {
    fn set(&self, key: String, value: V) {
        let expires = self.ttl.map(|d| Instant::now() + d);
        self.cache
            .write()
            .unwrap()
            .insert(key.clone(), (value.clone(), expires));
        if let Err(e) = self._tx.send((key, value, self.ttl)) {
            tracing::warn!(error = %e, "redis background channel send failed");
        }
    }

    fn get(&self, key: &str) -> Option<V> {
        let map = self.cache.read().unwrap();
        match map.get(key) {
            Some((_, Some(expires))) if *expires <= Instant::now() => None,
            Some((value, _)) => Some(value.clone()),
            None => None,
        }
    }
}
