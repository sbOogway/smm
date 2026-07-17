use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::mpsc;

use crate::data::storage::expiration_buffer::ExpirationBuffer;
use crate::data::storage::memory_map::MemoryMap;

static BUF_COUNTER: AtomicU64 = AtomicU64::new(0);

pub struct RedisMemoryMap<V> {
    client: redis::Client,
    tx: mpsc::UnboundedSender<(String, String, Option<Duration>)>,
    _marker: std::marker::PhantomData<V>,
}

impl<V: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static> RedisMemoryMap<V> {
    pub fn new(socket_path: &str) -> Self {
        let client = redis::Client::open(format!("redis+unix://{}", socket_path))
            .expect("invalid redis url");
        let (tx, rx) = mpsc::unbounded_channel();
        let path = socket_path.to_string();
        tokio::spawn(async move {
            Self::background_worker(path, rx).await;
        });
        Self {
            client,
            tx,
            _marker: std::marker::PhantomData,
        }
    }

    async fn background_worker(
        socket_path: String,
        mut rx: mpsc::UnboundedReceiver<(String, String, Option<Duration>)>,
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
            let result = if let Some(ttl) = ttl {
                let secs = ttl.as_secs();
                redis::cmd("SET")
                    .arg(&[&key, &value, "EX", &secs.to_string()])
                    .query_async::<()>(&mut conn)
                    .await
            } else {
                redis::cmd("SET")
                    .arg(&[&key, &value])
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

impl<V: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static> MemoryMap<V>
    for RedisMemoryMap<V>
{
    fn set(&self, key: String, value: V) {
        let json = serde_json::to_string(&value).expect("serialization failed");
        let _ = self.tx.send((key, json, None));
    }

    fn get(&self, key: &str) -> Option<V> {
        let mut conn = self.client.get_connection().ok()?;
        let value: Option<String> = redis::cmd("GET").arg(key).query(&mut conn).ok()?;
        value.map(|v| serde_json::from_str(&v).expect("deserialization failed"))
    }
}

pub struct RedisExpirationBuffer<V> {
    client: redis::Client,
    key: String,
    ttl: Duration,
    _marker: std::marker::PhantomData<V>,
}

impl<V: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static>
    RedisExpirationBuffer<V>
{
    pub fn new(socket_path: &str, ttl: Duration) -> Self {
        let client = redis::Client::open(format!("redis+unix://{}", socket_path))
            .expect("invalid redis url");
        let id = BUF_COUNTER.fetch_add(1, Ordering::Relaxed);
        let key = format!("mma:expiration_buffer:{id}");
        Self {
            client,
            key,
            ttl,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<V: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static> ExpirationBuffer<V>
    for RedisExpirationBuffer<V>
{
    fn add(&self, value: V) {
        let json = serde_json::to_string(&value).expect("serialization failed");
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        let mut conn = match self.client.get_connection() {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(error = %e, "redis connection failed");
                return;
            }
        };
        let _: Result<(), _> = redis::cmd("ZADD")
            .arg(&self.key)
            .arg(now)
            .arg(&json)
            .query(&mut conn);
    }

    fn get(&self) -> Box<dyn Iterator<Item = V>> {
        let mut conn = match self.client.get_connection() {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(error = %e, "redis connection failed");
                return Box::new(std::iter::empty());
            }
        };
        let min_score = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64()
            - self.ttl.as_secs_f64();
        let values: Vec<String> = match redis::cmd("ZRANGEBYSCORE")
            .arg(&self.key)
            .arg(min_score)
            .arg("+inf")
            .query(&mut conn)
        {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "redis query failed");
                return Box::new(std::iter::empty());
            }
        };
        let _: Result<(), _> = redis::cmd("ZREMRANGEBYSCORE")
            .arg(&self.key)
            .arg("-inf")
            .arg(min_score)
            .query(&mut conn);
        Box::new(
            values
                .into_iter()
                .map(|v| serde_json::from_str(&v).expect("deserialization failed")),
        )
    }
}
