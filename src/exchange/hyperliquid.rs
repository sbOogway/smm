use std::{future::Future, pin::Pin};

use disruptor::{MultiProducer, Producer, SingleConsumerBarrier};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use rust_decimal::Decimal;
use std::str::FromStr;

use crate::{
    common_data_representation::price_update::PriceUpdate,
    exchange::{DataProvider, Exchange, Executor},
};

const WS_URL: &str = "wss://api.hyperliquid.xyz/ws";

pub struct Hyperliquid {
    coins: Vec<String>,
    ws_url: String,
}

#[derive(Serialize)]
struct Subscription {
    method: String,
    subscription: SubscriptionParams,
}

#[derive(Serialize)]
struct SubscriptionParams {
    #[serde(rename = "type")]
    kind: String,
    coin: String,
}

#[derive(Deserialize)]
struct WsMessage {
    channel: String,
    data: serde_json::Value,
}

#[derive(Deserialize)]
struct WsTrade {
    coin: String,
    side: String,
    #[serde(rename = "px")]
    price: String,
    #[serde(rename = "sz")]
    size: String,
    time: u64,
}

impl Hyperliquid {
    pub fn new(coins: Vec<String>) -> Self {
        Self {
            coins,
            ws_url: WS_URL.into(),
        }
    }
}

impl DataProvider<PriceUpdate> for Hyperliquid {
    fn listen_trades(
        &self,
        mut disruptor: MultiProducer<PriceUpdate, SingleConsumerBarrier>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        let ws_url = self.ws_url.clone();
        let coins = self.coins.clone();
        Box::pin(async move {
            let (mut ws_stream, response) = connect_async(&ws_url).await.unwrap();

            tracing::info!(status = %response.status(), "connected");

            for coin in &coins {
                let sub = Subscription {
                    method: "subscribe".into(),
                    subscription: SubscriptionParams {
                        kind: "trades".into(),
                        coin: coin.clone(),
                    },
                };
                ws_stream
                    .send(Message::Text(serde_json::to_string(&sub).unwrap().into()))
                    .await
                    .unwrap();
            }

            loop {
                if let Some(msg) = ws_stream.next().await {
                    let text = match msg {
                        Ok(Message::Text(t)) => t,
                        _ => continue,
                    };

                    let root: WsMessage = match serde_json::from_str(&text) {
                        Ok(m) => m,
                        Err(_) => continue,
                    };

                    if root.channel != "trades" {
                        continue;
                    }

                    let trades: Vec<WsTrade> = match serde_json::from_value(root.data) {
                        Ok(t) => t,
                        Err(_) => continue,
                    };

                    for trade in trades {
                        let price = match Decimal::from_str(&trade.price) {
                            Ok(p) => p,
                            Err(e) => {
                                tracing::warn!(raw = %trade.price, error = %e, "failed to parse price");
                                continue;
                            }
                        };
                        let size = match Decimal::from_str(&trade.size) {
                            Ok(s) => s,
                            Err(e) => {
                                tracing::warn!(raw = %trade.size, error = %e, "failed to parse size");
                                continue;
                            }
                        };
                        disruptor.publish(|slot: &mut PriceUpdate| {
                            slot.exchange = "hyperliquid".into();
                            slot.symbol = trade.coin;
                            slot.side = trade.side;
                            slot.price = price;
                            slot.size = size;
                            slot.time = trade.time;
                        });
                    }
                }
            }
        })
    }
}

impl Executor for Hyperliquid {
    fn send_order(&self) {
        todo!()
    }

    fn cancel_order(&self) {
        todo!()
    }
}

impl Exchange<PriceUpdate> for Hyperliquid {}

#[cfg(test)]
mod tests {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::connect_async;

    use super::*;

    #[tokio::test]
    async fn connect_and_receive_trades() {
        let (mut ws_stream, response) = connect_async(WS_URL).await.expect("failed to connect");

        assert!(
            response.status().is_success() || response.status().as_u16() == 101,
            "unexpected status: {}",
            response.status(),
        );

        let sub = Subscription {
            method: "subscribe".into(),
            subscription: SubscriptionParams {
                kind: "trades".into(),
                coin: "BTC".into(),
            },
        };

        ws_stream
            .send(Message::Text(serde_json::to_string(&sub).unwrap().into()))
            .await
            .expect("failed to send subscription");

        for i in 0..10 {
            let msg = tokio::time::timeout(std::time::Duration::from_secs(5), ws_stream.next())
                .await
                .expect("timeout waiting for message")
                .expect("stream ended");

            let text = match msg {
                Ok(Message::Text(t)) => t,
                Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => continue,
                _ => continue,
            };

            let root: WsMessage = serde_json::from_str(&text).unwrap();
            tracing::info!(channel = %root.channel, "message {i}");

            if root.channel != "trades" {
                continue;
            }

            let trades: Vec<WsTrade> = serde_json::from_value(root.data).unwrap();
            assert!(!trades.is_empty(), "expected at least one trade");

            for trade in &trades {
                let price = Decimal::from_str(&trade.price).expect("failed to parse price");
                let size = Decimal::from_str(&trade.size).expect("failed to parse size");
                assert!(price > Decimal::ZERO, "price must be positive");
                assert!(size > Decimal::ZERO, "size must be positive");
            }

            return;
        }

        panic!("no trade data received after 10 messages");
    }

    #[tokio::test]
    async fn ping_latency_under_500ms() {
        let (mut ws_stream, response) = connect_async(WS_URL).await.expect("failed to connect");

        assert!(
            response.status().is_success() || response.status().as_u16() == 101,
            "unexpected status: {}",
            response.status(),
        );

        let payload: Vec<u8> = std::time::Instant::now()
            .elapsed()
            .as_nanos()
            .to_be_bytes()
            .to_vec();
        let start = std::time::Instant::now();

        ws_stream
            .send(Message::Ping(payload.clone().into()))
            .await
            .expect("failed to send ping");

        loop {
            let msg = tokio::time::timeout(std::time::Duration::from_secs(5), ws_stream.next())
                .await
                .expect("timeout waiting for pong")
                .expect("stream ended")
                .expect("message error");

            match msg {
                Message::Pong(data) if data.as_ref() == payload.as_slice() => break,
                _ => continue,
            }
        }

        let latency = start.elapsed();
        println!("ping latency: {latency:?}");
        assert!(
            latency < std::time::Duration::from_millis(500),
            "ping latency too high: {latency:?}",
        );
    }
}
