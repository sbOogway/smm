use std::{future::Future, pin::Pin};

use disruptor::{MultiProducer, Producer, SingleConsumerBarrier};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use rust_decimal::{Decimal, prelude::Zero};
use std::str::FromStr;

use crate::{
    common_data_representation::message::{BboUpdate, Message as AppMessage, TradeUpdate}, config::HyperliquidConfig, exchange::{DataProvider, Exchange, Executor, Infos},
};

const WS_URL: &str = "wss://api.hyperliquid.xyz/ws";

pub struct Hyperliquid {
    coins: Vec<String>,
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

#[derive(Deserialize)]
struct WsBbo {
    coin: String,
    time: u64,
    bbo: [Option<WsLevel>; 2],
}

#[derive(Deserialize)]
struct WsLevel {
    #[serde(rename = "px")]
    px: String,
    #[serde(rename = "sz")]
    sz: String,
}

impl Hyperliquid {
    pub fn new(cfg: HyperliquidConfig) -> Self {
        Self { coins: cfg.coins }
    }
}

impl Infos for Hyperliquid {
    fn name(&self) -> String {
        "hyperliquid".to_string()
    }

    fn symbols(&self) -> Vec<String> {
        self.coins.clone()
    }
}

impl DataProvider for Hyperliquid {
    fn listen(
        &self,
        mut disruptor: MultiProducer<AppMessage, SingleConsumerBarrier>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        let coins = self.coins.clone();
        Box::pin(async move {
            let (mut ws_stream, response) = connect_async(WS_URL).await.unwrap();

            tracing::info!(status = %response.status(), "connected");

            let sub_message = |kind: &str, coin: &str| -> Message {
                let sub = Subscription {
                    method: "subscribe".into(),
                    subscription: SubscriptionParams {
                        kind: kind.into(),
                        coin: coin.into(),
                    },
                };
                Message::Text(serde_json::to_string(&sub).unwrap().into())
            };

            for coin in &coins {
                ws_stream.send(sub_message("trades", coin)).await.unwrap();
            }
            for coin in &coins {
                ws_stream.send(sub_message("bbo", coin)).await.unwrap();
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

                    match root.channel.as_str() {
                        "trades" => {
                            let trades: Vec<WsTrade> = match serde_json::from_value(root.data) {
                                Ok(t) => t,
                                Err(_) => continue,
                            };
                            tracing::debug!(number = trades.len(), "trades received");

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
                                let trade_msg = AppMessage::TradeUpdate(TradeUpdate {
                                    exchange: "hyperliquid".into(),
                                    symbol: trade.coin,
                                    side: trade.side,
                                    price,
                                    size,
                                    time: trade.time,
                                });
                                disruptor.publish(|slot: &mut AppMessage| {
                                    *slot = trade_msg;
                                });
                            }
                        }
                        "bbo" => {
                            let bbo: WsBbo = match serde_json::from_value(root.data) {
                                Ok(b) => b,
                                Err(_) => continue,
                            };
                            tracing::debug!(coin = %bbo.coin, "bbo received");

                            let (bid, ask) = match bbo.bbo {
                                [Some(b), Some(a)] => (b, a),
                                _ => continue,
                            };

                            let bid_price = match Decimal::from_str(&bid.px) {
                                Ok(p) => p,
                                Err(e) => {
                                    tracing::warn!(raw = %bid.px, error = %e, "failed to parse bid price");
                                    continue;
                                }
                            };
                            let bid_size = match Decimal::from_str(&bid.sz) {
                                Ok(s) => s,
                                Err(e) => {
                                    tracing::warn!(raw = %bid.sz, error = %e, "failed to parse bid size");
                                    continue;
                                }
                            };
                            let ask_price = match Decimal::from_str(&ask.px) {
                                Ok(p) => p,
                                Err(e) => {
                                    tracing::warn!(raw = %ask.px, error = %e, "failed to parse ask price");
                                    continue;
                                }
                            };
                            let ask_size = match Decimal::from_str(&ask.sz) {
                                Ok(s) => s,
                                Err(e) => {
                                    tracing::warn!(raw = %ask.sz, error = %e, "failed to parse ask size");
                                    continue;
                                }
                            };

                            let bbo_msg = AppMessage::BboUpdate(BboUpdate {
                                exchange: "hyperliquid".into(),
                                symbol: bbo.coin,
                                bid_price,
                                bid_size,
                                ask_price,
                                ask_size,
                                time: bbo.time,
                                mid_price: Decimal::zero()
                            });
                            disruptor.publish(|slot: &mut AppMessage| {
                                *slot = bbo_msg;
                            });
                        }
                        _ => continue,
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

impl Exchange for Hyperliquid {}

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
