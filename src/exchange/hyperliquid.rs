use std::{future::Future, pin::Pin};

use disruptor::{MultiProducer, Producer, SingleConsumerBarrier};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use rust_decimal::Decimal;
use std::str::FromStr;

use crate::{common_data_representation::price_update::PriceUpdate, exchange::traits::{DataProvider, Exchange, Executor}};

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
