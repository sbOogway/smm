//! hyperliquid exchange implementation.
//!
//! <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/>

use std::{future::Future, pin::Pin, sync::OnceLock};
use tokio::net::TcpStream;

use disruptor::{MultiProducer, Producer, SingleConsumerBarrier};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

use rust_decimal::{Decimal, prelude::Zero};
use std::str::FromStr;

use crate::{
    common_data_representation::message::{BboUpdate, Message as AppMessage, TradeUpdate},
    config::HyperliquidConfig,
    exchange::{DataProvider, Exchange, Executor, Infos},
};

/// <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket>
const WS_URL_MAINNET: &str = "wss://api.hyperliquid.xyz/ws";
const WS_URL_TESTNET: &str = "wss://api.hyperliquid-testnet.xyz/ws";

static WS_TX: OnceLock<mpsc::Sender<String>> = OnceLock::new();

pub struct Hyperliquid {
    coins: Vec<String>,
    ws_url: String,
    address: String,
}

/// <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions#subscription-messages>
#[derive(Serialize)]
struct Subscription {
    method: String,
    subscription: SubscriptionParams,
}

/// <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions>
#[derive(Serialize)]
struct SubscriptionParams {
    #[serde(rename = "type")]
    kind: String,
    coin: String,
}

/// <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions#data-formats>
#[derive(Deserialize)]
struct WsMessage {
    channel: String,
    data: serde_json::Value,
}

/// <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions#9-trades>
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

/// <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions#19-bbo>
#[derive(Deserialize)]
struct WsBbo {
    coin: String,
    time: u64,
    bbo: [Option<WsLevel>; 2],
}

/// <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions#data-type-definitions>
#[derive(Deserialize)]
struct WsLevel {
    #[serde(rename = "px")]
    px: String,
    #[serde(rename = "sz")]
    sz: String,
}

impl Hyperliquid {
    pub fn new(cfg: HyperliquidConfig) -> Self {
        let ws_url = if cfg.mainnet {
            WS_URL_MAINNET
        } else {
            WS_URL_TESTNET
        };
        Self {
            coins: cfg.coins,
            ws_url: ws_url.to_string(),
            address: cfg.address,
        }
    }

    async fn connect(
        &self,
    ) -> (
        WebSocketStream<MaybeTlsStream<TcpStream>>,
        mpsc::Receiver<String>,
    ) {
        let (tx, rx) = mpsc::channel(256);
        WS_TX.set(tx).ok();

        let (mut ws_stream, response) = connect_async(&self.ws_url).await.unwrap();
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

        for coin in &self.coins {
            ws_stream.send(sub_message("trades", coin)).await.unwrap();
        }
        for coin in &self.coins {
            ws_stream.send(sub_message("bbo", coin)).await.unwrap();
        }

        (ws_stream, rx)
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
        Box::pin(async move {
            let (mut ws_stream, mut rx) = self.connect().await;

            loop {
                tokio::select! {
                    Some(msg) = ws_stream.next() => {
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
                                    mid_price: Decimal::zero(),
                                });
                                disruptor.publish(|slot: &mut AppMessage| {
                                    *slot = bbo_msg;
                                });
                            }
                            _ => continue,
                        }
                    }
                    Some(text) = rx.recv() => {
                        ws_stream.send(Message::Text(text.into())).await.unwrap();
                    }
                }
            }
        })
    }
}

impl Executor for Hyperliquid {
    fn create_order(&self) {
        todo!()
    }

    fn update_order(&self) {
        todo!()
    }

    fn cancel_order(&self) {
        todo!()
    }

    fn balance_of(&self, _symbol: String) {
        let request = serde_json::json!({
            "method": "post",
            "id": 0,
            "request": {
                "type": "info",
                "payload": {
                    "type": "clearinghouseState",
                    "user": self.address,
                },
            },
        });

        if let Some(ws_tx) = WS_TX.get() {
            let _ = ws_tx.try_send(request.to_string());
        }
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
        let (mut ws_stream, response) = connect_async(WS_URL_MAINNET)
            .await
            .expect("failed to connect");

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
        let (mut ws_stream, response) = connect_async(WS_URL_MAINNET)
            .await
            .expect("failed to connect");

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

    #[tokio::test]
    async fn balance_of_request() {
        let (mut ws_stream, response) = connect_async(WS_URL_MAINNET)
            .await
            .expect("failed to connect");

        assert!(
            response.status().is_success() || response.status().as_u16() == 101,
            "unexpected status: {}",
            response.status(),
        );

        let request = serde_json::json!({
            "method": "post",
            "id": 1,
            "request": {
                "type": "info",
                "payload": {
                    "type": "clearinghouseState",
                    "user": "0x83473ff587C2aeE4a55Fb3c396bD981D469f4236",
                },
            },
        });

        ws_stream
            .send(Message::Text(
                serde_json::to_string(&request).unwrap().into(),
            ))
            .await
            .expect("failed to send post request");

        let msg = tokio::time::timeout(std::time::Duration::from_secs(5), ws_stream.next())
            .await
            .expect("timeout waiting for response")
            .expect("stream ended")
            .expect("message error");

        let text = match msg {
            Message::Text(t) => t,
            _ => panic!("expected text message"),
        };

        let root: WsMessage = serde_json::from_str(&text).unwrap();
        assert_eq!(root.channel, "post");

        let data = root.data;
        assert_eq!(data["id"], 1);

        let response_type = data["response"]["type"].as_str().unwrap();
        assert!(
            response_type == "info" || response_type == "error",
            "unexpected response type: {}",
            response_type,
        );

        if response_type == "info" {
            let result = &data["response"]["payload"]["data"];
            println!("balance response: {result}");
            assert!(result["assetPositions"].is_array());
            assert!(result["marginSummary"].is_object());
        }
    }
}
