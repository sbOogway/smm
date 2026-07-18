//! `mqtt` module is responsible to publish data to the broker
//!
//! the current design of the system implies that data is only published and never read.

use rumqttc::{AsyncClient, EventLoop, MqttOptions, QoS};
use rust_decimal::prelude::ToPrimitive;
use tokio::sync::mpsc;

use crate::config::MqttConfig;
use crate::exchange::types::message::Message;

pub struct MqttPublisher;

impl MqttPublisher {
    async fn publish(client: &AsyncClient, topic: &str, payload: String) {
        tracing::debug!(topic = %topic, payload = %payload, "mqtt publish");
        if let Err(e) = client.publish(topic, QoS::AtMostOnce, false, payload).await {
            tracing::warn!(error = %e, topic = %topic, "mqtt publish failed");
        }
    }

    async fn publish_to_topic(
        client: &AsyncClient,
        config: &MqttConfig,
        exchange: &str,
        symbol: &str,
        kind: &str,
        price: rust_decimal::Decimal,
    ) {
        let topic = format!("{}/{}/{}/{}", config.topic_prefix, exchange, kind, symbol);
        let payload = serde_json::json!({ kind: price.to_f64().unwrap_or(0.0) }).to_string();
        Self::publish(client, &topic, payload).await;
    }

    pub async fn run(config: MqttConfig, mut rx: mpsc::Receiver<Message>) {
        tracing::info!("mqtt publisher starting: {}/{}", config.broker, config.port);

        let mqttoptions = {
            let mut opts = MqttOptions::new(&config.client_id, &config.broker, config.port);
            opts.set_keep_alive(std::time::Duration::from_secs(30));
            opts.set_clean_session(true);
            opts
        };

        let (client, eventloop) = AsyncClient::new(mqttoptions, 100);

        tokio::spawn(poll_eventloop(eventloop));

        tracing::info!("mqtt publisher ready, waiting for messages");

        while let Some(msg) = rx.recv().await {
            match msg {
                Message::TradeUpdate(t) => {
                    Self::publish_to_topic(
                        &client,
                        &config,
                        &t.exchange,
                        &t.symbol,
                        "price",
                        t.price,
                    )
                    .await;
                }
                Message::BboUpdate(b) => {
                    Self::publish_to_topic(
                        &client,
                        &config,
                        &b.exchange,
                        &b.symbol,
                        "bid",
                        b.bid_price,
                    )
                    .await;
                    Self::publish_to_topic(
                        &client,
                        &config,
                        &b.exchange,
                        &b.symbol,
                        "ask",
                        b.ask_price,
                    )
                    .await;
                    Self::publish_to_topic(
                        &client,
                        &config,
                        &b.exchange,
                        &b.symbol,
                        "mid_price",
                        b.mid_price,
                    )
                    .await;
                }
                Message::AsmmQuote(q) => {
                    Self::publish_to_topic(
                        &client,
                        &config,
                        &q.exchange,
                        &q.symbol,
                        "reservation_price",
                        q.reservation_price,
                    )
                    .await;
                    Self::publish_to_topic(
                        &client,
                        &config,
                        &q.exchange,
                        &q.symbol,
                        "asmm_bid_price",
                        q.asmm_bid_price,
                    )
                    .await;
                    Self::publish_to_topic(
                        &client,
                        &config,
                        &q.exchange,
                        &q.symbol,
                        "asmm_ask_price",
                        q.asmm_ask_price,
                    )
                    .await;
                }
                Message::BalanceUpdate(_) => {}
                Message::Empty => {}
                Message::FillUpdate(_fill_update) => todo!(),
            }
        }

        tracing::warn!("mqtt publisher channel closed, exiting");
    }
}

async fn poll_eventloop(mut eventloop: EventLoop) {
    tracing::info!("mqtt eventloop started");

    loop {
        match eventloop.poll().await {
            Ok(rumqttc::Event::Incoming(incoming)) => {
                tracing::trace!(?incoming, "mqtt incoming");
            }
            Ok(rumqttc::Event::Outgoing(outgoing)) => {
                tracing::trace!(?outgoing, "mqtt outgoing");
            }
            Err(e) => {
                tracing::warn!(error = %e, "mqtt eventloop error, reconnecting in 1s");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::*;
    use crate::exchange::types::{Side::Long, message::{bbo_update::BboUpdate, trade_update::TradeUpdate}};

    fn test_config(suffix: &str) -> MqttConfig {
        MqttConfig {
            enabled: true,
            broker: "localhost".into(),
            port: 1883,
            topic_prefix: format!("test/mma/{suffix}"),
            client_id: format!("mma-test-publisher-{suffix}"),
        }
    }

    async fn broker_available() -> bool {
        let opts = MqttOptions::new("mma-healthcheck", "localhost", 1883);
        let (_client, mut eventloop) = AsyncClient::new(opts, 10);

        let timeout = std::time::Duration::from_secs(3);
        tokio::time::timeout(timeout, async {
            loop {
                match eventloop.poll().await {
                    Ok(rumqttc::Event::Incoming(rumqttc::Incoming::ConnAck(_))) => return true,
                    Ok(_) => continue,
                    Err(_) => return false,
                }
            }
        })
        .await
        .unwrap_or(false)
    }

    async fn create_subscriber(suffix: &str) -> (AsyncClient, mpsc::Receiver<rumqttc::Publish>) {
        let client_id = format!("mma-test-subscriber-{suffix}");
        let opts = MqttOptions::new(&client_id, "localhost", 1883);
        let (client, mut eventloop) = AsyncClient::new(opts, 100);
        let (tx, rx) = mpsc::channel(100);

        tokio::spawn(async move {
            loop {
                match eventloop.poll().await {
                    Ok(rumqttc::Event::Incoming(rumqttc::Incoming::Publish(packet))) => {
                        if tx.send(packet).await.is_err() {
                            break;
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!(%e, "subscriber eventloop error");
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                }
            }
        });

        (client, rx)
    }

    #[tokio::test]
    async fn publish_trade_update() {
        if !broker_available().await {
            eprintln!("skipping: no MQTT broker at localhost:1883");
            return;
        }

        let config = test_config("trade");
        let (tx, rx) = mpsc::channel(100);
        let _publisher = tokio::spawn(MqttPublisher::run(config, rx));

        let (sub_client, mut sub_rx) = create_subscriber("trade").await;
        sub_client
            .subscribe("test/mma/trade/+/price/+", QoS::AtMostOnce)
            .await
            .expect("subscribe failed");

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        tx.send(Message::TradeUpdate(TradeUpdate {
            exchange: "hyperliquid".into(),
            symbol: "BTC".into(),
            side: Long,
            price: Decimal::new(50000, 0),
            size: Decimal::new(1, 0),
            time: 1234567890,
        }))
        .await
        .unwrap();

        let received = tokio::time::timeout(std::time::Duration::from_secs(5), sub_rx.recv())
            .await
            .expect("timeout waiting for trade message")
            .expect("channel closed");

        assert_eq!(received.topic, "test/mma/trade/hyperliquid/price/BTC");

        let payload: serde_json::Value = serde_json::from_slice(&received.payload).unwrap();
        assert_eq!(payload["price"], 50000.0);
    }

    #[tokio::test]
    async fn publish_bbo_update() {
        if !broker_available().await {
            eprintln!("skipping: no MQTT broker at localhost:1883");
            return;
        }

        let config = test_config("bbo");
        let (tx, rx) = mpsc::channel(100);
        let _publisher = tokio::spawn(MqttPublisher::run(config, rx));

        let (sub_client, mut sub_rx) = create_subscriber("bbo").await;
        sub_client
            .subscribe("test/mma/bbo/+/#", QoS::AtMostOnce)
            .await
            .expect("subscribe failed");

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let bid_price = Decimal::new(49900, 0);
        let ask_price = Decimal::new(50100, 0);

        tx.send(Message::BboUpdate(BboUpdate {
            exchange: "hyperliquid".into(),
            symbol: "BTC".into(),
            bid_price: bid_price,
            bid_size: ask_price,
            ask_price: Decimal::new(50100, 0),
            ask_size: Decimal::new(5, 0),
            time: 1234567890,
            mid_price: bid_price + ask_price / Decimal::new(2, 0),
        }))
        .await
        .unwrap();

        let mut topics = Vec::new();
        for _ in 0..2 {
            let received = tokio::time::timeout(std::time::Duration::from_secs(5), sub_rx.recv())
                .await
                .expect("timeout waiting for bbo message")
                .expect("channel closed");
            topics.push(received.topic);
        }

        topics.sort();
        assert_eq!(topics[0], "test/mma/bbo/hyperliquid/ask/BTC");
        assert_eq!(topics[1], "test/mma/bbo/hyperliquid/bid/BTC");
    }

    #[tokio::test]
    async fn publish_empty_does_not_crash() {
        if !broker_available().await {
            eprintln!("skipping: no MQTT broker at localhost:1883");
            return;
        }

        let config = test_config("empty");
        let (tx, rx) = mpsc::channel(100);
        let _publisher = tokio::spawn(MqttPublisher::run(config, rx));

        tx.send(Message::Empty).await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}
