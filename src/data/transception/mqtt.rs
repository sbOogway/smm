//! `mqtt` module is responsible to publish data to the broker
//!
//! the current design of the system implies that data is only published and never read.

use rumqttc::{AsyncClient, EventLoop, MqttOptions, QoS};
use rust_decimal::prelude::ToPrimitive;
use tokio::sync::mpsc;

use crate::config::MqttConfig;
use crate::ccxt::CcxtMessage;

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

    pub async fn run(config: MqttConfig, mut rx: mpsc::Receiver<CcxtMessage>) {
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
