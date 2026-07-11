use rumqttc::{AsyncClient, EventLoop, MqttOptions, QoS};
use rust_decimal::prelude::ToPrimitive;
use tokio::sync::mpsc;

use crate::common_data_representation::message::Message;
use crate::config::MqttConfig;

pub struct MqttPublisher;

impl MqttPublisher {
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
                    let topic = format!("{}/hyperliquid/price/{}", config.topic_prefix, t.symbol);
                    let payload = serde_json::json!({ "price": t.price.to_f64().unwrap_or(0.0) }).to_string();
                    tracing::debug!(topic = %topic, payload = %payload, "mqtt publish trade");
                    if let Err(e) = client.publish(&topic, QoS::AtMostOnce, false, payload).await {
                        tracing::warn!(error = %e, topic = %topic, "mqtt publish failed");
                    }
                }
                Message::BboUpdate(b) => {
                    let bid_topic = format!("{}/hyperliquid/bid/{}", config.topic_prefix, b.symbol);
                    let bid_payload = serde_json::json!({ "bid": b.bid_price.to_f64().unwrap_or(0.0) }).to_string();
                    tracing::debug!(topic = %bid_topic, payload = %bid_payload, "mqtt publish bid");
                    if let Err(e) = client.publish(&bid_topic, QoS::AtMostOnce, false, bid_payload).await {
                        tracing::warn!(error = %e, topic = %bid_topic, "mqtt publish failed");
                    }

                    let ask_topic = format!("{}/hyperliquid/ask/{}", config.topic_prefix, b.symbol);
                    let ask_payload = serde_json::json!({ "ask": b.ask_price.to_f64().unwrap_or(0.0) }).to_string();
                    tracing::debug!(topic = %ask_topic, payload = %ask_payload, "mqtt publish ask");
                    if let Err(e) = client.publish(&ask_topic, QoS::AtMostOnce, false, ask_payload).await {
                        tracing::warn!(error = %e, topic = %ask_topic, "mqtt publish failed");
                    }
                }
                Message::Empty => {}
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
