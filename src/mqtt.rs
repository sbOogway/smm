use rumqttc::{AsyncClient, EventLoop, MqttOptions, QoS};
use rust_decimal::prelude::ToPrimitive;
use tokio::sync::mpsc;

use crate::common_data_representation::message::Message;
use crate::config::MqttConfig;

pub struct MqttPublisher;

impl MqttPublisher {
    pub async fn run(config: MqttConfig, mut rx: mpsc::Receiver<Message>) {
        let mqttoptions = {
            let mut opts = MqttOptions::new(&config.client_id, &config.broker, config.port);
            opts.set_keep_alive(std::time::Duration::from_secs(30));
            opts.set_clean_session(true);
            opts
        };

        let (client, eventloop) = AsyncClient::new(mqttoptions, 100);

        let publish = |msg: Message| {
            let topic = match topic_for(&config, &msg) {
                Some(t) => t,
                None => return,
            };
            let payload = match payload_for(&msg) {
                Some(p) => p,
                None => return,
            };
            let client = client.clone();
            tokio::spawn(async move {
                if let Err(e) = client.publish(&topic, QoS::AtLeastOnce, false, payload).await {
                    tracing::warn!(error = %e, topic = %topic, "mqtt publish failed");
                }
            });
        };

        poll_loop(eventloop, &mut rx, publish).await;
    }
}

async fn poll_loop(
    mut eventloop: EventLoop,
    rx: &mut mpsc::Receiver<Message>,
    publish: impl Fn(Message),
) {
    loop {
        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    Some(msg) => publish(msg),
                    None => break,
                }
            }
            event = eventloop.poll() => {
                match event {
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!(error = %e, "mqtt eventloop error");
                    }
                }
            }
        }
    }
}

fn topic_for(config: &MqttConfig, msg: &Message) -> Option<String> {
    let (kind, symbol) = match msg {
        Message::TradeUpdate(t) => ("trades", &t.symbol),
        Message::BboUpdate(b) => ("bbo", &b.symbol),
        Message::Empty => return None,
    };
    Some(format!("{}/hyperliquid/{kind}/{symbol}", config.topic_prefix))
}

fn payload_for(msg: &Message) -> Option<String> {
    let value = match msg {
        Message::TradeUpdate(t) => serde_json::json!({
            "exchange": t.exchange,
            "symbol": t.symbol,
            "side": t.side,
            "price": t.price.to_f64().unwrap_or(0.0),
            "size": t.size.to_f64().unwrap_or(0.0),
            "time": t.time,
        }),
        Message::BboUpdate(b) => serde_json::json!({
            "exchange": b.exchange,
            "symbol": b.symbol,
            "bid_price": b.bid_price.to_f64().unwrap_or(0.0),
            "bid_size": b.bid_size.to_f64().unwrap_or(0.0),
            "ask_price": b.ask_price.to_f64().unwrap_or(0.0),
            "ask_size": b.ask_size.to_f64().unwrap_or(0.0),
            "time": b.time,
        }),
        Message::Empty => return None,
    };
    Some(value.to_string())
}
