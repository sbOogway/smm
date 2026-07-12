use std::{
    cell::UnsafeCell,
    collections::HashMap,
    sync::{Arc, LazyLock, OnceLock},
    time::Duration,
};

use async_trait::async_trait;
use disruptor::{
    MultiProducer, MultiProducerBarrier, ProcessorSettings, SingleConsumerBarrier, Sleep,
    builder::{NC, multi::MPBuilder},
};
use futures_util::future;
use rust_decimal::Decimal;
use tokio::sync::mpsc::{self, Sender};

use crate::{
    common_data_representation::message::Message,
    common_data_representation::mqtt::MqttPublisher,
    config::AppConfig,
    exchange::{self, Exchange},
    strategy::Strategy,
};

pub struct AvellanedaStoikovMarketMaking {}

impl AvellanedaStoikovMarketMaking {
    fn handle_message(message: &Message) {
        tracing::debug!("{:#?}", message);

        match message {
            Message::Empty => todo!(),
            Message::TradeUpdate(update) => unsafe {
                let state = &mut *STATE.0.get();

                let key = format!("{}_{}", update.exchange, update.symbol);

                state.insert(key, update.price);

                if let Some(tx) = MQTT_TX.get() {
                    let _ = tx.try_send(message.clone());
                }
            },
            Message::BboUpdate(update) => {
                let mid_price = (update.bid_price + update.ask_price) / Decimal::new(2, 0);

                let bid_price_key = format!("{}_{}_bid_price", update.exchange, update.symbol);
                let bid_size_key = format!("{}_{}_bid_size", update.exchange, update.symbol);

                let ask_price_key = format!("{}_{}_ask_price", update.exchange, update.symbol);
                let ask_size_key = format!("{}_{}_ask_size", update.exchange, update.symbol);

                let mid_price_key = format!("{}_{}_mid_price", update.exchange, update.symbol);

                unsafe {
                    let state = &mut *STATE.0.get();

                    state.insert(bid_price_key, update.bid_price);
                    state.insert(bid_size_key, update.bid_size);

                    state.insert(ask_price_key, update.ask_price);
                    state.insert(ask_size_key, update.ask_size);

                    state.insert(mid_price_key, mid_price);
                }

                if let Some(tx) = MQTT_TX.get() {
                    let mut message_clone = update.clone();
                    message_clone.mid_price = mid_price;
                    let _ = tx.try_send(Message::BboUpdate(message_clone));
                }
            }
        }
    }
}
struct State(UnsafeCell<HashMap<String, Decimal>>);
unsafe impl Sync for State {}

pub static DISRUPTOR_PRODUCER: OnceLock<MultiProducer<Message, SingleConsumerBarrier>> =
    OnceLock::new();

pub static MQTT_TX: OnceLock<Sender<Message>> = OnceLock::new();
pub static EXCHANGES: OnceLock<Vec<Box<dyn Exchange>>> = OnceLock::new();

static STATE: LazyLock<State> = LazyLock::new(|| State(UnsafeCell::new(HashMap::new())));

#[async_trait]
impl Strategy for AvellanedaStoikovMarketMaking {
    fn new(cfg: &AppConfig) -> Self {
        if cfg.mqtt.enabled {
            let (mqtt_tx, mqtt_rx) = mpsc::channel(256);
            tokio::spawn(MqttPublisher::run(cfg.mqtt.clone(), mqtt_rx));
            let _ = MQTT_TX.set(mqtt_tx);
        }

        let disruptor_producer = disruptor::build_multi_producer(
            cfg.disruptor.buffer_size,
            || Message::empty(),
            Sleep::new(Duration::from_millis(1)),
        )
        .pin_at_core(1)
        .handle_events_with(|message, seq, batch| {
            AvellanedaStoikovMarketMaking::handle_message(message)
        })
        .build();

        let _ = DISRUPTOR_PRODUCER.set(disruptor_producer);
        let _ = EXCHANGES.set(
            cfg.runtime
                .exchanges
                .iter()
                .map(|name| exchange::new(name, cfg))
                .collect(),
        );

        Self {}
    }

    async fn run(&self) {
        for exchange in EXCHANGES.get().unwrap() {
            let producer = DISRUPTOR_PRODUCER.get().unwrap().clone();
            tokio::spawn(async move {
                exchange.listen(producer).await;
            });
        }
        future::pending::<()>().await;
    }
}
