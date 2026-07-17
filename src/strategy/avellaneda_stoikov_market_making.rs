//! implementation of the infamous market making strategy proposed by avellaneda and stoikov
//!
//! this is the paper we take this strategy from
//!
//! <https://people.orie.cornell.edu/sfs33/LimitOrderBook.pdf>
//! <https://doi.org/10.1080/14697680701381228>

use std::{sync::OnceLock, time::Duration};

use async_trait::async_trait;
use disruptor::{MultiProducer, ProcessorSettings, SingleConsumerBarrier, Sleep};
use futures_util::future;
use rust_decimal::{Decimal, MathematicalOps};
use tokio::sync::mpsc::{self, Sender};

use crate::{
    config::AppConfig,
    data::{
        storage::{
            expiration_buffer::{self, ExpirationBuffer},
            memory_map::{self, MemoryMap},
        },
        transception::mqtt::MqttPublisher,
    },
    exchange::{self, Exchange},
    strategy::Strategy,
    exchange::types::message::{Message, asmm_quote::AsmmQuote},
};

/// i decided to have these objects static to avoid lifetime headaches and complains
/// by the rust compiler. my vision was to have something like a singleton.
static DISRUPTOR_PRODUCER: OnceLock<MultiProducer<Message, SingleConsumerBarrier>> =
    OnceLock::new();
static MQTT_TX: OnceLock<Sender<Message>> = OnceLock::new();
static EXCHANGES: OnceLock<Vec<Box<dyn Exchange>>> = OnceLock::new();

/// is intended to store variables that change frequently (like q,
/// best_bid, best_ask etc) and only need to store the last value of them.
static STATE_STORAGE: OnceLock<Box<dyn MemoryMap<Decimal>>> = OnceLock::new();

/// is intended to store trades that happen in the specified rolling time window.
/// we use tthese values to calculate γ and κ.
static TRADES_STORAGE: OnceLock<Box<dyn ExpirationBuffer<Decimal>>> = OnceLock::new();

pub struct AvellanedaStoikovMarketMaking {}

impl AvellanedaStoikovMarketMaking {
    fn reservation_price(s: Decimal, q: Decimal, γ: Decimal, σ: Decimal) -> Decimal {
        s - q * γ * σ.powi(2)
    }

    fn optimal_spread(γ: Decimal, κ: Decimal) -> Decimal {
        let one = Decimal::ONE;
        let two = Decimal::from(2);

        two / γ * (one + (γ / κ)).ln()
    }

    fn init_state(cfg: &AppConfig) {
        let _ = STATE_STORAGE.set(memory_map::new("native", None));
        let _ = TRADES_STORAGE.set(expiration_buffer::new(
            "native",
            Duration::from_mins(5),
            None,
        ));

        let state = &**STATE_STORAGE.get().expect("storage not initialized");
        let _trades = &**TRADES_STORAGE.get().expect("trades not initialized");

        for exchange in EXCHANGES.get().unwrap() {
            for symbol in exchange.symbols() {
                let γ_key = format!("{}_{}_γ", exchange.name(), symbol);
                state.set(
                    γ_key,
                    cfg.strategy
                        .avellaneda_stoikov_market_making
                        .as_ref()
                        .unwrap()
                        .γ,
                );

                let σ_key = format!("{}_{}_σ", exchange.name(), symbol);
                state.set(σ_key, Decimal::ONE);

                let κ_key = format!("{}_{}_κ", exchange.name(), symbol);
                state.set(κ_key, Decimal::ONE);

                let q_key = format!("{}_{}_q", exchange.name(), symbol);
                state.set(q_key, Decimal::ZERO);
            }
        }
    }

    /// `disruptor` callback
    ///
    /// we can afford `unsafe` code here because the `disruptor` architecture ensures that each `Message` is
    /// processed sequentially
    fn handle_message(message: &Message) {
        tracing::debug!("{:#?}", message);

        let state = &**STATE_STORAGE.get().expect("storage not initialized");
        let trades = &**TRADES_STORAGE.get().expect("trades not initialized");

        match message {
            Message::BalanceUpdate(update) => {
                tracing::info!("{:#?}", update);

                let key = format!("{}_{}_q", update.exchange, update.symbol);
                state.set(key, update.quantity);
            }
            Message::Empty => todo!(),
            Message::AsmmQuote(_) => todo!(),
            Message::TradeUpdate(update) => {
                tracing::debug!("{:#?}", message);
                let key = format!("{}_{}", update.exchange, update.symbol);

                state.set(key, update.price);

                trades.add(update.price);

                if let Some(tx) = MQTT_TX.get() {
                    let _ = tx.try_send(message.clone());
                }
            }
            Message::BboUpdate(update) => {
                let mid_price = (update.bid_price + update.ask_price) / Decimal::new(2, 0);

                let bid_price_key = format!("{}_{}_bid_price", update.exchange, update.symbol);
                let bid_size_key = format!("{}_{}_bid_size", update.exchange, update.symbol);

                let ask_price_key = format!("{}_{}_ask_price", update.exchange, update.symbol);
                let ask_size_key = format!("{}_{}_ask_size", update.exchange, update.symbol);

                let mid_price_key = format!("{}_{}_mid_price", update.exchange, update.symbol);

                let q_key = format!("{}_{}_q", update.exchange, update.symbol);
                let γ_key = format!("{}_{}_γ", update.exchange, update.symbol);
                let σ_key = format!("{}_{}_σ", update.exchange, update.symbol);
                let κ_key = format!("{}_{}_κ", update.exchange, update.symbol);

                let q = state.get(&q_key).unwrap();
                let γ = state.get(&γ_key).unwrap();
                let σ = state.get(&σ_key).unwrap();
                let κ = state.get(&κ_key).unwrap();

                let reservation_price =
                    AvellanedaStoikovMarketMaking::reservation_price(mid_price, q, γ, σ);

                let optimal_spread = AvellanedaStoikovMarketMaking::optimal_spread(γ, κ);

                let asmm_bid_price = reservation_price - optimal_spread / Decimal::new(2, 0);
                let asmm_ask_price = reservation_price + optimal_spread / Decimal::new(2, 0);

                state.set(bid_price_key, update.bid_price);
                state.set(bid_size_key, update.bid_size);

                state.set(ask_price_key, update.ask_price);
                state.set(ask_size_key, update.ask_size);

                state.set(mid_price_key, mid_price);

                if let Some(tx) = MQTT_TX.get() {
                    let mut message_clone = update.clone();
                    message_clone.mid_price = mid_price;
                    let _ = tx.try_send(Message::BboUpdate(message_clone));

                    let _ = tx.try_send(Message::AsmmQuote(AsmmQuote {
                        exchange: update.exchange.clone(),
                        symbol: update.symbol.clone(),
                        reservation_price,
                        asmm_bid_price,
                        asmm_ask_price,
                    }));
                }
            }
        }
    }
}

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
            Message::empty,
            Sleep::new(Duration::from_millis(1)),
        )
        .pin_at_core(1)
        .handle_events_with(|message, _seq, _batch| {
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

        AvellanedaStoikovMarketMaking::init_state(cfg);

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
