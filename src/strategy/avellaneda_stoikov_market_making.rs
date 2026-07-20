//! implementation of the infamous market making strategy proposed by avellaneda and stoikov
//!
//! this is the paper we take this strategy from
//!
//! <https://people.orie.cornell.edu/sfs33/LimitOrderBook.pdf>
//! <https://doi.org/10.1080/14697680701381228>

use std::{
    future,
    sync::{Arc, OnceLock},
    time::Duration,
};

use async_trait::async_trait;
use disruptor::{MultiProducer, ProcessorSettings, SingleConsumerBarrier, Sleep};
use rust_decimal::{Decimal, MathematicalOps};
use tokio::sync::Mutex;

use crate::{
    ccxt::CcxtMessage,
    config::AppConfig,
    data::storage::{
        expiration_buffer::{self, ExpirationBuffer},
        memory_map::{self, MemoryMap},
    },
    exchange::{self, Exchange, Info, dydx::Dydx},
    strategy::Strategy,
};

/// i decided to have these objects static to avoid lifetime headaches and complains
/// by the rust compiler. my vision was to have something like a singleton.
// static DISRUPTOR_PRODUCER: OnceLock<MultiProducer<Message, SingleConsumerBarrier>> =
//     OnceLock::new();
// static MQTT_TX: OnceLock<Sender<Message>> = OnceLock::new();
// static EXCHANGES: OnceLock<Vec<Box<dyn Exchange>>> = OnceLock::new();

/// is intended to store variables that change frequently (like q,
/// best_bid, best_ask etc) and only need to store the last value of them.
static STATE_STORAGE: OnceLock<Box<dyn MemoryMap<Decimal>>> = OnceLock::new();

/// is intended to store trades that happen in the specified rolling time window.
/// we use tthese values to calculate γ and κ.
static TRADES_STORAGE: OnceLock<Box<dyn ExpirationBuffer<Decimal>>> = OnceLock::new();

pub struct AvellanedaStoikovMarketMaking {
    exchange: Box<dyn Exchange>,
    disruptor: Option<MultiProducer<CcxtMessage, SingleConsumerBarrier>>,
}

impl AvellanedaStoikovMarketMaking {
    fn reservation_price(s: Decimal, q: Decimal, γ: Decimal, σ: Decimal) -> Decimal {
        s - q * γ * σ.powi(2)
    }

    fn optimal_spread(γ: Decimal, κ: Decimal) -> Decimal {
        let one = Decimal::ONE;
        let two = Decimal::from(2);

        two / γ * (one + (γ / κ)).ln()
    }

    fn init_state(exchange: &dyn Exchange, cfg: &AppConfig) {
        let state = STATE_STORAGE.get().expect("state not initialized");

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

    fn handle_message(message: &CcxtMessage, exchange_name: &str) {
        tracing::debug!("{:#?}", message);

        let state = STATE_STORAGE.get().expect("state not initialized");
        let trades = TRADES_STORAGE.get().expect("trades not initialized");

        match message {
            CcxtMessage::CcxtBalance(_update) => {}
            CcxtMessage::CcxtEmpty => todo!(),
            CcxtMessage::CcxtTrade(update) => {
                tracing::debug!("{:#?}", message);
                let key = format!("{}_{}", exchange_name, update.symbol);

                state.set(key, update.price);
                trades.add(update.price);
            }
            CcxtMessage::CcxtOrderBook(update) => {
                let bid_price = update.bids.first().unwrap().price;
                let ask_price = update.asks.first().unwrap().price;

                let bid_size = update.bids.first().unwrap().amount;
                let ask_size = update.asks.first().unwrap().amount;

                let mid_price = (bid_price + ask_price) / Decimal::new(2, 0);

                let bid_price_key = format!("{}_{}_bid_price", exchange_name, update.symbol);
                let bid_size_key = format!("{}_{}_bid_size", exchange_name, update.symbol);

                let ask_price_key = format!("{}_{}_ask_price", exchange_name, update.symbol);
                let ask_size_key = format!("{}_{}_ask_size", exchange_name, update.symbol);

                let mid_price_key = format!("{}_{}_mid_price", exchange_name, update.symbol);

                let q_key = format!("{}_{}_q", exchange_name, update.symbol);
                let γ_key = format!("{}_{}_γ", exchange_name, update.symbol);
                let σ_key = format!("{}_{}_σ", exchange_name, update.symbol);
                let κ_key = format!("{}_{}_κ", exchange_name, update.symbol);

                let q = state.get(&q_key).unwrap();
                let γ = state.get(&γ_key).unwrap();
                let σ = state.get(&σ_key).unwrap();
                let κ = state.get(&κ_key).unwrap();

                let reservation_price =
                    AvellanedaStoikovMarketMaking::reservation_price(mid_price, q, γ, σ);

                let optimal_spread = AvellanedaStoikovMarketMaking::optimal_spread(γ, κ);

                let asmm_bid_price = reservation_price - optimal_spread / Decimal::new(2, 0);
                let asmm_ask_price = reservation_price + optimal_spread / Decimal::new(2, 0);

                state.set(bid_price_key, bid_price);
                state.set(bid_size_key, bid_size);

                state.set(ask_price_key, ask_price);
                state.set(ask_size_key, ask_size);

                state.set(mid_price_key, mid_price);
            }
            CcxtMessage::CcxtPosition(_fill_update) => {}
            CcxtMessage::CcxtOrder(_ccxt_order) => todo!(),
        }
    }
}

#[async_trait]
impl Strategy for AvellanedaStoikovMarketMaking {
    fn new(cfg: &AppConfig) -> Self {
        let config = cfg.exchange.dydx.clone().unwrap();
        let exchange = Box::new(Dydx::new(&config));
        let exchange_name = exchange.name().to_string();

        let _ = STATE_STORAGE.set(memory_map::new("native", None));
        let _ = TRADES_STORAGE.set(expiration_buffer::new(
            "native",
            Duration::from_mins(5),
            None,
        ));

        AvellanedaStoikovMarketMaking::init_state(&*exchange, cfg);

        let disruptor_producer = disruptor::build_multi_producer(
            cfg.disruptor.buffer_size,
            || CcxtMessage::CcxtEmpty,
            Sleep::new(Duration::from_millis(1)),
        )
        .pin_at_core(1)
        .handle_events_with(move |message, _seq, _batch| {
            AvellanedaStoikovMarketMaking::handle_message(message, &exchange_name)
        })
        .build();

        Self {
            exchange: exchange,
            disruptor: Some(disruptor_producer),
        }
    }

    async fn run(mut self: Box<Self>) {
        let symbols: Vec<String> = vec!["BTC-USD".into()]; //, "ETH-USD".into()];

        for s in &symbols {
            self.exchange.add_symbol(s.clone());
        }

        let symbol = self.exchange.symbols().first().unwrap().to_string();
        self.exchange.load_markets().await;

        loop {
            tokio::select! {
                trade = self.exchange.watch_trades(symbol.clone(), None, None) => {tracing::debug!("{:#?}", trade);}
                order_book = self.exchange.watch_order_book(symbol.clone(), None) => {tracing::debug!("{:#?}", order_book)}
            }
        }

        future::pending::<()>().await;
    }
}
