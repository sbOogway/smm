//! `exchange` module is responsible to interact with an exchange, that can be a dex, a cex,
//! a prediction market or anything really. It is responsible for data gathering (candles, ticks, order book)
//! over various protocols (e.g. Websocket, FIX), sending, deleting and modyfing orders and checking
//! balances.

pub mod dydx;
// pub mod hyperliquid;
pub mod types;

use std::{future::Future, pin::Pin};

use disruptor::{MultiProducer, SingleConsumerBarrier};
use rust_decimal::Decimal;

use crate::config::AppConfig;
use crate::exchange::types::message::Message;
use types::portfolio::Portfolio as PortfolioType;

use self::dydx::Dydx;
// use self::hyperliquid::Hyperliquid;

pub trait Portfolio {
    fn get_portfolio(&self) -> PortfolioType;

    fn balance_of(&self, symbol: &str) -> Decimal {
        let portfolio = self.get_portfolio();
        portfolio
            .positions
            .iter()
            .find(|&position| position.symbol == symbol)
            .unwrap()
            .quantity
    }
}

pub trait Orders {
    fn create_order(&self);
    fn update_order(&self);
    fn cancel_order(&self);
}

pub trait DataProvider {
    fn listen(
        &self,
        disruptor: MultiProducer<Message, SingleConsumerBarrier>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

pub trait Infos {
    fn name(&self) -> String;
    fn symbols(&self) -> Vec<String>;
}

pub trait Exchange: DataProvider + Portfolio + Orders + Send + Sync + Infos {}

pub fn new(name: &str, cfg: &AppConfig) -> Box<dyn Exchange> {
    match name {
        // "hyperliquid" => Box::new(Hyperliquid::new(
        //     cfg.exchange
        //         .hyperliquid
        //         .clone()
        //         .expect("missing [exchange.hyperliquid] config"),
        // )),
        "dydx" => Box::new(Dydx::new(
            cfg.exchange
                .dydx
                .clone()
                .expect("missing [exchange.dydx] config"),
        )),
        other => panic!("unknown exchange: {other}"),
    }
}
