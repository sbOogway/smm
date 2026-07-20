//! `exchange` module is responsible to interact with an exchange, that can be a dex, a cex,
//! a prediction market or anything really. It is responsible for data gathering (candles, ticks, order book)
//! over various protocols (e.g. Websocket, FIX), sending, deleting and modyfing orders and checking
//! balances.

pub mod dydx;
// pub mod hyperliquid;
// pub mod types;

use std::{future::Future, pin::Pin};

use disruptor::{MultiProducer, SingleConsumerBarrier};
use rust_decimal::Decimal;

use crate::ccxt::Ccxt;
use crate::config::AppConfig;
use crate::exchange::dydx::Dydx;
// use crate::exchange::types::message::Message;
// use crate::exchange::types::portfolio::Order;
// use types::portfolio::Portfolio as PortfolioType;
pub trait Info {
    fn add_symbol(&mut self, symbol: String);
    fn symbols(&self) -> Vec<String>;
    fn name(&self) -> String;
}

pub trait Exchange: Info + Ccxt {}

pub fn new(name: &str, _cfg: &AppConfig) -> Box<dyn Exchange> {
    match name {
        // "hyperliquid" => Box::new(Hyperliquid::new(
        //     cfg.exchange
        //         .hyperliquid
        //         .clone()
        //         .expect("missing [exchange.hyperliquid] config"),
        // )),
        "dydx" => Box::new(Dydx::new(
            &_cfg
                .exchange
                .dydx
                .clone()
                .expect("missing [exchange.dydx] config"),
        )),
        other => panic!("unknown exchange: {other}"),
    }
}
