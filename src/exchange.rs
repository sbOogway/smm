pub mod hyperliquid;

use std::{future::Future, pin::Pin};

use disruptor::{MultiProducer, SingleConsumerBarrier};

use crate::config::AppConfig;

use self::hyperliquid::Hyperliquid;

use super::common_data_representation::price_update::PriceUpdate;

pub trait Executor {
    fn send_order(&self);
    fn cancel_order(&self);
}

pub trait DataProvider<T> {
    fn listen_trades(
        &self,
        disruptor: MultiProducer<T, SingleConsumerBarrier>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

pub trait Exchange<T>: DataProvider<T> + Executor + Send + Sync {}

pub fn create_exchange(name: &str, cfg: &AppConfig) -> Box<dyn Exchange<PriceUpdate>> {
    match name {
        "hyperliquid" => Box::new(Hyperliquid::new(
            cfg.exchange
                .hyperliquid
                .clone()
                .expect("missing [exchange.hyperliquid] config")
                .coins,
        )),
        other => panic!("unknown exchange: {other}"),
    }
}
