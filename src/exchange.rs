pub mod hyperliquid;

use std::{future::Future, pin::Pin};

use disruptor::{MultiProducer, SingleConsumerBarrier};

use crate::config::AppConfig;

use self::hyperliquid::Hyperliquid;

use super::common_data_representation::message::Message;

pub trait Executor {
    fn send_order(&self);
    fn cancel_order(&self);
}

pub trait DataProvider {
    fn listen(
        &self,
        disruptor: MultiProducer<Message, SingleConsumerBarrier>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

pub trait Exchange: DataProvider + Executor + Send + Sync {}

pub fn new(name: &str, cfg: &AppConfig) -> Box<dyn Exchange> {
    match name {
        "hyperliquid" => Box::new(Hyperliquid::new(
            cfg.exchange
                .hyperliquid
                .clone()
                .expect("missing [exchange.hyperliquid] config"),
        )),
        other => panic!("unknown exchange: {other}"),
    }
}
