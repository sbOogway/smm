use std::{future::Future, pin::Pin};

use disruptor::{MultiProducer, SingleConsumerBarrier};

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
