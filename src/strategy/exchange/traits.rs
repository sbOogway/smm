use disruptor::{MultiProducer, SingleConsumerBarrier, };

use crate::strategy::common_data_representation::{
    disruptor::Disruptor, price_update::PriceUpdate,
};

pub trait Executor {
    fn send_order();
    fn cancel_order();
}

pub trait DataProvider<T> {
    async fn listen_trades(disruptor: MultiProducer<T, SingleConsumerBarrier>);
}
