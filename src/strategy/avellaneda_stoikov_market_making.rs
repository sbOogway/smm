use async_trait::async_trait;
use futures_util::future;

use crate::{
    common_data_representation::{disruptor::Disruptor, price_update::PriceUpdate},
    config::AppConfig,
    exchange::{Exchange, create_exchange},
    strategy::Strategy,
};

pub struct AvellanedaStoikovMarketMaking {
    exchanges: Vec<Box<dyn Exchange<PriceUpdate>>>,
    producer: disruptor::MultiProducer<PriceUpdate, disruptor::SingleConsumerBarrier>,
}

#[async_trait]
impl Strategy for AvellanedaStoikovMarketMaking {
    fn new(cfg: &AppConfig) -> Self {
        let d = Disruptor::new(
            cfg.disruptor.buffer_size,
            || PriceUpdate::empty(),
            |update, seq, batch| update.handle(seq, batch),
        );
        Self {
            exchanges: cfg
                .runtime
                .exchanges
                .iter()
                .map(|name| create_exchange(name, cfg))
                .collect(),
            producer: d.producer,
        }
    }

    async fn run(self: Box<Self>) {
        for exchange in self.exchanges {
            let producer = self.producer.clone();
            tokio::spawn(async move {
                exchange.listen_trades(producer).await;
            });
        }
        future::pending::<()>().await;
    }
}
