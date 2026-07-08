use async_trait::async_trait;
use futures_util::future;

use crate::{
    common_data_representation::{disruptor::Disruptor, message::Message},
    config::AppConfig,
    exchange::{self, Exchange},
    strategy::Strategy,
};

pub struct AvellanedaStoikovMarketMaking {
    exchanges: Vec<Box<dyn Exchange>>,
    producer: disruptor::MultiProducer<Message, disruptor::SingleConsumerBarrier>,
}

#[async_trait]
impl Strategy for AvellanedaStoikovMarketMaking {
    fn new(cfg: &AppConfig) -> Self {
        let d = Disruptor::new(
            cfg.disruptor.buffer_size,
            || Message::empty(),
            |update, seq, batch| update.handle(seq, batch),
        );
        Self {
            exchanges: cfg
                .runtime
                .exchanges
                .iter()
                .map(|name| exchange::new(name, cfg))
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
