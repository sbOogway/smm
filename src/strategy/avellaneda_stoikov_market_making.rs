use async_trait::async_trait;
use futures_util::future;
use tokio::sync::mpsc;

use crate::{
    common_data_representation::{disruptor::Disruptor, message::Message},
    config::AppConfig,
    exchange::{self, Exchange},
    mqtt::MqttPublisher,
    strategy::Strategy,
};

pub struct AvellanedaStoikovMarketMaking {
    exchanges: Vec<Box<dyn Exchange>>,
    producer: disruptor::MultiProducer<Message, disruptor::SingleConsumerBarrier>,
    mqtt_tx: mpsc::Sender<Message>,
}

#[async_trait]
impl Strategy for AvellanedaStoikovMarketMaking {
    fn new(cfg: &AppConfig) -> Self {
        let d = Disruptor::new(
            cfg.disruptor.buffer_size,
            || Message::empty(),
            |update, seq, batch| update.handle(seq, batch),
        );
        let (mqtt_tx, mqtt_rx) = mpsc::channel(256);
        let _mqtt_handle = tokio::spawn(MqttPublisher::run(cfg.mqtt.clone(), mqtt_rx));
        Self {
            exchanges: cfg
                .runtime
                .exchanges
                .iter()
                .map(|name| exchange::new(name, cfg))
                .collect(),
            producer: d.producer,
            mqtt_tx,
        }
    }

    async fn run(self: Box<Self>) {
        for exchange in self.exchanges {
            let producer = self.producer.clone();
            let mqtt_tx = self.mqtt_tx.clone();
            tokio::spawn(async move {
                exchange.listen(producer, mqtt_tx).await;
            });
        }
        future::pending::<()>().await;
    }
}
