use async_trait::async_trait;
use futures_util::future;
use tokio::sync::mpsc::{self, Sender};

use crate::{
    common_data_representation::mqtt::MqttPublisher,
    common_data_representation::{disruptor::Disruptor, message::Message},
    config::AppConfig,
    exchange::{self, Exchange},
    strategy::Strategy,
};

pub struct AvellanedaStoikovMarketMaking {
    exchanges: Vec<Box<dyn Exchange>>,
    producer: disruptor::MultiProducer<Message, disruptor::SingleConsumerBarrier>,
}

impl AvellanedaStoikovMarketMaking {
    fn handle_message(message: &Message, sender: &Sender<Message>) {
        tracing::info!("{:#?}", message);
        let _ = sender.try_send(message.clone());
    }
}

#[async_trait]
impl Strategy for AvellanedaStoikovMarketMaking {
    fn new(cfg: &AppConfig) -> Self {
        let (mqtt_tx, mqtt_rx) = mpsc::channel(256);
        let _mqtt_handle = tokio::spawn(MqttPublisher::run(cfg.mqtt.clone(), mqtt_rx));

        let d = Disruptor::new(
            cfg.disruptor.buffer_size,
            || Message::empty(),
            move |message, seq, batch| {
                AvellanedaStoikovMarketMaking::handle_message(&message, &mqtt_tx);
            },
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
                exchange.listen(producer).await;
            });
        }
        future::pending::<()>().await;
    }
}
