use disruptor::{MultiProducer, MultiProducerBarrier, SingleConsumerBarrier};
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::strategy::{
    common_data_representation::{disruptor::Disruptor, price_update::PriceUpdate},
    exchange::traits::DataProvider,
};

const WSS_URL: &str = "wss://api.hyperliquid.xyz/ws";
pub struct Hyperliquid {}

impl DataProvider<PriceUpdate> for Hyperliquid {
    async fn listen_trades(disruptor: MultiProducer<PriceUpdate, SingleConsumerBarrier>) {
        let (mut ws_stream, response) = connect_async(WSS_URL).await.unwrap();

        println!("Connected: {}", response.status());

        ws_stream
            .send(Message::Text(
                r#"{ "method": "subscribe", "subscription": {"type": "trades", "coin": "BTC/USDC"} }"#.into(),
            ))
            .await
            .unwrap();

        if let Some(msg) = ws_stream.next().await {
            println!("Received: {:?}", msg);
        }
    }
}
