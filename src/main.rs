use crate::strategy::{
    common_data_representation::{disruptor::Disruptor, price_update::PriceUpdate},
    exchange::{hyperliquid::Hyperliquid, traits::DataProvider},
};

mod strategy;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Hello, world!");
    // strategy::avellaneda_stoikov_market_making::new();
    let disruptor = Disruptor::new(
        64,
        || PriceUpdate {
            symbol: "BTC/USDC".to_string(),
            exchange: "hyperliquid".to_string(),
            price: 0.0,
        },
        |_, _, _| println!("lol"),
    );
    let producer = disruptor.producer.clone();

    Hyperliquid::listen_trades(producer).await;

    Ok(())
}
