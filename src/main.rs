use tracing_subscriber::EnvFilter;

use crate::{
    config::{AppConfig, AvellanedaStoikovConfig},
    strategy::avellaneda_stoikov_market_making::AvellanedaStoikovMarketMaking,
};

mod common_data_representation;
mod config;
mod exchange;
mod strategy;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cfg = AppConfig::load()?;

    tracing::info!(?cfg, "configuration loaded");

    let strategy = match cfg.runtime.strategy.as_str() {
        "avellaneda_stoikov_market_making" => AvellanedaStoikovMarketMaking::new(&cfg),
        _ => panic!("strategy not implemented"),
    };

    strategy.run().await;

    Ok(())
}
