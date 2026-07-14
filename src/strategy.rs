//! `strategy` is the main component of the package. It is responsible for coordinating the exchanges,
//! sending `Message`s into the disruptor and execute the core logic.

use crate::config::AppConfig;
use async_trait::async_trait;

pub mod avellaneda_stoikov_market_making;

#[async_trait]
pub trait Strategy {
    fn new(cfg: &AppConfig) -> Self
    where
        Self: Sized;
    async fn run(&self);
}
