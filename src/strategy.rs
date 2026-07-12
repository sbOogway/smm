

use crate::config::AppConfig;
use async_trait::async_trait;

pub mod avellaneda_stoikov_market_making;

#[async_trait]
pub trait Strategy {
    fn new(cfg: &AppConfig) -> Self where Self: Sized;
    async fn run(&self);
}
