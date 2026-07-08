pub mod hyperliquid;
pub mod traits;

use crate::config::AppConfig;

use self::{
    hyperliquid::Hyperliquid,
    traits::Exchange,
};

use super::common_data_representation::price_update::PriceUpdate;

pub fn create_exchange(name: &str, cfg: &AppConfig) -> Box<dyn Exchange<PriceUpdate>> {
    match name {
        "hyperliquid" => Box::new(Hyperliquid::new(
            cfg.exchange
                .hyperliquid
                .clone()
                .expect("missing [exchange.hyperliquid] config")
                .coins,
        )),
        other => panic!("unknown exchange: {other}"),
    }
}
