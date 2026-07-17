use rust_decimal::Decimal;

use crate::exchange::types::Side;

#[derive(Debug, Clone)]
pub struct TradeUpdate {
    pub exchange: String,
    pub symbol: String,
    pub side: Side,
    pub price: Decimal,
    pub size: Decimal,
    pub time: u64,
}

