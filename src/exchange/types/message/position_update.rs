use rust_decimal::Decimal;

use crate::exchange::types::Side;

#[derive(Debug, Clone)]
pub struct PositionUpdate {
    pub exchange: String,
    pub symbol: String,
    pub side: Side,
    pub quantity: Decimal,
    pub average_price: Decimal
}

