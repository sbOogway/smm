use rust_decimal::Decimal;

#[derive(Debug, Clone)]
pub struct BboUpdate {
    pub exchange: String,
    pub symbol: String,
    pub bid_price: Decimal,
    pub bid_size: Decimal,
    pub ask_price: Decimal,
    pub ask_size: Decimal,
    pub time: u64,
    pub mid_price: Decimal
}
