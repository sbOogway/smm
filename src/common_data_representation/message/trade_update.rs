use rust_decimal::Decimal;

#[derive(Debug, Clone)]
pub struct TradeUpdate {
    pub exchange: String,
    pub symbol: String,
    pub side: String,
    pub price: Decimal,
    pub size: Decimal,
    pub time: u64,
}
