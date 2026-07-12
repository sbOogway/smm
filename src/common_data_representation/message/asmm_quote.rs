use rust_decimal::Decimal;

#[derive(Debug, Clone)]
pub struct AsmmQuote {
    pub exchange: String,
    pub symbol: String,
    pub reservation_price: Decimal,
    pub asmm_bid_price: Decimal,
    pub asmm_ask_price: Decimal,
}
