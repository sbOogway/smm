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
}

impl BboUpdate {
    pub fn handle(&self, _seq: i64, _batch: bool) {
        tracing::info!(
            exchange = %self.exchange,
            symbol = %self.symbol,
            bid_price = %self.bid_price,
            bid_size = %self.bid_size,
            ask_price = %self.ask_price,
            ask_size = %self.ask_size,
            time = self.time,
            "bbo",
        );
    }
}
