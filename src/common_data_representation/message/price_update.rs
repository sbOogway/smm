use rust_decimal::Decimal;

#[derive(Debug)]
pub struct PriceUpdate {
    pub exchange: String,
    pub symbol: String,
    pub side: String,
    pub price: Decimal,
    pub size: Decimal,
    pub time: u64,
}

impl PriceUpdate {
    pub fn handle(&self, _seq: i64, _batch: bool) {
        tracing::info!(
            exchange = %self.exchange,
            symbol = %self.symbol,
            side = %self.side,
            price = %self.price,
            size = %self.size,
            time = self.time,
            "trade",
        );
    }
}
