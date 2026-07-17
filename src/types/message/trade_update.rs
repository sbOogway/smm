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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn construction_and_fields() {
        let t = TradeUpdate {
            exchange: "hyperliquid".into(),
            symbol: "BTC".into(),
            side: "buy".into(),
            price: Decimal::new(50000, 0),
            size: Decimal::new(1, 0),
            time: 1234567890,
        };
        assert_eq!(t.exchange, "hyperliquid");
        assert_eq!(t.symbol, "BTC");
        assert_eq!(t.side, "buy");
        assert_eq!(t.price, Decimal::new(50000, 0));
        assert_eq!(t.size, Decimal::new(1, 0));
        assert_eq!(t.time, 1234567890);
    }

    #[test]
    fn clone_equality() {
        let t = TradeUpdate {
            exchange: "binance".into(),
            symbol: "ETH".into(),
            side: "sell".into(),
            price: Decimal::new(3000, 0),
            size: Decimal::new(2, 0),
            time: 0,
        };
        let c = t.clone();
        assert_eq!(t.exchange, c.exchange);
        assert_eq!(t.symbol, c.symbol);
        assert_eq!(t.side, c.side);
        assert_eq!(t.price, c.price);
        assert_eq!(t.size, c.size);
        assert_eq!(t.time, c.time);
    }

    #[test]
    fn debug_format() {
        let t = TradeUpdate {
            exchange: "test".into(),
            symbol: "XRP".into(),
            side: "buy".into(),
            price: Decimal::new(1, 1),
            size: Decimal::new(10, 0),
            time: 1,
        };
        let fmt = format!("{t:?}");
        assert!(fmt.contains("TradeUpdate"));
        assert!(fmt.contains("test"));
        assert!(fmt.contains("XRP"));
    }

    #[test]
    fn zero_values_accepted() {
        let t = TradeUpdate {
            exchange: String::new(),
            symbol: String::new(),
            side: String::new(),
            price: Decimal::ZERO,
            size: Decimal::ZERO,
            time: 0,
        };
        assert_eq!(t.price, Decimal::ZERO);
        assert_eq!(t.size, Decimal::ZERO);
        assert_eq!(t.time, 0);
    }
}
