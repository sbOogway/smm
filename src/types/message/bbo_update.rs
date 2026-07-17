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
    pub mid_price: Decimal,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn construction_and_fields() {
        let b = BboUpdate {
            exchange: "hyperliquid".into(),
            symbol: "BTC".into(),
            bid_price: Decimal::new(49900, 0),
            bid_size: Decimal::new(10, 0),
            ask_price: Decimal::new(50100, 0),
            ask_size: Decimal::new(5, 0),
            time: 1234567890,
            mid_price: Decimal::new(50000, 0),
        };
        assert_eq!(b.exchange, "hyperliquid");
        assert_eq!(b.symbol, "BTC");
        assert_eq!(b.bid_price, Decimal::new(49900, 0));
        assert_eq!(b.bid_size, Decimal::new(10, 0));
        assert_eq!(b.ask_price, Decimal::new(50100, 0));
        assert_eq!(b.ask_size, Decimal::new(5, 0));
        assert_eq!(b.time, 1234567890);
        assert_eq!(b.mid_price, Decimal::new(50000, 0));
    }

    #[test]
    fn clone_equality() {
        let b = BboUpdate {
            exchange: "binance".into(),
            symbol: "ETH".into(),
            bid_price: Decimal::new(2900, 0),
            bid_size: Decimal::new(20, 0),
            ask_price: Decimal::new(2950, 0),
            ask_size: Decimal::new(15, 0),
            time: 0,
            mid_price: Decimal::new(2925, 0),
        };
        let c = b.clone();
        assert_eq!(b.exchange, c.exchange);
        assert_eq!(b.symbol, c.symbol);
        assert_eq!(b.bid_price, c.bid_price);
        assert_eq!(b.bid_size, c.bid_size);
        assert_eq!(b.ask_price, c.ask_price);
        assert_eq!(b.ask_size, c.ask_size);
        assert_eq!(b.time, c.time);
        assert_eq!(b.mid_price, c.mid_price);
    }

    #[test]
    fn debug_format() {
        let b = BboUpdate {
            exchange: "test".into(),
            symbol: "XRP".into(),
            bid_price: Decimal::new(1, 0),
            bid_size: Decimal::new(1, 0),
            ask_price: Decimal::new(2, 0),
            ask_size: Decimal::new(1, 0),
            time: 1,
            mid_price: Decimal::new(1, 1),
        };
        let fmt = format!("{b:?}");
        assert!(fmt.contains("BboUpdate"));
        assert!(fmt.contains("test"));
        assert!(fmt.contains("XRP"));
    }

    #[test]
    fn bid_below_ask() {
        let b = BboUpdate {
            exchange: "hyperliquid".into(),
            symbol: "SOL".into(),
            bid_price: Decimal::new(100, 0),
            bid_size: Decimal::new(50, 0),
            ask_price: Decimal::new(101, 0),
            ask_size: Decimal::new(50, 0),
            time: 42,
            mid_price: Decimal::new(10050, 2),
        };
        assert!(b.bid_price < b.ask_price);
        assert_eq!(
            b.mid_price,
            (b.bid_price + b.ask_price) / Decimal::new(2, 0)
        );
    }

    #[test]
    fn zero_values_accepted() {
        let b = BboUpdate {
            exchange: String::new(),
            symbol: String::new(),
            bid_price: Decimal::ZERO,
            bid_size: Decimal::ZERO,
            ask_price: Decimal::ZERO,
            ask_size: Decimal::ZERO,
            time: 0,
            mid_price: Decimal::ZERO,
        };
        assert_eq!(b.bid_price, Decimal::ZERO);
        assert_eq!(b.ask_price, Decimal::ZERO);
        assert_eq!(b.time, 0);
    }
}
