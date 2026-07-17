use rust_decimal::Decimal;

#[derive(Debug, Clone)]
pub struct AsmmQuote {
    pub exchange: String,
    pub symbol: String,
    pub reservation_price: Decimal,
    pub asmm_bid_price: Decimal,
    pub asmm_ask_price: Decimal,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn construction_and_fields() {
        let q = AsmmQuote {
            exchange: "hyperliquid".into(),
            symbol: "BTC".into(),
            reservation_price: Decimal::new(50000, 0),
            asmm_bid_price: Decimal::new(49950, 0),
            asmm_ask_price: Decimal::new(50050, 0),
        };
        assert_eq!(q.exchange, "hyperliquid");
        assert_eq!(q.symbol, "BTC");
        assert_eq!(q.reservation_price, Decimal::new(50000, 0));
        assert_eq!(q.asmm_bid_price, Decimal::new(49950, 0));
        assert_eq!(q.asmm_ask_price, Decimal::new(50050, 0));
    }

    #[test]
    fn clone_equality() {
        let q = AsmmQuote {
            exchange: "binance".into(),
            symbol: "ETH".into(),
            reservation_price: Decimal::new(3000, 0),
            asmm_bid_price: Decimal::new(2990, 0),
            asmm_ask_price: Decimal::new(3010, 0),
        };
        let c = q.clone();
        assert_eq!(q.exchange, c.exchange);
        assert_eq!(q.symbol, c.symbol);
        assert_eq!(q.reservation_price, c.reservation_price);
        assert_eq!(q.asmm_bid_price, c.asmm_bid_price);
        assert_eq!(q.asmm_ask_price, c.asmm_ask_price);
    }

    #[test]
    fn debug_format() {
        let q = AsmmQuote {
            exchange: "test".into(),
            symbol: "XRP".into(),
            reservation_price: Decimal::new(1, 0),
            asmm_bid_price: Decimal::new(1, 1),
            asmm_ask_price: Decimal::new(2, 0),
        };
        let fmt = format!("{q:?}");
        assert!(fmt.contains("AsmmQuote"));
        assert!(fmt.contains("test"));
        assert!(fmt.contains("XRP"));
    }

    #[test]
    fn bid_below_ask() {
        let q = AsmmQuote {
            exchange: "hyperliquid".into(),
            symbol: "SOL".into(),
            reservation_price: Decimal::new(100, 0),
            asmm_bid_price: Decimal::new(99, 0),
            asmm_ask_price: Decimal::new(101, 0),
        };
        assert!(q.asmm_bid_price < q.asmm_ask_price);
    }

    #[test]
    fn zero_values_accepted() {
        let q = AsmmQuote {
            exchange: String::new(),
            symbol: String::new(),
            reservation_price: Decimal::ZERO,
            asmm_bid_price: Decimal::ZERO,
            asmm_ask_price: Decimal::ZERO,
        };
        assert_eq!(q.reservation_price, Decimal::ZERO);
        assert_eq!(q.asmm_bid_price, Decimal::ZERO);
        assert_eq!(q.asmm_ask_price, Decimal::ZERO);
    }
}
