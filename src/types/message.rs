//! `message` module is responsible to encompass all subtypes under a single `enum`.

pub mod asmm_quote;
pub mod balance_update;
pub mod bbo_update;
pub mod trade_update;

pub use asmm_quote::AsmmQuote;
pub use balance_update::{BalanceUpdate, PositionInfo};
pub use bbo_update::BboUpdate;
pub use trade_update::TradeUpdate;

#[derive(Clone, Debug)]
pub enum Message {
    Empty,
    TradeUpdate(TradeUpdate),
    BboUpdate(BboUpdate),
    AsmmQuote(AsmmQuote),
    BalanceUpdate(BalanceUpdate),
}

impl Message {
    pub fn empty() -> Self {
        Self::Empty
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    #[test]
    fn empty_variant() {
        assert!(matches!(Message::empty(), Message::Empty));
    }

    #[test]
    fn trade_update_variant() {
        let msg = Message::TradeUpdate(TradeUpdate {
            exchange: "hyperliquid".into(),
            symbol: "BTC".into(),
            side: "buy".into(),
            price: Decimal::new(50000, 0),
            size: Decimal::new(1, 0),
            time: 123,
        });
        assert!(matches!(msg, Message::TradeUpdate(_)));
    }

    #[test]
    fn bbo_update_variant() {
        let msg = Message::BboUpdate(BboUpdate {
            exchange: "hyperliquid".into(),
            symbol: "ETH".into(),
            bid_price: Decimal::new(2900, 0),
            bid_size: Decimal::new(10, 0),
            ask_price: Decimal::new(2950, 0),
            ask_size: Decimal::new(5, 0),
            time: 456,
            mid_price: Decimal::new(2925, 0),
        });
        assert!(matches!(msg, Message::BboUpdate(_)));
    }

    #[test]
    fn balance_update_variant() {
        let mut balances = std::collections::HashMap::new();
        balances.insert("USDC".into(), Decimal::new(5000, 0));
        let msg = Message::BalanceUpdate(BalanceUpdate {
            exchange: "dydx".into(),
            address: "dydx14zzueazeh0hj67cghhf9jypslcf9sh2n5k6art".into(),
            equity: Decimal::ZERO,
            free_collateral: Decimal::ZERO,
            balances,
            positions: std::collections::HashMap::new(),
        });
        assert!(matches!(msg, Message::BalanceUpdate(_)));
    }

    #[test]
    fn asmm_quote_variant() {
        let msg = Message::AsmmQuote(AsmmQuote {
            exchange: "hyperliquid".into(),
            symbol: "SOL".into(),
            reservation_price: Decimal::new(100, 0),
            asmm_bid_price: Decimal::new(99, 0),
            asmm_ask_price: Decimal::new(101, 0),
        });
        assert!(matches!(msg, Message::AsmmQuote(_)));
    }

    #[test]
    fn clone_preserves_variant() {
        let msgs = vec![
            Message::empty(),
            Message::TradeUpdate(TradeUpdate {
                exchange: "a".into(),
                symbol: "b".into(),
                side: "c".into(),
                price: Decimal::ONE,
                size: Decimal::TWO,
                time: 0,
            }),
            Message::BboUpdate(BboUpdate {
                exchange: "a".into(),
                symbol: "b".into(),
                bid_price: Decimal::ONE,
                bid_size: Decimal::ONE,
                ask_price: Decimal::TWO,
                ask_size: Decimal::ONE,
                time: 0,
                mid_price: Decimal::ONE,
            }),
            Message::AsmmQuote(AsmmQuote {
                exchange: "a".into(),
                symbol: "b".into(),
                reservation_price: Decimal::ONE,
                asmm_bid_price: Decimal::ONE,
                asmm_ask_price: Decimal::TWO,
            }),
            Message::BalanceUpdate(BalanceUpdate {
                exchange: "a".into(),
                address: "addr".into(),
                equity: Decimal::ZERO,
                free_collateral: Decimal::ZERO,
                balances: std::collections::HashMap::new(),
                positions: std::collections::HashMap::new(),
            }),
        ];
        for msg in &msgs {
            assert_eq!(format!("{msg:?}"), format!("{:?}", msg.clone()));
        }
    }

    #[test]
    fn debug_format() {
        let msg = Message::TradeUpdate(TradeUpdate {
            exchange: "test".into(),
            symbol: "XRP".into(),
            side: "sell".into(),
            price: Decimal::new(1, 1),
            size: Decimal::new(100, 0),
            time: 7,
        });
        let fmt = format!("{msg:?}");
        assert!(fmt.contains("TradeUpdate"));
        assert!(fmt.contains("test"));
        assert!(fmt.contains("XRP"));
    }

    #[test]
    fn empty_does_not_match_data_variants() {
        let e = Message::empty();
        assert!(!matches!(e, Message::TradeUpdate(_)));
        assert!(!matches!(e, Message::BboUpdate(_)));
        assert!(!matches!(e, Message::AsmmQuote(_)));
        assert!(!matches!(e, Message::BalanceUpdate(_)));
    }
}
