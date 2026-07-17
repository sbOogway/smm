use rust_decimal::Decimal;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub struct PositionInfo {
    pub size: Decimal,
    pub entry_price: Decimal,
    pub realized_pnl: Decimal,
    pub unrealized_pnl: Decimal,
    pub net_funding: Decimal,
    pub value: Decimal,
}

#[derive(Debug, Clone)]
pub struct BalanceUpdate {
    pub exchange: String,
    pub address: String,
    pub equity: Decimal,
    pub free_collateral: Decimal,
    pub balances: HashMap<String, Decimal>,
    pub positions: HashMap<String, PositionInfo>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn construction_and_fields() {
        let mut balances = HashMap::new();
        balances.insert("USDC".into(), Decimal::new(10000, 0));
        balances.insert("BTC".into(), Decimal::new(1, 0));
        let mut positions = HashMap::new();
        positions.insert(
            "BTC-USD".into(),
            PositionInfo {
                size: Decimal::new(2, 0),
                entry_price: Decimal::new(50000, 0),
                realized_pnl: Decimal::new(1000, 0),
                unrealized_pnl: Decimal::new(500, 0),
                net_funding: Decimal::new(-50, 0),
                value: Decimal::new(101500, 0),
            },
        );
        let b = BalanceUpdate {
            exchange: "dydx".into(),
            address: "dydx14zzueazeh0hj67cghhf9jypslcf9sh2n5k6art".into(),
            equity: Decimal::new(111500, 0),
            free_collateral: Decimal::new(10000, 0),
            balances,
            positions,
        };
        assert_eq!(b.exchange, "dydx");
        assert_eq!(b.address, "dydx14zzueazeh0hj67cghhf9jypslcf9sh2n5k6art");
        assert_eq!(b.equity, Decimal::new(111500, 0));
        assert_eq!(b.free_collateral, Decimal::new(10000, 0));
        assert_eq!(b.balances.get("USDC"), Some(&Decimal::new(10000, 0)));
        assert_eq!(b.balances.get("BTC"), Some(&Decimal::new(1, 0)));
        let btc_pos = b.positions.get("BTC-USD").unwrap();
        assert_eq!(btc_pos.size, Decimal::new(2, 0));
        assert_eq!(btc_pos.entry_price, Decimal::new(50000, 0));
    }

    #[test]
    fn clone_equality() {
        let mut balances = HashMap::new();
        balances.insert("ETH".into(), Decimal::new(10, 0));
        let mut positions = HashMap::new();
        positions.insert(
            "ETH-USD".into(),
            PositionInfo {
                size: Decimal::new(-5, 0),
                entry_price: Decimal::new(3000, 0),
                realized_pnl: Decimal::new(200, 0),
                unrealized_pnl: Decimal::new(-100, 0),
                net_funding: Decimal::new(10, 0),
                value: Decimal::new(-14900, 0),
            },
        );
        let b = BalanceUpdate {
            exchange: "hyperliquid".into(),
            address: "0xabc123".into(),
            equity: Decimal::ZERO,
            free_collateral: Decimal::ZERO,
            balances,
            positions,
        };
        let c = b.clone();
        assert_eq!(b.exchange, c.exchange);
        assert_eq!(b.address, c.address);
        assert_eq!(b.equity, c.equity);
        assert_eq!(b.free_collateral, c.free_collateral);
        assert_eq!(b.balances, c.balances);
        assert_eq!(b.positions, c.positions);
    }

    #[test]
    fn debug_format() {
        let mut balances = HashMap::new();
        balances.insert("SOL".into(), Decimal::new(100, 0));
        let b = BalanceUpdate {
            exchange: "test".into(),
            address: "addr1".into(),
            equity: Decimal::ZERO,
            free_collateral: Decimal::ZERO,
            balances,
            positions: HashMap::new(),
        };
        let fmt = format!("{b:?}");
        assert!(fmt.contains("BalanceUpdate"));
        assert!(fmt.contains("test"));
        assert!(fmt.contains("addr1"));
    }

    #[test]
    fn empty_balances_accepted() {
        let b = BalanceUpdate {
            exchange: String::new(),
            address: String::new(),
            equity: Decimal::ZERO,
            free_collateral: Decimal::ZERO,
            balances: HashMap::new(),
            positions: HashMap::new(),
        };
        assert!(b.balances.is_empty());
        assert!(b.positions.is_empty());
    }
}
