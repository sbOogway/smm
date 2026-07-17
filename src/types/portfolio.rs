use std::collections::HashMap;

use rust_decimal::Decimal;

use super::message::PositionInfo;

#[derive(Debug, Clone)]
pub struct Portfolio {
    /// balances + positions
    pub equity: Decimal,
    /// value of all the assets on a exchange
    pub balances: HashMap<String, Decimal>,
    /// value of all the open positions
    pub positions: HashMap<String, PositionInfo>,
}
