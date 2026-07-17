use rust_decimal::Decimal;

#[derive(Debug, Clone)]
pub struct Balance {
    pub symbol: String,
    pub quantity: Decimal,
}

#[derive(Debug, Clone)]
pub struct Position {
    pub symbol: String,
    pub quantity: Decimal,
    pub open_price: Decimal,
    pub leverage: i8,
}

#[derive(Debug, Clone)]
pub struct Order {
    pub symbol: String, 
    pub quantity: Decimal,
    pub price: Decimal,

}


#[derive(Debug, Clone)]
pub struct Portfolio {
    /// balances + positions
    pub equity: Decimal,
    /// value of all the assets on a exchange
    pub balances: Vec<Balance>,
    /// value of all the open positions
    pub positions: Vec<Position>,

    pub orders: Vec<Order>
}
