//! this is an implementation of the ccxt api for rust. i chose this to avoid
//! overcomplicate design and rely on an architecture that is battle tested.
//! implements only websocket functionality as i dont need http shit (in ccxt
//! corresponds to ccxt pro api).
//!
//! this is the structure stolen from the docs that i want to replicate
//!
//! ```text
//!                              User
//! +-------------------------------------------------------------+
//! |                          CCXT Pro                           |
//! +------------------------------+------------------------------+
//! |            Public            .           Private            |
//! +=============================================================+
//! │                              .                              |
//! │                  The Unified CCXT Pro API                   |
//! |                              .                              |
//! |     loadMarkets              .         watchBalance         |
//! |     watchTicker              .         watchOrders          |
//! |     watchTickers             .         watchMyTrades        |
//! |     watchOrderBook           .         watchPositions       |
//! |     watchOHLCV               .         createOrderWs        |
//! |     watchStatus              .         editOrderWs          |
//! |     watchTrades              .         cancelOrderWs        |
//! │     watchOHLCVForSymbols     .         cancelOrdersWs       |
//! │     watchTradesForSymbols    .         cancelAllOrdersWs    |
//! │     watchOrderBookForSymbols .                              |
//! │                              .                              |
//! +=============================================================+
//! │                          unWatch                            |
//! │                   (to stop **watch** method)                |
//! +=============================================================+
//! │                              .                              |
//! |            The Underlying Exchange-Specific APIs            |
//! |         (Derived Classes And Their Implementations)         |
//! │                              .                              |
//! +=============================================================+
//! │                              .                              |
//! |                 CCXT Pro Base Exchange Class                |
//! │                              .                              |
//! +=============================================================+
//! +-------------------------------------------------------------+
//! |                                                             |
//! |                            CCXT                             |
//! |                                                             |
//! +=============================================================+
//! ```
//!
//!
//! <https://docs.ccxt.com/docs/pro-manual#unified-api>

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub enum CcxtMessage {
    CcxtEmpty,
    CcxtBalance(CcxtBalance),
    CcxtOrderBook(CcxtOrderBook),
    CcxtOrder(CcxtOrder),
    CcxtPosition(CcxtPosition),
    CcxtTrade(CcxtTrade),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum CcxtLiquiditySide {
    Taker,
    #[default]
    Maker,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum CcxtMarginMode {
    #[default]
    Cross,
    Isolated,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum CcxtOrderSide {
    #[default]
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum CcxtOrderStatus {
    #[default]
    Open,
    Closed,
    Canceled,
    Expired,
    Rejected,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum CcxtOrderType {
    Market,
    #[default]
    Limit,
    StopLimit,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum CcxtPositionSide {
    #[default]
    Long,
    Short,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum CcxtTimeInForce {
    #[default]
    GTC,
    IOC,
    FOK,
    PO,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CcxtBalance {
    pub info: Value,
    pub timestamp: u64,
    pub datetime: String,
    pub free: HashMap<String, f64>,
    pub used: HashMap<String, f64>,
    pub total: HashMap<String, f64>,
    pub debt: HashMap<String, f64>,
    #[serde(flatten)]
    pub currencies: HashMap<String, CcxtCurrencyBalance>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CcxtCurrencyBalance {
    pub free: f64,
    pub used: f64,
    pub total: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CcxtFee {
    pub cost: Decimal,
    pub currency: String,
    pub rate: Option<Decimal>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CcxtOrderBookLevel {
    pub price: Decimal,
    pub amount: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CcxtOrderBook {
    pub bids: Vec<CcxtOrderBookLevel>,
    pub asks: Vec<CcxtOrderBookLevel>,
    pub symbol: String,
    pub timestamp: Option<i64>,
    pub datetime: Option<DateTime<Utc>>,
    pub nonce: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CcxtOrderFee {
    pub currency: String,
    pub cost: f64,
    pub rate: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CcxtOrder {
    pub id: String,
    pub client_order_id: Option<String>,
    pub datetime: String,
    pub timestamp: u64,
    pub last_trade_timestamp: Option<u64>,
    pub status: CcxtOrderStatus,
    pub symbol: String,
    pub r#type: CcxtOrderType,
    pub time_in_force: Option<CcxtTimeInForce>,
    pub side: CcxtOrderSide,
    pub price: Option<f64>,
    pub average: Option<f64>,
    pub amount: f64,
    pub filled: f64,
    pub remaining: f64,
    pub cost: f64,
    pub trades: Vec<Value>,
    pub fee: Option<CcxtOrderFee>,
    pub trigger_price: Option<f64>,
    pub stop_loss_price: Option<f64>,
    pub take_profit_price: Option<f64>,
    pub reduce_only: bool,
    pub post_only: bool,
    pub info: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CcxtPosition {
    pub info: Value,
    pub id: String,
    pub symbol: String,
    pub timestamp: u64,
    pub datetime: String,
    pub isolated: bool,
    pub hedged: bool,
    pub side: CcxtPositionSide,
    pub contracts: Decimal,
    pub contract_size: Decimal,
    pub entry_price: Decimal,
    pub mark_price: Decimal,
    pub notional: Decimal,
    pub leverage: Decimal,
    pub collateral: Decimal,
    pub initial_margin: Decimal,
    pub maintenance_margin: Decimal,
    pub initial_margin_percentage: Decimal,
    pub maintenance_margin_percentage: Decimal,
    pub unrealized_pnl: Decimal,
    pub liquidation_price: Decimal,
    pub margin_mode: CcxtMarginMode,
    pub percentage: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CcxtTrade {
    pub info: Value,
    pub id: String,
    pub timestamp: i64,
    pub datetime: DateTime<Utc>,
    pub symbol: String,
    pub order: Option<String>,
    #[serde(rename = "type")]
    pub order_type: Option<CcxtOrderType>,
    pub side: Option<CcxtOrderSide>,
    pub taker_or_maker: Option<CcxtLiquiditySide>,
    pub price: Decimal,
    pub amount: Decimal,
    pub cost: Option<Decimal>,
    pub fee: Option<CcxtFee>,
    pub fees: Option<Vec<CcxtFee>>,
}

#[async_trait]
pub trait Ccxt: Send + Sync {
    async fn load_markets(&mut self);
    async fn watch_trades(
        &self,
        symbol: String,
        since: Option<u64>,
        limit: Option<u64>,
    ) -> CcxtTrade;
    async fn watch_trades_for_symbols(
        &self,
        symbols: Vec<String>,
        since: Option<u64>,
        limit: Option<u64>,
    ) -> Vec<CcxtTrade>;
    async fn watch_order_book(&self, symbol: String, limit: Option<u8>) -> CcxtOrderBook;
    async fn watch_balance(&self) -> CcxtBalance;
    async fn watch_orders(
        &self,
        symbol: String,
        since: Option<u64>,
        limit: Option<u64>,
    ) -> CcxtOrder;
    async fn watch_my_trades(
        &self,
        symbols: Vec<String>,
        since: Option<u64>,
        limit: Option<u64>,
    ) -> CcxtTrade;
    async fn watch_positions(&self, symbols: Vec<String>) -> CcxtPosition;

    async fn create_order_ws(
        &self,
        symbol: String,
        type_: CcxtOrderType,
        side: CcxtOrderSide,
        amount: Decimal,
        price: Option<Decimal>,
    ) -> CcxtOrder;
    async fn edit_order_ws(
        &self,
        id: String,
        symbol: Option<String>,
        type_: Option<CcxtOrderType>,
        side: Option<CcxtOrderSide>,
        amount: Option<Decimal>,
        price: Option<Decimal>,
    ) -> CcxtOrder;
    async fn cancel_orders_ws(&self, id: String) -> CcxtOrder;
    async fn cancel_all_orders_ws(&self);
}
