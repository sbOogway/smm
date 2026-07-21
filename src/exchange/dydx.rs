use std::collections::{BTreeMap, HashMap};

use crate::{
    ccxt::{CcxtBalance, CcxtOrderBook, CcxtOrderBookLevel, CcxtOrderSide, CcxtPosition, CcxtPositionSide, CcxtTrade},
    config::DydxConfig,
    exchange::{Exchange, Info},
    utils::big_decimal_to_decimal,
};
use async_trait::async_trait;
use bigdecimal::{ToPrimitive, Zero};
use chrono::Utc;
use dydx::{
    indexer::{
        Feed, IndexerClient, IndexerConfig, OrderSide, OrderbookResponsePriceLevel, OrdersMessage, Price, Quantity, RestConfig, SockConfig, SubaccountsMessage, Ticker, TradesMessage,
    }, node::Wallet,
};

use rust_decimal::Decimal;

use crate::ccxt::{self, Ccxt};
use tokio::sync::{
    Mutex,
    mpsc::{self, UnboundedReceiver, UnboundedSender},
};

impl From<OrderSide> for CcxtOrderSide {
    fn from(value: OrderSide) -> Self {
        match value {
            OrderSide::Buy => Self::Buy,
            OrderSide::Sell => Self::Sell,
        }
    }
}

pub struct Dydx {
    indexer: Mutex<IndexerClient>,
    symbols: Vec<String>,
    address: String,
    subaccount_number: u32,
    trades_tx: UnboundedSender<CcxtTrade>,
    trades_rx: Mutex<UnboundedReceiver<CcxtTrade>>,
    order_book_tx: UnboundedSender<CcxtOrderBook>,
    order_book_rx: Mutex<UnboundedReceiver<CcxtOrderBook>>,
    balance_tx: UnboundedSender<CcxtBalance>,
    balance_rx: Mutex<UnboundedReceiver<CcxtBalance>>,
    position_tx: UnboundedSender<CcxtPosition>,
    position_rx: Mutex<UnboundedReceiver<CcxtPosition>>,
}
#[derive(Default, Debug)]
pub struct OrderBook {
    // Use `BTreeMap` for easier sorting. 
    pub bids: BTreeMap<Price, (Quantity, u64)>,
    pub asks: BTreeMap<Price, (Quantity, u64)>,
    pub offset: u64,
}
 
impl OrderBook {
    pub fn update_bids(&mut self, bids: Vec<OrderbookResponsePriceLevel>) {
        Self::update(&mut self.bids, bids, &mut self.offset);
    }
 
    pub fn update_asks(&mut self, asks: Vec<OrderbookResponsePriceLevel>) {
        Self::update(&mut self.asks, asks, &mut self.offset);
    }
 
    fn update(map: &mut BTreeMap<Price, (Quantity, u64)>, levels: Vec<OrderbookResponsePriceLevel>, offset: &mut u64) {
        for level in levels {
            if level.size.is_zero() {
                map.remove(&level.price);
            } else {
                map.insert(level.price, (level.size, *offset));
                *offset += 1;
            }
        }
    }
}

async fn handle_trades_feed(
    feed: &mut Feed<TradesMessage>,
    sender: UnboundedSender<CcxtTrade>,
    symbol: &String,
) {
    loop {
        let trades = match feed.recv().await {
            Some(dydx::indexer::TradesMessage::Initial(trades)) => trades
                .contents
                .trades
                .iter()
                .map(|trade| CcxtTrade {
                    id: trade.id.0.clone(),
                    timestamp: trade.created_at.timestamp_millis(),
                    datetime: trade.created_at,
                    symbol: symbol.clone(),
                    order: Some(trade.id.0.clone()),
                    side: Some(trade.side.clone().into()),
                    price: big_decimal_to_decimal(trade.price.0.clone()),
                    amount: big_decimal_to_decimal(trade.size.0.clone()),
                    ..Default::default()
                })
                .collect::<Vec<CcxtTrade>>(),
            Some(dydx::indexer::TradesMessage::Update(trades)) => {
                let trades_update_contents = trades.contents;

                trades_update_contents
                    .iter()
                    .flat_map(|update| {
                        update
                            .trades
                            .iter()
                            .map(|trade| CcxtTrade {
                                id: trade.id.0.clone(),
                                timestamp: trade.created_at.timestamp_millis(),
                                datetime: trade.created_at,
                                symbol: symbol.clone(),
                                order: Some(trade.id.0.clone()),
                                side: Some(trade.side.clone().into()),
                                price: big_decimal_to_decimal(trade.price.0.clone()),
                                amount: big_decimal_to_decimal(trade.size.0.clone()),
                                ..Default::default()
                            })
                            .collect::<Vec<CcxtTrade>>()
                    })
                    .collect::<Vec<CcxtTrade>>()
            }

            None => Vec::new(),
        };

        for trade in trades {
            let _ = sender.send(trade.clone());
        }
    }
}

fn book_to_ccxt(order_book: &OrderBook, symbol: &str, nonce: u64) -> CcxtOrderBook {
    let best_bid = order_book.bids.last_key_value();
    let best_ask = order_book.asks.first_key_value();

    let bids = best_bid
        .into_iter()
        .map(|(price, (size, _))| CcxtOrderBookLevel {
            price: big_decimal_to_decimal(price.0.clone()),
            amount: big_decimal_to_decimal(size.0.clone()),
        })
        .collect();

    let asks = best_ask
        .into_iter()
        .map(|(price, (size, _))| CcxtOrderBookLevel {
            price: big_decimal_to_decimal(price.0.clone()),
            amount: big_decimal_to_decimal(size.0.clone()),
        })
        .collect();

    CcxtOrderBook {
        bids,
        asks,
        symbol: symbol.to_string(),
        nonce: Some(nonce),
        ..Default::default()
    }
}

async fn handle_order_book_feed(
    feed: &mut Feed<OrdersMessage>,
    sender: UnboundedSender<CcxtOrderBook>,
    symbol: &String,
) {
    let mut book = OrderBook::default();

    loop {
        match feed.recv().await {
            Some(dydx::indexer::OrdersMessage::Initial(initial)) => {
                book.bids.clear();
                book.asks.clear();
                book.update_bids(initial.contents.bids);
                book.update_asks(initial.contents.asks);
                let ob = book_to_ccxt(&book, symbol, initial.message_id);
                let _ = sender.send(ob);
            }
            Some(dydx::indexer::OrdersMessage::Update(update)) => {
                if let Some(bids) = update.contents.bids {
                    book.update_bids(bids);
                }
                if let Some(asks) = update.contents.asks {
                    book.update_asks(asks);
                }
                let ob = book_to_ccxt(&book, symbol, update.message_id);
                let _ = sender.send(ob);
            }
            None => {
                tracing::warn!("order book feed closed for {symbol}");
                break;
            }
        }
    }
}

async fn handle_subaccount_feed(
    feed: &mut Feed<SubaccountsMessage>,
    balance_tx: UnboundedSender<CcxtBalance>,
    position_tx: UnboundedSender<CcxtPosition>,
) {
    loop {
        match feed.recv().await {
            Some(SubaccountsMessage::Initial(initial)) => {
                let subaccount = &initial.contents.subaccount;
                let free_collateral = big_decimal_to_decimal(subaccount.free_collateral.clone())
                    .to_f64()
                    .unwrap_or(0.0);
                let mut usdc_balance = free_collateral;

                for (ticker, asset) in &subaccount.asset_positions {
                    if ticker.0 == "USDC" {
                        usdc_balance = big_decimal_to_decimal(asset.size.0.clone())
                            .to_f64()
                            .unwrap_or(0.0);
                    }
                }

                let mut free = HashMap::new();
                free.insert("USDC".into(), usdc_balance);

                let _ = balance_tx.send(CcxtBalance {
                    timestamp: Utc::now().timestamp() as u64,
                    datetime: Utc::now().to_rfc3339(),
                    free,
                    ..Default::default()
                });

                for (_ticker, position) in &subaccount.open_perpetual_positions {
                    let side = match position.side {
                        dydx::indexer::PositionSide::Long => CcxtPositionSide::Long,
                        dydx::indexer::PositionSide::Short => CcxtPositionSide::Short,
                    };
                    let contracts = big_decimal_to_decimal(position.size.0.clone());
                    let entry_price = big_decimal_to_decimal(position.entry_price.0.clone());
                    let unrealized_pnl = big_decimal_to_decimal(position.unrealized_pnl.clone());

                    let _ = position_tx.send(CcxtPosition {
                        id: position.market.0.clone(),
                        symbol: position.market.0.clone(),
                        timestamp: Utc::now().timestamp() as u64,
                        datetime: Utc::now().to_rfc3339(),
                        side,
                        contracts,
                        contract_size: Decimal::ONE,
                        entry_price,
                        unrealized_pnl,
                        liquidation_price: Decimal::ZERO,
                        ..Default::default()
                    });
                }
            }
            Some(SubaccountsMessage::Update(update)) => {
                let mut free = HashMap::new();

                for content in &update.contents {
                    if let Some(asset_positions) = &content.asset_positions {
                        for asset in asset_positions {
                            let size = big_decimal_to_decimal(asset.size.0.clone())
                                .to_f64()
                                .unwrap_or(0.0);
                            let symbol = asset.symbol.0.clone();
                            free.insert(symbol, size);
                        }
                    }

                    if let Some(perpetual_positions) = &content.perpetual_positions {
                        for position in perpetual_positions {
                            let side = match position.side {
                                dydx::indexer::PositionSide::Long => CcxtPositionSide::Long,
                                dydx::indexer::PositionSide::Short => CcxtPositionSide::Short,
                            };
                            let contracts = big_decimal_to_decimal(position.size.0.clone());
                            let entry_price = big_decimal_to_decimal(position.entry_price.0.clone());
                            let unrealized_pnl = position.unrealized_pnl.as_ref()
                                .map(|v| big_decimal_to_decimal(v.clone()))
                                .unwrap_or(Decimal::ZERO);

                            let _ = position_tx.send(CcxtPosition {
                                id: position.position_id.clone(),
                                symbol: position.market.0.clone(),
                                timestamp: Utc::now().timestamp() as u64,
                                datetime: Utc::now().to_rfc3339(),
                                side,
                                contracts,
                                contract_size: Decimal::ONE,
                                entry_price,
                                unrealized_pnl,
                                liquidation_price: Decimal::ZERO,
                                ..Default::default()
                            });
                        }
                    }
                }

                if !free.is_empty() {
                    let _ = balance_tx.send(CcxtBalance {
                        timestamp: Utc::now().timestamp() as u64,
                        datetime: Utc::now().to_rfc3339(),
                        free,
                        ..Default::default()
                    });
                }
            }
            None => {
                tracing::warn!("balance feed closed");
                break;
            }
        }
    }
}

impl Dydx {
    pub fn new(cfg: &DydxConfig) -> Self {
        let sock_cfg = SockConfig {
            endpoint: cfg.indexer_ws_endpoint.clone(),
            timeout: 5_000,
            rate_limit: std::num::NonZeroU32::new(2).unwrap(),
        };
        let rest_cfg = RestConfig {
            endpoint: "http://localhost".into(),
        };
        let indexer_cfg = IndexerConfig {
            rest: rest_cfg,
            sock: sock_cfg,
        };

        let indexer = IndexerClient::new(indexer_cfg);

        let wallet = match Wallet::from_mnemonic(&cfg.mnemonic) {
            Ok(w) => w,
            Err(e) => {
                tracing::error!(error = %e, "failed to create wallet from mnemonic");
                panic!();
            }
        };
        let account = match wallet.account_offline(0) {
            Ok(a) => a,
            Err(e) => {
                tracing::error!(error = %e, "failed to derive account");
                panic!();
            }
        };
        tracing::info!(address = %account.address(), "dydx wallet derived");

        let (trades_tx, trades_rx) = mpsc::unbounded_channel::<CcxtTrade>();
        let (order_book_tx, order_book_rx) = mpsc::unbounded_channel::<CcxtOrderBook>();
        let (balance_tx, balance_rx) = mpsc::unbounded_channel::<CcxtBalance>();
        let (position_tx, position_rx) = mpsc::unbounded_channel::<CcxtPosition>();
        Self {
            indexer: Mutex::new(indexer),
            symbols: Vec::new(),
            address: account.address().to_string(),
            subaccount_number: cfg.subaccount_number,
            trades_tx,
            trades_rx: Mutex::new(trades_rx),
            order_book_tx,
            order_book_rx: Mutex::new(order_book_rx),
            balance_tx,
            balance_rx: Mutex::new(balance_rx),
            position_tx,
            position_rx: Mutex::new(position_rx),
        }
    }
}

#[async_trait]
impl Ccxt for Dydx {
    async fn load_markets(&mut self) {
        {
            let mut indexer = self.indexer.lock().await;
            let subaccount: dydx::indexer::Subaccount = dydx::indexer::Subaccount::new(
                self.address.parse().unwrap(),
                self.subaccount_number.try_into().unwrap(),
            );
            let mut balance_feed = indexer.feed().subaccounts(subaccount, false).await.unwrap();
            let balance_tx = self.balance_tx.clone();
            let position_tx = self.position_tx.clone();
            tokio::spawn(async move {
                handle_subaccount_feed(&mut balance_feed, balance_tx, position_tx).await;
            });
        }

        for symbol in self.symbols() {
            let ticker = Ticker(symbol.clone());

            let mut indexer = self.indexer.lock().await;
            let mut trades_feed = indexer.feed().trades(&ticker, false).await.unwrap();
            let mut order_book_feed = indexer.feed().orders(&ticker, false).await.unwrap();

            let symbol_clone = symbol.clone();
            let sender_trades_clone = self.trades_tx.clone();
            let sender_order_book_clone = self.order_book_tx.clone();

            tokio::spawn(async move {
                handle_trades_feed(&mut trades_feed, sender_trades_clone, &symbol_clone).await;
            });

            tokio::spawn(async move {
                handle_order_book_feed(&mut order_book_feed, sender_order_book_clone, &symbol).await;
            });

            // self.trades_feed.insert(symbol.clone(), trades_feed);
            // self.order_book_feed.insert(symbol.clone(), order_book_feed);
        }
    }

    async fn watch_trades(
        &self,
        symbol: String,
        _since: Option<u64>,
        _limit: Option<u64>,
    ) -> ccxt::CcxtTrade {
        let mut rx = self.trades_rx.lock().await;
        loop {
            match rx.recv().await {
                Some(trade) if trade.symbol == symbol => return trade,
                Some(_) => continue,
                None => panic!("trades channel closed"),
            }
        }
    }

    async fn watch_trades_for_symbols(
        &self,
        symbols: Vec<String>,
        _since: Option<u64>,
        _limit: Option<u64>,
    ) -> Vec<ccxt::CcxtTrade> {
        todo!()
    }

    async fn watch_order_book(&self, symbol: String, _limit: Option<u8>) -> ccxt::CcxtOrderBook {
        let mut rx = self.order_book_rx.lock().await;
        loop {
            match rx.recv().await {
                Some(ob) if ob.symbol == symbol => return ob,
                Some(_) => continue,
                None => panic!("order book channel closed"),
            }
        }
    }

    async fn watch_balance(&self) -> ccxt::CcxtBalance {
        let mut rx = self.balance_rx.lock().await;
        loop {
            match rx.recv().await {
                Some(balance) => return balance,
                None => panic!("balance channel closed"),
            }
        }
    }

    async fn watch_orders(
        &self,
        _symbol: String,
        _since: Option<u64>,
        _limit: Option<u64>,
    ) -> ccxt::CcxtOrder {
        todo!()
    }

    async fn watch_my_trades(
        &self,
        _symbols: Vec<String>,
        _since: Option<u64>,
        _limit: Option<u64>,
    ) -> ccxt::CcxtTrade {
        todo!()
    }

    async fn watch_positions(&self, _symbols: Vec<String>) -> ccxt::CcxtPosition {
        let mut rx = self.position_rx.lock().await;
        loop {
            match rx.recv().await {
                Some(position) => return position,
                None => panic!("position channel closed"),
            }
        }
    }

    async fn create_order_ws(
        &self,
        _symbol: String,
        _type_: ccxt::CcxtOrderType,
        _side: ccxt::CcxtOrderSide,
        _amount: rust_decimal::prelude::Decimal,
        _price: Option<rust_decimal::prelude::Decimal>,
    ) -> ccxt::CcxtOrder {
        todo!()
    }

    async fn edit_order_ws(
        &self,
        _id: String,
        _symbol: Option<String>,
        _type_: Option<ccxt::CcxtOrderType>,
        _side: Option<ccxt::CcxtOrderSide>,
        _amount: Option<rust_decimal::prelude::Decimal>,
        _price: Option<rust_decimal::prelude::Decimal>,
    ) -> ccxt::CcxtOrder {
        todo!()
    }

    async fn cancel_orders_ws(&self, _id: String) -> ccxt::CcxtOrder {
        todo!()
    }

    async fn cancel_all_orders_ws(&self) {
        todo!()
    }
}
impl Info for Dydx {
    fn symbols(&self) -> Vec<String> {
        self.symbols.clone()
    }

    fn name(&self) -> String {
        "dydx".into()
    }

    fn add_symbol(&mut self, symbol: String) {
        self.symbols.push(symbol);
    }
}

impl Exchange for Dydx {}

#[cfg(test)]
mod tests {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::connect_async;
    use tokio_tungstenite::tungstenite::Message;

    #[tokio::test]
    async fn ping_latency_under_500ms() {
        let (mut ws_stream, response) = connect_async("wss://indexer.dydx.trade/v4/ws")
            .await
            .expect("failed to connect");

        assert!(
            response.status().is_success() || response.status().as_u16() == 101,
            "unexpected status: {}",
            response.status(),
        );

        let payload: Vec<u8> = std::time::Instant::now()
            .elapsed()
            .as_nanos()
            .to_be_bytes()
            .to_vec();
        let start = std::time::Instant::now();

        ws_stream
            .send(Message::Ping(payload.clone().into()))
            .await
            .expect("failed to send ping");

        loop {
            let msg = tokio::time::timeout(std::time::Duration::from_secs(5), ws_stream.next())
                .await
                .expect("timeout waiting for pong")
                .expect("stream ended")
                .expect("message error");

            match msg {
                Message::Pong(data) if data.as_ref() == payload.as_slice() => break,
                _ => continue,
            }
        }

        let latency = start.elapsed();
        println!("ping latency dydx: {latency:?}");
        assert!(
            latency < std::time::Duration::from_millis(500),
            "ping latency too high: {latency:?}",
        );
    }
}
