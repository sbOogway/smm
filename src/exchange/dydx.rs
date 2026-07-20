use std::sync::Arc;

use crate::{
    ccxt::{CcxtMessage, CcxtOrderBook, CcxtOrderBookLevel, CcxtOrderSide, CcxtTrade},
    config::DydxConfig,
    exchange::{Exchange, Info},
    utils::big_decimal_to_decimal,
};
use async_trait::async_trait;
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use dydx::{
    indexer::{
        Feed, IndexerClient, IndexerConfig, OrderSide, OrdersMessage, Price, RestConfig,
        SockConfig, Symbol, Ticker, TradesMessage,
    },
    node::{Subaccount, Wallet},
};
use futures_util::stream::{FuturesUnordered, StreamExt};
use rust_decimal::Decimal;
use serde_json::Value::Null;

use crate::ccxt::{self, Ccxt};
use tokio::sync::{
    Mutex,
    watch::{self, Receiver, Sender},
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
    trades_tx: Sender<CcxtTrade>,
    trades_rx: Receiver<CcxtTrade>,
    order_book_tx: Sender<CcxtOrderBook>,
    order_book_rx: Receiver<CcxtOrderBook>,
    // trades_feed: HashMap<String, Feed<TradesMessage>>,
    // order_book_feed: HashMap<String, Feed<OrdersMessage>>,
}

async fn handle_trades_feed(
    feed: &mut Feed<TradesMessage>,
    sender: Sender<CcxtTrade>,
    symbol: &String,
) {
    loop {
        let trades = match feed.recv().await {
            Some(dydx::indexer::TradesMessage::Initial(trades)) => trades
                .contents
                .trades
                .iter()
                .map(|trade| CcxtTrade {
                    info: Null,
                    id: trade.id.0.clone(),
                    timestamp: trade.created_at.timestamp_millis(),
                    datetime: trade.created_at,
                    symbol: symbol.clone(),
                    order: Some(trade.id.0.clone()),
                    order_type: None,
                    side: Some(trade.side.clone().into()),
                    taker_or_maker: None,
                    price: big_decimal_to_decimal(trade.price.0.clone()),
                    amount: big_decimal_to_decimal(trade.size.0.clone()),
                    cost: None,
                    fee: None,
                    fees: None,
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
                                info: Null,
                                id: trade.id.0.clone(),
                                timestamp: trade.created_at.timestamp_millis(),
                                datetime: trade.created_at,
                                symbol: symbol.clone(),
                                order: Some(trade.id.0.clone()),
                                order_type: None,
                                side: Some(trade.side.clone().into()),
                                taker_or_maker: None,
                                price: big_decimal_to_decimal(trade.price.0.clone()),
                                amount: big_decimal_to_decimal(trade.size.0.clone()),
                                cost: None,
                                fee: None,
                                fees: None,
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

async fn handle_order_book_feed(feed: &mut Feed<OrdersMessage>, sender: Sender<CcxtOrderBook>, symbol: &String) {
    loop {
        let order_book = match feed.recv().await {
            Some(dydx::indexer::OrdersMessage::Initial(order_book)) => {
                let best_bid_price = big_decimal_to_decimal(
                    order_book.contents.bids.first().unwrap().price.0.clone(),
                );
                let best_bid_amount = big_decimal_to_decimal(
                    order_book.contents.bids.first().unwrap().size.0.clone(),
                );
                let best_ask_price = big_decimal_to_decimal(
                    order_book.contents.asks.first().unwrap().price.0.clone(),
                );
                let best_ask_amount = big_decimal_to_decimal(
                    order_book.contents.asks.first().unwrap().size.0.clone(),
                );
                CcxtOrderBook {
                    bids: vec![CcxtOrderBookLevel {
                        price: best_bid_price,
                        amount: best_bid_amount,
                    }],
                    asks: vec![CcxtOrderBookLevel {
                        price: best_ask_price,
                        amount: best_ask_amount,
                    }],
                    symbol: symbol.to_string(),
                    timestamp: None,
                    datetime: None,
                    nonce: None,
                }
            }
            Some(dydx::indexer::OrdersMessage::Update(order_book)) => {
                let (best_bid_price, best_bid_amount) = match order_book.contents.bids.clone() {
                    Some(bids) => (
                        big_decimal_to_decimal(bids.first().unwrap().price.0.clone()),
                        big_decimal_to_decimal(bids.first().unwrap().size.0.clone()),
                    ),
                    None => (Decimal::ZERO, Decimal::ZERO),
                };
                let (best_ask_price, best_ask_amount) = match order_book.contents.asks.clone() {
                    Some(asks) => (
                        big_decimal_to_decimal(asks.first().unwrap().price.0.clone()),
                        big_decimal_to_decimal(asks.first().unwrap().size.0.clone()),
                    ),
                    None => (Decimal::ZERO, Decimal::ZERO),
                };
                CcxtOrderBook {
                    bids: vec![CcxtOrderBookLevel {
                        price: best_bid_price,
                        amount: best_bid_amount,
                    }],
                    asks: vec![CcxtOrderBookLevel {
                        price: best_ask_price,
                        amount: best_ask_amount,
                    }],
                    symbol: symbol.to_string(),
                    timestamp: None,
                    datetime: None,
                    nonce: None,
                }
            }
            None => CcxtOrderBook {
                bids: todo!(),
                asks: todo!(),
                symbol: symbol.to_string(),
                timestamp: todo!(),
                datetime: todo!(),
                nonce: todo!(),
            },
        };
        let _ = sender.send(order_book.clone());
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
        let _subaccount: Subaccount = match account.subaccount(cfg.subaccount_number) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "failed to create subaccount");
                panic!();
            }
        };

        let (trades_tx, trades_rx) = watch::channel::<CcxtTrade>(CcxtTrade {
            info: Null,
            id: "null".into(),
            timestamp: 0,
            datetime: Utc::now(),
            symbol: "none".into(),
            order: None,
            order_type: None,
            side: None,
            taker_or_maker: None,
            price: Decimal::ZERO,
            amount: Decimal::ZERO,
            cost: None,
            fee: None,
            fees: None,
        });
        let (order_book_tx, order_book_rx) = watch::channel::<CcxtOrderBook>(CcxtOrderBook {
            bids: Vec::new(),
            asks: Vec::new(),
            symbol: "none".into(),
            timestamp: None,
            datetime: None,
            nonce: None,
        });
        Self {
            indexer: Mutex::new(indexer),
            // trades_feed: HashMap::new(),
            // order_book_feed: HashMap::new(),
            symbols: Vec::new(),
            trades_tx,
            trades_rx,
            order_book_tx,
            order_book_rx,
        }
    }
}

#[async_trait]
impl Ccxt for Dydx {
    async fn load_markets(&mut self) {
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


    }

    async fn watch_balance(&self) -> ccxt::CcxtBalance {
        todo!()
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
        todo!()
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
