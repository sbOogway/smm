use std::collections::BTreeMap;

use crate::{
    ccxt::{CcxtOrderBook, CcxtOrderBookLevel, CcxtOrderSide, CcxtTrade},
    config::DydxConfig,
    exchange::{Exchange, Info},
    utils::big_decimal_to_decimal,
};
use async_trait::async_trait;
use bigdecimal::Zero;
use dydx::{
    indexer::{
        Feed, IndexerClient, IndexerConfig, OrderSide, OrderbookResponsePriceLevel, OrdersMessage, Price, Quantity, RestConfig, SockConfig, Ticker, TradesMessage,
    }, node::{Subaccount, Wallet},
};

use rust_decimal::Decimal;
use serde_json::Value::Null;

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
    trades_tx: UnboundedSender<CcxtTrade>,
    trades_rx: Mutex<UnboundedReceiver<CcxtTrade>>,
    order_book_tx: UnboundedSender<CcxtOrderBook>,
    order_book_rx: Mutex<UnboundedReceiver<CcxtOrderBook>>,
    
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
        timestamp: None,
        datetime: None,
        nonce: Some(nonce),
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

        let (trades_tx, trades_rx) = mpsc::unbounded_channel::<CcxtTrade>();
        let (order_book_tx, order_book_rx) = mpsc::unbounded_channel::<CcxtOrderBook>();
        Self {
            indexer: Mutex::new(indexer),
            symbols: Vec::new(),
            trades_tx,
            trades_rx: Mutex::new(trades_rx),
            order_book_tx,
            order_book_rx: Mutex::new(order_book_rx),
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
