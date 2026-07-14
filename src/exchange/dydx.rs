//! dydx exchange implementation.
//!
//! <https://docs.dydx.xyz>

use std::str::FromStr;
use std::{future::Future, pin::Pin, sync::OnceLock};

use bigdecimal::BigDecimal as BigDec;
use disruptor::{MultiProducer, Producer, SingleConsumerBarrier};
use dydx::indexer::{
    IndexerClient, IndexerConfig, OrderSide, OrdersMessage, RestConfig, SockConfig,
    Subaccount, SubaccountsMessage, Ticker, TradesMessage,
};
use dydx::node::Wallet;
use rust_decimal::Decimal;
use tokio::sync::watch;

use crate::{
    common_data_representation::message::{BboUpdate, Message as AppMessage, TradeUpdate},
    config::DydxConfig,
    exchange::{DataProvider, Exchange, Executor, Infos},
};

fn bd_to_dec(bd: &BigDec) -> Decimal {
    Decimal::from_str(&bd.to_string()).expect("bigdecimal to decimal conversion")
}

static BALANCE_TX: OnceLock<watch::Sender<Decimal>> = OnceLock::new();

pub struct Dydx {
    tickers: Vec<String>,
    balance_rx: watch::Receiver<Decimal>,
    config: DydxConfig,
}

impl Dydx {
    pub fn new(cfg: DydxConfig) -> Self {
        let (balance_tx, balance_rx) = watch::channel(Decimal::ZERO);
        let _ = BALANCE_TX.set(balance_tx);
        Self {
            tickers: cfg.tickers.clone(),
            balance_rx,
            config: cfg,
        }
    }
}

async fn handle_trades_feed(
    mut disruptor: MultiProducer<AppMessage, SingleConsumerBarrier>,
    mut feed: dydx::indexer::Feed<TradesMessage>,
    ticker: String,
) {
    while let Some(msg) = feed.recv().await {
        match msg {
            TradesMessage::Initial(init) => {
                for trade in init.contents.trades {
                    let price = bd_to_dec(&trade.price.0);
                    let size = bd_to_dec(&trade.size.0);
                    let side = match trade.side {
                        OrderSide::Buy => "buy",
                        OrderSide::Sell => "sell",
                    };
                    let time = trade.created_at.timestamp() as u64;
                    let msg = AppMessage::TradeUpdate(TradeUpdate {
                        exchange: "dydx".into(),
                        symbol: ticker.clone(),
                        side: side.into(),
                        price,
                        size,
                        time,
                    });
                    disruptor.publish(|slot: &mut AppMessage| {
                        *slot = msg;
                    });
                }
            }
            TradesMessage::Update(upd) => {
                for contents in upd.contents {
                    for trade in contents.trades {
                        let price = bd_to_dec(&trade.price.0);
                        let size = bd_to_dec(&trade.size.0);
                        let side = match trade.side {
                            OrderSide::Buy => "buy",
                            OrderSide::Sell => "sell",
                        };
                        let time = trade.created_at.timestamp() as u64;
                        let msg = AppMessage::TradeUpdate(TradeUpdate {
                            exchange: "dydx".into(),
                            symbol: ticker.clone(),
                            side: side.into(),
                            price,
                            size,
                            time,
                        });
                        disruptor.publish(|slot: &mut AppMessage| {
                            *slot = msg;
                        });
                    }
                }
            }
        }
    }
}

async fn handle_orders_feed(
    mut disruptor: MultiProducer<AppMessage, SingleConsumerBarrier>,
    mut feed: dydx::indexer::Feed<OrdersMessage>,
    ticker: String,
) {
    let mut best_bid_price = Decimal::ZERO;
    let mut best_bid_size = Decimal::ZERO;
    let mut best_ask_price = Decimal::ZERO;
    let mut best_ask_size = Decimal::ZERO;

    while let Some(msg) = feed.recv().await {
        let (bids, asks) = match msg {
            OrdersMessage::Initial(init) => (init.contents.bids, init.contents.asks),
            OrdersMessage::Update(upd) => (
                upd.contents.bids.unwrap_or_default(),
                upd.contents.asks.unwrap_or_default(),
            ),
        };

        for level in &bids {
            let price = bd_to_dec(&level.price.0);
            let size = bd_to_dec(&level.size.0);
            if size.is_zero() && price == best_bid_price {
                best_bid_price = Decimal::ZERO;
                best_bid_size = Decimal::ZERO;
            } else if !size.is_zero() && price > best_bid_price {
                best_bid_price = price;
                best_bid_size = size;
            }
        }

        for level in &asks {
            let price = bd_to_dec(&level.price.0);
            let size = bd_to_dec(&level.size.0);
            if size.is_zero() && price == best_ask_price {
                best_ask_price = Decimal::ZERO;
                best_ask_size = Decimal::ZERO;
            } else if !size.is_zero() && (best_ask_price.is_zero() || price < best_ask_price) {
                best_ask_price = price;
                best_ask_size = size;
            }
        }

        if best_bid_price > Decimal::ZERO && best_ask_price > Decimal::ZERO {
            let mid = (&best_bid_price + &best_ask_price) / Decimal::new(2, 0);

            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            let msg = AppMessage::BboUpdate(BboUpdate {
                exchange: "dydx".into(),
                symbol: ticker.clone(),
                bid_price: best_bid_price,
                bid_size: best_bid_size,
                ask_price: best_ask_price,
                ask_size: best_ask_size,
                time: now,
                mid_price: mid,
            });

            disruptor.publish(|slot: &mut AppMessage| {
                *slot = msg;
            });
        }
    }
}

async fn handle_subaccounts_feed(mut feed: dydx::indexer::Feed<SubaccountsMessage>) {
    while let Some(msg) = feed.recv().await {
        match msg {
            SubaccountsMessage::Initial(init) => {
                let equity = bd_to_dec(&init.contents.subaccount.equity);
                tracing::info!(%equity, "dydx subaccount equity");
                if let Some(tx) = BALANCE_TX.get() {
                    let _ = tx.send(equity);
                }
            }
            SubaccountsMessage::Update(_) => {
                tracing::info!("dydx subaccount update received");
            }
        }
    }
}

impl Infos for Dydx {
    fn name(&self) -> String {
        "dydx".into()
    }

    fn symbols(&self) -> Vec<String> {
        self.tickers.clone()
    }
}

impl DataProvider for Dydx {
    fn listen(
        &self,
        disruptor: MultiProducer<AppMessage, SingleConsumerBarrier>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        let cfg = self.config.clone();
        let tickers = self.tickers.clone();
        let d = disruptor.clone();

        Box::pin(async move {
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

            let mut indexer = IndexerClient::new(indexer_cfg);

            let wallet = match Wallet::from_mnemonic(&cfg.mnemonic) {
                Ok(w) => w,
                Err(e) => {
                    tracing::error!(error = %e, "failed to create wallet from mnemonic");
                    return;
                }
            };
            let account = match wallet.account_offline(0) {
                Ok(a) => a,
                Err(e) => {
                    tracing::error!(error = %e, "failed to derive account");
                    return;
                }
            };
            tracing::info!(address = %account.address(), "dydx wallet derived");
            let subaccount: Subaccount = match account.subaccount(cfg.subaccount_number) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(error = %e, "failed to create subaccount");
                    return;
                }
            };

            let mut handles = Vec::new();

            for ticker_str in &tickers {
                let ticker = Ticker(ticker_str.clone());

                match indexer.feed().trades(&ticker, false).await {
                    Ok(feed) => {
                        let d = d.clone();
                        let t = ticker_str.clone();
                        handles.push(tokio::spawn(async move {
                            handle_trades_feed(d, feed, t).await;
                        }));
                    }
                    Err(e) => {
                        tracing::error!(ticker = %ticker_str, error = %e, "failed to subscribe to trades");
                    }
                }

                let d = d.clone();
                let t = ticker_str.clone();
                match indexer.feed().orders(&ticker, false).await {
                    Ok(feed) => {
                        handles.push(tokio::spawn(async move {
                            handle_orders_feed(d, feed, t).await;
                        }));
                    }
                    Err(e) => {
                        tracing::error!(ticker = %ticker_str, error = %e, "failed to subscribe to orderbook");
                    }
                }
            }

            match indexer.feed().subaccounts(subaccount, false).await {
                Ok(feed) => {
                    handles.push(tokio::spawn(async move {
                        handle_subaccounts_feed(feed).await;
                    }));
                }
                Err(e) => {
                    tracing::error!(error = %e, "failed to subscribe to subaccounts");
                }
            }

            drop(indexer);

            for h in handles {
                let _ = h.await;
            }
        })
    }
}

impl Executor for Dydx {
    fn create_order(&self) {
        todo!()
    }

    fn update_order(&self) {
        todo!()
    }

    fn cancel_order(&self) {
        todo!()
    }

    fn balance_of(&self, _symbol: String) {
        let equity = *self.balance_rx.borrow();
        tracing::info!(%equity, "dydx balance");
    }
}

impl Exchange for Dydx {}
