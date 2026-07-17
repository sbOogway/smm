//! dydx exchange implementation.
//!
//! <https://docs.dydx.xyz>

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::{future::Future, pin::Pin};

use bigdecimal::BigDecimal as BigDec;
use disruptor::{MultiProducer, Producer, SingleConsumerBarrier};
use dydx::indexer::{
    IndexerClient, IndexerConfig, OrderSide, OrdersMessage, PositionSide, RestConfig, SockConfig,
    Subaccount, SubaccountsMessage, Ticker, TradesMessage,
};
use dydx::node::Wallet;
use rust_decimal::Decimal;

use crate::{
    config::DydxConfig,
    exchange::{DataProvider, Exchange, Executor, Infos},
    types::message::{
        BalanceUpdate, BboUpdate, Message as AppMessage, PositionInfo, TradeUpdate,
    },
    types::portfolio::Portfolio,
};

fn bd_to_dec(bd: &BigDec) -> Decimal {
    Decimal::from_str(&bd.to_string()).expect("bigdecimal to decimal conversion")
}

pub struct Dydx {
    tickers: Vec<String>,
    portfolio: Arc<Mutex<Portfolio>>,
    config: DydxConfig,
}

impl Dydx {
    pub fn new(cfg: DydxConfig) -> Self {
        Self {
            tickers: cfg.tickers.clone(),
            portfolio: Arc::new(Mutex::new(Portfolio {
                equity: Decimal::ZERO,
                balances: HashMap::new(),
                positions: HashMap::new(),
            })),
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
            let mid = (best_bid_price + best_ask_price) / Decimal::new(2, 0);

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

async fn handle_subaccounts_feed(
    mut disruptor: MultiProducer<AppMessage, SingleConsumerBarrier>,
    mut feed: dydx::indexer::Feed<SubaccountsMessage>,
    portfolio: Arc<Mutex<Portfolio>>,
) {
    let mut address = String::new();
    while let Some(msg) = feed.recv().await {
        match msg {
            SubaccountsMessage::Initial(init) => {
                let subaccount = &init.contents.subaccount;
                tracing::info!("{subaccount:#?}");

                let mut balances = HashMap::new();
                for (ticker, pos) in &subaccount.asset_positions {
                    let balance = bd_to_dec(&pos.size.0);
                    balances.insert(ticker.0.clone(), balance);
                }

                let mut positions = HashMap::new();
                for (market, pos) in &subaccount.open_perpetual_positions {
                    let size = match pos.side {
                        PositionSide::Short => -bd_to_dec(&pos.size.0),
                        PositionSide::Long => bd_to_dec(&pos.size.0),
                    };
                    let entry_price = bd_to_dec(&pos.entry_price.0);
                    let realized_pnl = bd_to_dec(&pos.realized_pnl);
                    let unrealized_pnl = bd_to_dec(&pos.unrealized_pnl);
                    let value = size * entry_price + realized_pnl + unrealized_pnl;
                    positions.insert(
                        market.0.clone(),
                        PositionInfo {
                            size,
                            entry_price,
                            realized_pnl,
                            unrealized_pnl,
                            net_funding: bd_to_dec(&pos.net_funding),
                            value,
                        },
                    );
                }

                let cash: Decimal = balances.values().sum();
                let position_value: Decimal = positions.values().map(|p| p.value).sum();
                let equity = cash + position_value;

                {
                    let mut p = portfolio.lock().unwrap();
                    p.equity = equity;
                    p.balances = balances.clone();
                    p.positions = positions.clone();
                }

                address = String::from(subaccount.address.clone());

                let update = BalanceUpdate {
                    exchange: "dydx".into(),
                    address: address.clone(),
                    equity,
                    free_collateral: cash,
                    balances: balances.clone(),
                    positions: positions.clone(),
                };
                tracing::info!("{update:#?}");
                disruptor.publish(|slot: &mut AppMessage| {
                    *slot = AppMessage::BalanceUpdate(update);
                });
            }
            SubaccountsMessage::Update(upd) => {
                let mut portfolio_guard = portfolio.lock().unwrap();
                for content in &upd.contents {
                    tracing::info!("{content:#?}");
                    if let Some(asset_positions) = &content.asset_positions {
                        for pos in asset_positions {
                            let balance = bd_to_dec(&pos.size.0);
                            portfolio_guard
                                .balances
                                .insert(pos.symbol.0.clone(), balance);
                        }
                    }
                    if let Some(perp_positions) = &content.perpetual_positions {
                        for pos in perp_positions {
                            let size = match pos.side {
                                PositionSide::Short => -bd_to_dec(&pos.size.0),
                                PositionSide::Long => bd_to_dec(&pos.size.0),
                            };
                            let entry_price = bd_to_dec(&pos.entry_price.0);
                            let realized_pnl =
                                pos.realized_pnl.as_ref().map_or(Decimal::ZERO, bd_to_dec);
                            let unrealized_pnl =
                                pos.unrealized_pnl.as_ref().map_or(Decimal::ZERO, bd_to_dec);
                            let value = size * entry_price + realized_pnl + unrealized_pnl;
                            portfolio_guard.positions.insert(
                                pos.market.0.clone(),
                                PositionInfo {
                                    size,
                                    entry_price,
                                    realized_pnl,
                                    unrealized_pnl,
                                    net_funding: bd_to_dec(&pos.net_funding),
                                    value,
                                },
                            );
                        }
                    }
                }

                let cash: Decimal = portfolio_guard.balances.values().sum();
                let position_value: Decimal =
                    portfolio_guard.positions.values().map(|p| p.value).sum();
                portfolio_guard.equity = cash + position_value;

                let snapshot = portfolio_guard.clone();
                drop(portfolio_guard);

                let update = BalanceUpdate {
                    exchange: "dydx".into(),
                    address: address.clone(),
                    equity: snapshot.equity,
                    free_collateral: cash,
                    balances: snapshot.balances.clone(),
                    positions: snapshot.positions.clone(),
                };
                tracing::info!("{update:#?}");
                disruptor.publish(|slot: &mut AppMessage| {
                    *slot = AppMessage::BalanceUpdate(update);
                });
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
        let portfolio = self.portfolio.clone();

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

            let portfolio = portfolio.clone();
            match indexer.feed().subaccounts(subaccount, false).await {
                Ok(feed) => {
                    let d = d.clone();
                    handles.push(tokio::spawn(async move {
                        handle_subaccounts_feed(d, feed, portfolio).await;
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

    fn get_portfolio(&self) -> Portfolio {
        self.portfolio.lock().unwrap().clone()
    }
}

impl Exchange for Dydx {}
