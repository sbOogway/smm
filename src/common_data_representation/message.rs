pub mod bbo_update;
pub mod trade_update;

pub use bbo_update::BboUpdate;
pub use trade_update::TradeUpdate;

#[derive(Clone)]
pub enum Message {
    Empty,
    TradeUpdate(TradeUpdate),
    BboUpdate(BboUpdate),
}

impl Message {
    pub fn empty() -> Self {
        Self::Empty
    }

    pub fn handle(&self, seq: i64, batch: bool) {
        match self {
            Self::Empty => {}
            Self::TradeUpdate(update) => update.handle(seq, batch),
            Self::BboUpdate(update) => update.handle(seq, batch),
        }
    }
}
