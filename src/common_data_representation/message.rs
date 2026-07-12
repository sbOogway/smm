pub mod asmm_quote;
pub mod bbo_update;
pub mod trade_update;

pub use asmm_quote::AsmmQuote;
pub use bbo_update::BboUpdate;
pub use trade_update::TradeUpdate;

#[derive(Clone, Debug)]
pub enum Message {
    Empty,
    TradeUpdate(TradeUpdate),
    BboUpdate(BboUpdate),
    AsmmQuote(AsmmQuote),
}

impl Message {
    pub fn empty() -> Self {
        Self::Empty
    }

}
