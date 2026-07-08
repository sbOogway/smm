pub mod price_update;

pub use price_update::PriceUpdate;

pub enum Message {
    Empty,
    PriceUpdate(PriceUpdate),
}

impl Message {
    pub fn empty() -> Self {
        Self::Empty
    }

    pub fn handle(&self, seq: i64, batch: bool) {
        match self {
            Self::Empty => {}
            Self::PriceUpdate(update) => update.handle(seq, batch),
        }
    }
}
