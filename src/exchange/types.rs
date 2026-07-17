pub mod message;
pub mod portfolio;

#[derive(Debug, Clone)]
pub enum Side {
    Long,
    Short,
}
