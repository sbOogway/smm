use std::str::FromStr;

use bigdecimal::BigDecimal;
use rust_decimal::Decimal;

pub fn big_decimal_to_decimal(value: BigDecimal) -> Decimal {
    Decimal::from_str(&value.to_string()).unwrap()
}
