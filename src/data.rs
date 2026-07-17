//! `data` module is responsible for transforming the raw data from the various
//! exchange in data classes that are standardized for all exchanges.
//!
//! also contains `transception` for sending the messages in the MQTT pub/sub which later is used by grafana
//! to visualize data.

pub mod storage;
pub mod transception;
