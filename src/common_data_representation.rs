//! `common_data_representation` module is responsible for transforming the raw data from the various
//! exchange in data classes that are standardized for all exchanges.
//!
//! also contains `mqtt` for sending the messages in the MQTT pub/sub which later is used by grafana
//! to visualize data.

pub mod memory_storage;
pub mod message;
pub mod mqtt;
