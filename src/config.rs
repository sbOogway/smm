//! `config` module is responsible for parsing the configuration, in order to modify the behaviour
//! of the system at runtime, without the need for recompilation.

use rust_decimal::Decimal;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub runtime: RuntimeConfig,
    pub exchange: ExchangeConfigs,
    pub strategy: StrategyConfigs,
    pub disruptor: DisruptorConfig,
    pub mqtt: MqttConfig,
    pub memory_storage: MemoryStorageConfig,
}

#[derive(Debug, Deserialize)]
pub struct RuntimeConfig {
    pub exchanges: Vec<String>,
    pub strategy: String,
}

#[derive(Debug, Deserialize)]
pub struct ExchangeConfigs {
    pub hyperliquid: Option<HyperliquidConfig>,
    pub dydx: Option<DydxConfig>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct HyperliquidConfig {
    pub coins: Vec<String>,
    pub mainnet: bool,
    pub address: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct DydxConfig {
    pub tickers: Vec<String>,
    pub indexer_ws_endpoint: String,
    pub mnemonic: String,
    pub subaccount_number: u32,
    pub chain_id: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct MemoryStorageConfig {
    pub backend: String,
    pub redis: Option<RedisConfig>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct RedisConfig {
    pub socket_path: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct MqttConfig {
    pub enabled: bool,
    pub broker: String,
    pub port: u16,
    pub topic_prefix: String,
    pub client_id: String,
}

#[derive(Debug, Deserialize)]
pub struct DisruptorConfig {
    pub buffer_size: usize,
}

#[derive(Debug, Deserialize)]
pub struct StrategyConfigs {
    pub avellaneda_stoikov_market_making: Option<AvellanedaStoikovConfig>,
}

#[derive(Debug, Deserialize)]
pub struct AvellanedaStoikovConfig {
    pub γ: Decimal,
    pub κ: Decimal,
    pub σ: Decimal,
}

impl AppConfig {
    pub fn load() -> Result<Self, config::ConfigError> {
        config::Config::builder()
            .add_source(config::File::with_name("config/default"))
            .add_source(config::File::with_name("config/local").required(false))
            .add_source(config::Environment::with_prefix("MMA"))
            .build()?
            .try_deserialize()
    }
}
