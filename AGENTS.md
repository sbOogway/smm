# Conventions for AI agents

## Commit style
- Only commit when explicitly prompted by the user
- Conventional commits: `type: description` (lowercase)
- Types: `feat`, `refactor`, `test`, `chore`, `fix`, `docs`
- Include a detailed body describing what was changed and why

## Code style
- No comments in production code
- Use `tracing::info!` / `tracing::warn!` for logging, never `println`
- No emojis in code
- Follow existing patterns for imports, struct layout, trait impls
- all lowercase everywhere: doc comments, commit messages, code identifiers, everything

## Build, test & check
- `cargo check` — verify compilation
- `cargo test` — run all tests (including `tests/check_deps.rs` which checks for circular dependencies)
- `cargo tt` — runs tests with `-- --nocapture` (alias in `.cargo/config.toml`)
- `cargo build --release` — release build
- Integration test `tests/check_deps.rs` requires `cargo modules` and `tred` (graphviz); runs on `--lib` target
- MQTT tests in `common_data_representation::mqtt::tests` are skipped unless broker is at `localhost:1883` (start via `docker compose up -d`)
- Hyperliquid integration tests (`connect_and_receive_trades`, `ping_latency_under_500ms`) need network access

## Config loading
- `config/default.toml` is always loaded
- `config/local.toml` is loaded if present (for local overrides, gitignored)
- Env vars with `MMA_` prefix override both
- Runtime strategy selected by `runtime.strategy` string field

## Architecture

### Data flow
```
exchange::DataProvider.listen() → disruptor (lock-free ring buffer) → strategy::handle_message() → mqtt (optional)
```

### Key traits (in `exchange.rs`)
- `DataProvider` — single `listen` method returning a `Pin<Box<dyn Future>>`, publishes `Message` into the disruptor
- `Executor` — `send_order` / `cancel_order` (todos on Hyperliquid)
- `Infos` — `name()` / `symbols()`
- `Exchange: DataProvider + Executor + Send + Sync + Infos` (auto-implemented for Hyperliquid)
- Factory: `exchange::new(name, cfg)` matches on string, panics on unknown
- Currently only `hyperliquid` is implemented

### Strategy (`strategy.rs`)
- `Strategy` trait with `new(cfg)` and `async fn run(&self)`
- Uses static singletons (`OnceLock`, `LazyLock`) for state, global disruptor producer, and exchange list
- `run` spawns one tokio task per exchange calling `exchange.listen()`, then hangs on `future::pending()`
- Disruptor is built with `pin_at_core(1)` and `Sleep` (1 ms polling)

### `Message` enum (in `common_data_representation::message`)
- Variants: `Empty`, `TradeUpdate`, `BboUpdate`, `AsmmQuote`
- Adding a new message type requires: variant in `Message` enum, handler arm in `handle_message`, and a match arm in `MqttPublisher::run` if it should be published

### Adding a new exchange
1. Create `src/exchange/<name>.rs` with a struct implementing `DataProvider` + `Executor` + `Infos` + `Exchange`
2. Add a `pub mod <name>` to `src/exchange.rs`
3. Register in `exchange::new()` match
4. Add exchange config to `config.rs` under `ExchangeConfigs`

### Adding a new strategy
1. Create `src/strategy/<name>.rs` implementing `Strategy`
2. Add `pub mod <name>` to `src/strategy.rs`
3. Add config to `config.rs` under `StrategyConfigs`
4. Register in `main.rs` match on `cfg.runtime.strategy`

### Infrastructure (Docker)
- `docker compose up -d` starts mosquitto (MQTT broker) and grafana (with MQTT datasource plugin)
- MQTT is optional; disabled by default (`mqtt.enabled = false`)

## Notes
- Rust edition 2024; crate has `#![allow(mixed_script_confusables)]`
- Uses `rust_decimal` throughout (never `f64` for prices)
- `UnsafeCell` + manual Sync impl is used for strategy state (justified by single-threaded disruptor consumer)
- Developer: mattia, on Fedora, uses `podman` (alias `docker=podman` or substitute `podman` for `docker` in commands)
