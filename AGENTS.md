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

## Testing
- `cargo test` to run all tests
- Tests use `#[tokio::test]`
- Tests are in a `#[cfg(test)] mod tests` block at the bottom of the file

## Build & check
- `cargo check` to verify compilation
- `cargo test` to run tests
- `cargo build --release` for release builds

## Architecture
- `exchange::DataProvider` trait: single `listen` method per exchange (one WebSocket)
- Messages flow through a `disruptor` (lock-free ring buffer)
- Each exchange is a module under `src/exchange/`
- Strategy spawns one task per exchange calling `exchange.listen()`
- New subscription types: add to `SubscriptionKind` list + `match` arm in `listen`
