# Repository Guidelines

## Project Structure & Modules
- `src/main.rs`: Server entrypoint plus RESP parsing, command dispatch, and tests. All new protocol handlers should live here until the code grows enough to split into modules.
- `Cargo.toml`: Crate metadata and dependencies. Keep Tokio features minimal; prefer `"full"` only when required.
- `README.md`: Quickstart, protocol examples, and current scope (PING/ECHO/QUIT). Update when behavior changes.

## Build, Test, and Development
- `cargo run`: Start the Redis-compatible TCP server (binds to `127.0.0.1:6379` unless `REDUST_ADDR` is set).
- `REDUST_ADDR="0.0.0.0:6380" cargo run`: Override listen address/port.
- `cargo test`: Run unit/integration/perf smoke tests; expected to finish quickly locally.
- `cargo fmt && cargo clippy -- -D warnings`: Format and lint; required before pushing.

## Coding Style & Naming
- Rust 2021 edition; prefer small, focused functions and explicit match arms over hidden fallthrough.
- Use snake_case for functions/modules, PascalCase for types/enums, SCREAMING_SNAKE_CASE for consts/env keys (e.g., `REDUST_ADDR`).
- Keep logs concise and structured (`[conn]`, `[resp]` prefixes). Avoid println! spam on hot paths unless debugging.

## Testing Guidelines
- Tests live in `src/main.rs` under `#[cfg(test)]`; mirror the pattern when adding new commands.
- Name tests for behavior (`responds_to_basic_commands`, `performance_ping_round_trips`); include timing assertions when perf-sensitive.
- Prefer Tokio `#[tokio::test]` for async flows. When adding latency checks, bound runtime with `Duration` and keep iteration counts modest.

## Commit & Pull Requests
- Commits: short imperative subject (`Add RESP parser`, `Tighten ping perf guard`). Group related changes; avoid mixing refactors with behavior changes.
- Pull requests: include what changed, why, and how to verify (commands run, expected responses). Link issues when applicable and note protocol surface changes (new commands or breaking responses).
- Add minimal repro snippets for bug fixes (e.g., RESP arrays triggering the bug) and updated README snippets if user-facing behavior shifts.

## Security & Config Notes
- Network binding is controlled via `REDUST_ADDR`; default is loopback. Avoid committing test harnesses that open `0.0.0.0` without mention.
- No persistence layer exists; avoid implying data durability. If adding stateful features, document storage paths and failure modes in README/PR notes.
