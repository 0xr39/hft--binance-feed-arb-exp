# hft--binance-feed-arb-exp

**Fusing multiple Binance depth data streams into a single high-resolution order book feed for HFT backtesting and arbitrage research.**

---

## Overview

Binance does not offer pure tick-by-tick Level-2 data. All depth streams are conflated/aggregated:

| Stream | Granularity | Limitation |
|--------|------------|------------|
| `diffDepth` / `depthUpdate` | ~100ms or 250ms batches | Multiple individual updates aggregated per message |
| `bookTicker` | Every BBO change (~tens of ms) | Only best bid/ask, no full depth |
| `partialBookDepth` | Top N levels every 100/250ms | Limited to N levels |

To generate accurate fill simulations and realistic backtesting results, this project fuses multiple stream types into a single coherent `LocalOrderBook`.

---

## Architecture

```
                  ┌─────────────────────────────────────┐
                  │     Binance Combined-Streams WS      │
                  │  wss://fstream.binance.com/stream    │
                  └──────────┬──────────────────────────┘
                             │
                  ┌──────────▼──────────────────────────┐
                  │       StreamReceiver                 │
                  │  • Multiplexed WS connection          │
                  │  • Exponential backoff reconnection   │
                  │  • Dry-run mode for diagnostics       │
                  │  • 16+ unit tests                     │
                  └──────────┬──────────────────────────┘
                             │  BookUpdate
                  ┌──────────▼──────────────────────────┐
                  │       LocalOrderBook                 │
                  │  • BTreeMap<u64, LevelMeta> storage  │
                  │  • BBO cache bypass (bookTicker)     │
                  │  • Timestamp-guarded writes          │
                  │  • Per-source level provenance       │
                  │  • Batched timing log                │
                  └─────────────────────────────────────┘
                             ▲
                  ┌──────────┴──────────────────────────┐
                  │  Background Snapshot Refresh (REST)  │
                  │  • GET /fapi/v1/depth every 30s      │
                  │  • try_lock — never blocks WS loop   │
                  └─────────────────────────────────────┘
```

### StreamReceiver

A single WebSocket connection subscribes to **all configured streams** via Binance's combined-streams endpoint. This guarantees true chronological ordering and avoids ordering ambiguity from per-stream channels.

**Subscribed stream types:**
- `@bookTicker` — real-time BBO (fires on every quote change)
- `@depth@<speed>ms` — incremental diff book depth
- `@depth<levels>@<speed>ms` — partial snapshot of top N levels

### LocalOrderBook

Prices are encoded as integer ticks (`round(price / tick_size)`) → zero-indexed `BTreeMap<u64, LevelMeta>`.

- **Bids** sorted descending (highest first) via `iter().rev()`
- **Asks** sorted ascending (lowest first) natively
- **BBO cache** bypasses tree lookups for `bookTicker` updates — the hottest path
- **Timestamp-guarded writes** per level — stale updates from slower streams are silently dropped
- Each level tracks its originating `StreamSource` + timestamps for per-source analysis

### Snapshot Refresh

A background task fetches `GET /fapi/v1/depth?symbol=BTCUSDT&limit=1000` every 30s and silently corrects the book via `try_lock` — never blocking the WS loop.

---

## Getting Started

### Prerequisites

- [Rust](https://www.rust-lang.org/) (edition 2021)

### Run

```bash
# Run the async stream receiver (connects to Binance futures testnet/live)
cargo run --release
```

### Run with diagnostics

Edit `main()` to call `stream_dry_run()` instead — prints raw messages without touching the book.

### Run mock demo

Edit `main()` to call `mock_book()` — runs a local update sequence to demonstrate book behaviour.

### Run tests

```bash
cargo test
```

---

## Performance

Typical `apply()` latency on live Binance streams (full steroid release build, batched timing log):

| Source | Levels | Tick to Trade Latency |
|--------|--------|---------|
| `bookTicker` | 1 bid + 1 ask | ~30-100 ns |
| `partialBookDepth` (5-20 levels) | 5 - 20 | ~2000-10000 ns |
| `diffBookDepth` | 10–100+ levels | ~30000 ns for a large update (100 bid 100 ask)|

The batched timing log avoids `eprintln!` on the hot path — records are pushed to a `Vec<TimingRecord>` in ~ns and flushed asynchronously every 10s.

---

## Release Profile

```toml
[profile.release]
opt-level = 3
lto = "fat"
codegen-units = 1
panic = "abort"
strip = true
debug = "none"
overflow-checks = false
```

---

## Project Structure

```
src/
├── main.rs       # Entry point: stream_to_book, stream_dry_run, mock_book
├── book.rs       # LocalOrderBook: storage, apply, snapshot fetch
├── stream.rs     # StreamReceiver, StreamSource, parsing, WS loop
└── util.rs       # now_nanos(), parse_levels()
```

---

## ⚠️ Open Source

This is an **open-source** repository. Before committing or pushing, ensure no secrets (API keys, tokens, credentials) are included. Use environment variables or gitignored config files for any sensitive values.

---

## References

- [hftbacktest — Fusing Depth Data tutorial](https://hftbacktest.readthedocs.io/en/latest/tutorials/Fusing%20Depth%20Data.html)
- [Binance WebSocket Streams (Futures)](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/public)
- [Binance REST Depth Endpoint](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/public-endpoint#depth)
