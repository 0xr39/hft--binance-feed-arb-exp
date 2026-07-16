---
name: snapshot-refresh
description: 'Periodic REST snapshot refresh for LocalOrderBook using Binance GET /fapi/v1/depth. Use when: designing or modifying the snapshot fetch logic, discussing periodic resync strategy, adding reqwest dependency, or changing how snapshots integrate with WebSocket streams.'
---

# Snapshot Refresh — Periodic REST Book Resync

## Motivation

The `PartialBookDepth` WebSocket stream only sends the **top N** levels (e.g. depth20). Over time, drift can accumulate between the local book and the exchange's canonical state due to:

- Missed diff events during reconnection windows
- Stale levels left behind by the lazy-filtering BBO strategy
- Gaps in depth coverage beyond the partial depth stream's N levels

A periodic REST snapshot fetch corrects all of this by replacing the book with an authoritative snapshot from Binance.

## REST Endpoint

**Futures** (used by `WS_B`/`WS_C`):
```
GET /fapi/v1/depth?symbol=BTCUSDT&limit=1000
```

**Spot** (used by `WS_A`):
```
GET /api/v3/depth?symbol=BTCUSDT&limit=1000
```

### Response format

```json
{
  "lastUpdateId": 1027024,
  "E": 1589436921000,
  "T": 1589436921000,
  "bids": [
    ["96351.4", "6.344"],
    ...
  ],
  "asks": [
    ["96351.5", "7.159"],
    ...
  ]
}
```

The `bids`/`asks` arrays use the same `[price, qty]` string-pair format as the WebSocket events — `parse_levels()` in `stream.rs` already handles this format.

### Parameters

| Param | Required | Default | Description |
|---|---|---|---|
| `symbol` | Yes | — | Trading pair, e.g. `BTCUSDT` |
| `limit` | No | 100 | Number of levels. Max 1000 for futures, 5000 for spot |

## Design

### Architecture overview

```
┌─────────────────────────────────────────────────────────┐
│  main.rs                                                 │
│                                                          │
│  1. LocalOrderBook::new()                                │
│     → empty book, wrapped in Arc<Mutex<>>                │
│                                                          │
│  2. StreamReceiver::new(book, configs, rest_url)         │
│     → takes the Arc<Mutex<book>>                         │
│                                                          │
│  3. receiver.run(on_update)                              │
│     ┌────────────────────┐   ┌────────────────────────┐  │
│     │  WS message loop   │   │  Snapshot fetch loop   │  │
│     │  (main task)       │   │  (spawned background)  │  │
│     │                    │   │                        │  │
│     │  lock().await      │   │  every 30s:            │  │
│     │  → apply WS update │   │    HTTP GET /depth     │  │
│     │  → on_update(&book)│   │    try_lock()          │  │
│     │  → unlock          │   │    → if locked: apply  │  │
│     └────────────────────┘   │    → if busy: skip     │  │
│                               └────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

### Locking strategy

`LocalOrderBook` is shared between two concurrent tasks via `Arc<Mutex<LocalOrderBook>>`:

| Task | Lock strategy | Behaviour if busy |
|---|---|---|
| **WS message loop** (main) | `lock().await` — **blocks** until acquired | Waits; WS processing is serialised anyway |
| **Snapshot fetch loop** (background) | `try_lock()` — **non-blocking** | Skips this cycle, retries next interval |

The snapshot loop never blocks the WS loop: if it can't acquire the lock immediately,
it lets go and waits for the next 30s tick. The WS loop gets priority since it carries
live market data.

### Why not lock-free inline?

An earlier design ran the snapshot fetch inline inside the WS message loop (no Mutex,
single-threaded). That blocks WS message processing for ~50–500ms during the HTTP
request — risky if a burst of messages arrives during that window. The background-task
approach decouples the concerns: the WS loop is never blocked by HTTP latency.

### Snapshot fetch on `LocalOrderBook`

`LocalOrderBook` gets a new method that applies a REST depth snapshot directly
to itself (instead of the old `from_snapshot` factory approach):

```rust
impl LocalOrderBook {
    /// Apply a REST depth snapshot, replacing all levels and resetting the
    /// BBO cache. The snapshot `BookUpdate` is built upstream by the caller
    /// from a `RestDepthSnapshot` response.
    pub fn apply_snapshot(&mut self, update: &BookUpdate) {
        assert!(update.is_snapshot);
        self.bbo_bid = None;
        self.bbo_ask = None;
        self.bids.clear();
        self.asks.clear();
        // ... insert levels from update.bids / update.asks
    }
}
```

> The initial snapshot at startup is applied the same way — the caller fetches
> REST once, locks the book, and calls `apply_snapshot()` before spawning the
> WS loop.

### Periodic refresh — background task

Inside `StreamReceiver::run()`, a background task is spawned for periodic
snapshot fetches before the WS message loop starts:

```rust
pub async fn run(&mut self, mut on_update: Box<dyn FnMut(&LocalOrderBook) + Send>) {
    // ── Spawn background snapshot loop ──────────────────────────────────
    let snapshot_book = self.book.clone();   // Arc<Mutex<LocalOrderBook>>
    let rest_url = self.rest_url.clone();
    let interval = self.snapshot_interval;
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            match fetch_depth_snapshot(&rest_url).await {
                Ok(update) => {
                    // try_lock — if WS loop holds the lock, skip this cycle.
                    if let Some(mut guard) = snapshot_book.try_lock() {
                        guard.apply_snapshot(&update);
                        // No on_update call — silent correction.
                    }
                }
                Err(e) => eprintln!("[snapshot] refresh error: {e}"),
            }
        }
    });

    // ── WS message loop ────────────────────────────────────────────────
    loop {
        // ... connect + read loop ...
        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    match parse_book_update(&text, util::now_nanos()) {
                        Ok(update) => {
                            let mut guard = self.book.lock().await;
                            guard.apply(&update);
                            on_update(&guard);
                        }
                        // ... error handling ...
                    }
                }
                // ... close/ping/error handling ...
            }
        }
    }
}
```

> The background task holds no locks during the HTTP fetch — the lock is only
> acquired for the brief `apply_snapshot` call. If the WS loop is processing a
> message at that moment, `try_lock` returns `None` and the snapshot is deferred
> to the next 30s interval.

### BBO cache interaction

`LocalOrderBook` has a separate BBO cache (`bbo_bid`/`bbo_ask`) that the
`BookTicker` stream updates directly, bypassing the BTreeMaps (see
`localbook-opt/SKILL.md`). A REST snapshot must **reset this cache** alongside
the depth trees, otherwise `best_bid()`/`best_ask()` return stale prices until
the next `BookTicker` event.

The snapshot path in `apply()` handles this:

```rust
if update.is_snapshot {
    self.bbo_bid = None;       // ← clear BBO cache
    self.bbo_ask = None;       // ← clear BBO cache
    if !update.bids.is_empty() { self.bids.clear(); }
    if !update.asks.is_empty() { self.asks.clear(); }
}
```

### Stale-diff guard (`last_snapshot_ts`)

After a snapshot clears the book, buffered diff events from before the snapshot
could corrupt the freshly-reset state. A single book-level watermark handles this:

```rust
struct LocalOrderBook {
    // ...
    /// Exchange timestamp of the most recent snapshot (nanoseconds since epoch).
    /// Used to discard stale diff events that arrive after a snapshot.
    last_snapshot_ts: i64,
}
```

**How it works:**

1. On each snapshot (`is_snapshot == true`), `apply()` sets `last_snapshot_ts = update.exch_ts`.
2. On each non-snapshot update, `apply()` checks:
   ```rust
   if !update.is_snapshot && update.exch_ts <= self.last_snapshot_ts {
       return;  // stale — discard
   }
   ```
3. This catches **all** stale events — BookTicker, DiffBookDepth, and
   PartialBookDepth alike — regardless of which price level they target.
4. The snapshot itself is never filtered (it's authoritative).

This is simpler and more robust than `lastUpdateId` sync (which requires
threading event IDs through `parse_book_update`) or relying solely on the
per-level `last_exch_ts` watermark (which can't protect against stale inserts of
new levels).

### New structs

A deserialization struct for the REST `/depth` response. The `bids`/`asks` format
matches the WebSocket events, so `parse_levels()` in `stream.rs` is reused directly:

```rust
#[derive(Deserialize)]
struct RestDepthSnapshot {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,
    #[serde(rename = "T")]
    transaction_time: i64,
    bids: Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
}
```

> **Note**: `PartialDepthEvent` (WebSocket) has the same fields minus `lastUpdateId`.
> A future refactor could merge them into a shared `DepthEvent` type, but keeping
> them separate avoids touching the WS code path.

A snapshot error type:

```rust
#[derive(Debug)]
pub enum SnapshotError {
    Http(String),
    Json(String),
}
```

On error, the snapshot is silently skipped — the book keeps its previous state and
will try again on the next interval. Errors are logged to stderr but not surfaced
through `on_update`.

### Changes to `StreamReceiver`

| Field | Type | Purpose |
|---|---|---|
| `book` | `Arc<Mutex<LocalOrderBook>>` | Shared between WS loop and snapshot background task |
| `configs` | `Vec<StreamConfig>` | Unchanged |
| `rest_url` | `String` | Built once, e.g. `https://fapi.binance.com/fapi/v1/depth` (without query params) |
| `symbol` | `String` | Trading pair, e.g. `"BTCUSDT"` |
| `snapshot_limit` | `u32` | How many levels to request from REST (default 1000) |
| `snapshot_interval` | `Duration` | How often to refresh (default 30s) |

Constructor signature:
```rust
pub fn new(
    book: Arc<Mutex<LocalOrderBook>>,
    configs: Vec<StreamConfig>,
    rest_url: String,
    symbol: impl Into<String>,
) -> Self
```

### Changes to callback

The callback receives `&LocalOrderBook` (the `MutexGuard` is deref'd by the caller):

```rust
mut on_update: Box<dyn FnMut(&LocalOrderBook) + Send>
```

The guard is held for the duration of the callback — the caller should keep
processing inside the guarded scope short.

Snapshot refreshes do NOT call `on_update` (they silently correct the book).

## Files to modify

| File | Change |
|---|---|
| `Cargo.toml` | Add `reqwest` dependency |
| `src/book.rs` | Add `apply_snapshot()` method, no structural changes needed |
| `src/stream.rs` | Wrap book in `Arc<Mutex<LocalOrderBook>>`, add `RestDepthSnapshot` struct, spawn background snapshot loop in `run()`, add `reqwest` fetch helper |

## Verification

1. `cargo build` — no new warnings, reqwest compiles cleanly
2. `cargo test` — all 26 existing tests pass (book tests are sync, no snapshot logic involved)
3. `cargo run` — WS messages processed, background snapshot fires every 30s
4. `cargo run --release` — verify release perf

## Future considerations

- **Dynamic limit**: Could expose `snapshot_limit` as a constructor parameter on `StreamReceiver`.
- **`rest_url` generation**: Currently built once with a hardcoded limit. Could be parameterised in the constructor.
