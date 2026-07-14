---
name: localbook-opt
description: 'LocalOrderBook data structure, stream fusion semantics, and speed optimization design for hft--binance-feed-arb-exp. Use when: designing or modifying the order book data layout, optimizing apply() hot path, discussing BBO caching vs BTreeMap trade-offs, adding new stream source types.'
---

# LocalOrderBook — Data Structure & Speed Optimization

## When to Use

- Modifying fields of `LocalOrderBook` or `LevelMeta`
- Changing `apply()` hot path logic
- Adding a new stream source variant to `StreamSource`
- Discussing BBO caching, BTreeMap alternatives, or allocation avoidance
- Profiling or measuring `apply()` latency

---

## Current Architecture

### Storage

```
LocalOrderBook
├── bids: BTreeMap<u64, LevelMeta>       # price in ticks -> metadata
├── asks: BTreeMap<u64, LevelMeta>       # price in ticks -> metadata
├── timing_log: RefCell<Vec<TimingRecord>>  # batched timing buffer
├── last_update_source: Option<StreamSource>
├── last_exch_ts: i64
├── last_local_ts: i64
├── update_count: u64
└── (metadata: symbol, tick_size, lot_size)

LevelMeta { qty: f64, source: StreamSource, last_exch_ts: i64, last_local_ts: i64, delay_ns: i64 }
TimingRecord { elapsed_ns: u128, bids: u32, asks: u32, source: StreamSource }
```

- **Bids sorted descending** (highest price first). `BTreeMap` iterates ascending, so `bids.iter().rev()`.
- **Asks sorted ascending** (lowest price first). `BTreeMap` iterates ascending natively.
- **Price → tick** via `(price / tick_size).round() as u64` for deterministic integer keys.

### `apply()` flow (current)

1. Record metadata: `last_update_source`, `last_exch_ts`, `last_local_ts`, `update_count++`
2. If snapshot, `clear()` the relevant side(s)
3. For each bid/ask level: `store()` closure — compute tick, check timestamp guard, then `remove` if qty==0 else `insert`
4. `self.timing_log.borrow_mut().push(TimingRecord { ... })` — pushes a cheap struct (no heap alloc, no syscall)

### Timestamp-guarded writes

When multiple conflated streams feed the same book, updates for the same price level can arrive out of order (e.g., a stale `DiffBookDepth` update arriving after a fresher `BookTicker` update). The `store()` closure guards against this:

- Before any insert or remove, it checks `exch_ts > existing.last_exch_ts`.
- If the incoming update is **older** (or equal) to what's already stored, it is **silently dropped** — the per-level `last_exch_ts` in `LevelMeta` serves as a Lamport-clock-style watermark.
- This also protects against stale `qty == 0.0` deletes that could incorrectly remove a level re-added by a later update.
- Snapshots bypass per-level timestamps (the side is cleared first, so there is no existing entry to compare against).

### Stream sources & frequencies

| Source | Frequency | Levels per update | Usage |
|---|---|---|---|
| `BookTicker` | Every BBO change (~tens of ms) | 1 bid + 1 ask | Fast BBO |
| `DiffBookDepth` | Every 100ms or 250ms | Variable (N changed levels) | Full book diffs |
| `PartialBookDepth` | Every 100ms or 250ms | Top N levels (snapshot) | Full book snapshots |

### Profiling baseline (debug mode, live Binance streams)

```
[apply]   1250 ns  |  1 bids, 1 asks  |  source=book_ticker
[apply]   1167 ns  |  1 bids, 1 asks  |  source=book_ticker
[apply]    958 ns  |  1 bids, 1 asks  |  source=book_ticker
[apply]   1000 ns  |  1 bids, 1 asks  |  source=book_ticker
[apply]   1042 ns  |  1 bids, 1 asks  |  source=book_ticker
[apply]   1500 ns  |  1 bids, 1 asks  |  source=book_ticker
[apply]   1375 ns  |  1 bids, 1 asks  |  source=book_ticker
[apply]  32583 ns  |  46 bids, 24 asks  |  source=diff_book_depth
[apply]   7875 ns  |  20 bids, 20 asks  |  source=partial_book_depth
[apply]  18417 ns  |  18 bids, 10 asks  |  source=diff_book_depth
[apply]   3334 ns  |  20 bids, 20 asks  |  source=partial_book_depth
[apply]   4625 ns  |  18 bids, 15 asks  |  source=diff_book_depth
[apply]   3625 ns  |  20 bids, 20 asks  |  source=partial_book_depth
[apply]   5375 ns  |  18 bids, 12 asks  |  source=diff_book_depth
```

Note: `eprintln!` has been removed from the hot path — these timings reflect pure book logic + `Vec::push` for the timing buffer. The 171 µs outlier from the old per-call `eprintln!` approach is gone. The current bottleneck is BTreeMap operations on depth updates (up to ~33 µs for 70 levels).

---

## Batched Timing Log (applied 2026-07-15)

### Motivation

The original `eprintln!` at the end of every `apply()` call caused:
- **Lock contention**: acquiring `stderr`'s mutex (~50-500 ns depending on contention)
- **Syscall overhead**: `write()` to stderr (~50-200 ns)
- **Format allocation**: heap allocation for the formatted string

For hot paths like `book_ticker` (333-500 ns total), the `eprintln!` alone could **double or triple** latency.

### Design

Replace the per-call `eprintln!` with an in-memory buffer that accumulates timing records and flushes periodically:

```
LocalOrderBook
└── timing_log: RefCell<Vec<TimingRecord>>
```

- `TimingRecord` is a small `Copy` struct (elapsed_ns, bids, asks, source)
- `RefCell` enables interior mutability — the stream callback (which receives `&LocalOrderBook`) can flush the buffer without `&mut` access
- Initial capacity is 256 elements, so after startup there are zero heap allocations per push

### `flush_timing_log(&self)`

- Drains all buffered records
- Builds one big `String` (single heap allocation for the batch)
- Calls `eprintln!` once — one lock acquisition, one write syscall
- Called every **5 seconds** from `stream_to_book`'s callback in `main.rs`

### Cost per `apply()` call

Just a `Vec::push` (amortized O(1), no allocation after warmup) + the `Instant::now()` timing call. No lock, no syscall, no format alloc.

### Expected improvement

| Update type | Before (per-call eprintln) | After (batched) |
|---|---|---|
| book_ticker (1b+1a) | 333-500 ns | ~100-200 ns |
| depth update (55 levels) | 28-171 µs | ~5-10 µs (pure tree work) |

The 171 µs outlier was dominated by stderr lock contention — batching eliminates that entirely.

---

## Proposed Optimization: BBO Cache

### Rationale

`bookTicker` is the **hottest** stream (every BBO tick) but only touches **2 price levels**. Paying `BTreeMap::insert` (O(log n) + potential heap alloc) on every tick is wasteful. The depth streams (`diff`/`partial`) update the full book at lower frequency — that's where BTreeMap cost is justified.

### Design

**Separate BBO cache from the BTreeMap:**

```
LocalOrderBook
├── bbo_bid: Option<PriceLevel>        # NEW — cached best bid from bookTicker
├── bbo_ask: Option<PriceLevel>        # NEW — cached best ask from bookTicker
├── bids: BTreeMap<u64, LevelMeta>     # depth-stream levels only
├── asks: BTreeMap<u64, LevelMeta>     # depth-stream levels only
└── ... (rest unchanged)
```

### `apply()` logic (proposed)

```
source == BookTicker:
  └─ update bbo_bid / bbo_ask fields only
  └─ update metadata (timestamps, update_count)
  └─ return — NO BTreeMap interaction

source == DiffBookDepth | PartialBookDepth:
  └─ update BTreeMaps as before (full depth)
  └─ update metadata
  └─ BBO cache refresh: DEFERRED (solve later)
```

### Accessor semantics (proposed)

| Method | Reads from | Notes |
|---|---|---|
| `best_bid()` | `bbo_bid` (fallback: `bids.last_key_value()`) | O(1) in common case |
| `best_ask()` | `bbo_ask` (fallback: `asks.first_key_value()`) | O(1) in common case |
| `bids()` iterator | `bids` BTreeMap only | Depth-stream view |
| `asks()` iterator | `asks` BTreeMap only | Depth-stream view |
| `source_at_price(price)` | Check BBO cache first, fallback to tree | Handle both storage locations |
| `bid_depth()` / `ask_depth()` | `bids.len()` / `asks.len()` | Unchanged |
| `clear()` | Also reset `bbo_bid` / `bbo_ask` to `None` | |

### Display (proposed)

```
── BTCUSDT ──
  BBO bid: 96351.4 @ 6.878  (book_ticker)
  BBO ask: 96351.5 @ 0.178  (book_ticker)
  Bids (4) | Asks (2)
  ...
```

Shows BBO cache values (with source) at the top, then full depth from tree below.

### Trade-offs

| Concern | Mitigation |
|---|---|
| Depth removes a level that BBO still shows | bookTicker fires again within ~tens of ms to correct — acceptable staleness window |
| Depth iterators miss BBO levels | Iterators return depth-stream view; BBO always accessible via `best_bid()`/`best_ask()` |
| `source_at_price()` needs awareness of both locations | Check `bbo_bid`/`bbo_ask` prices first, then tree |

### Expected improvement

bookTicker `apply()` drops from ~542 ns → **~20-50 ns** (a few field assignments, no BTreeMap calls, no allocation).

---

## Future Optimization Ideas

### Parallelize bids and asks with `rayon::join`

`self.bids` and `self.asks` are completely independent `BTreeMap`s — no aliasing, no data race. They can be processed concurrently to cut wall time for large updates roughly in half:

```rust
use rayon::join;

// Inside apply(), after snapshot handling:
join(
    || { for bid in &update.bids { store(&mut self.bids, bid, ...); } },
    || { for ask in &update.asks { store(&mut self.asks, ask, ...); } },
);
```

**Overhead:** `rayon::join` costs ~0.5–2 µs for work-stealing coordination.

| Update type | Sequential | Parallel (est.) | Worth it? |
|---|---|---|---|
| book_ticker (1b+1a, ~500 ns) | 500 ns | ~2.5 µs (worse!) | ❌ No — overhead dominates |
| depth update (70 levels, ~33 µs) | 33 µs | ~18 µs | ✅ Yes — ~45% faster |

**Recommendation:** Only parallelize when `update.bids.len() + update.asks.len()` exceeds some threshold (e.g. >4 levels total). Or gate it behind a branch: small updates stay sequential, large diffs take the parallel path.

### Inline the `store` closure as a standalone `fn`

The `store` closure is currently defined **inside** `apply()` — it's re-created on every call with fresh captures. Lifting it to a standalone `fn` (or a method on `LocalOrderBook`) eliminates the closure capture overhead and gives the compiler better inlining visibility:

```rust
// Current: closure created per apply() call
let store = |map: &mut BTreeMap<u64, LevelMeta>, level: &PriceLevel, ...| { ... };

// Proposed: standalone method
fn store_level(
    map: &mut BTreeMap<u64, LevelMeta>,
    level: &PriceLevel,
    source: StreamSource,
    exch_ts: i64,
    local_ts: i64,
    delay_ns: i64,
    tick_size: f64,
) { ... }
```

The compiler may already be inlining this in release mode, but in debug builds the closure overhead adds measurable cost. Making it a `fn` is a zero-risk change with no downsides.

### Offload timing log to a background thread

The `RefCell<Vec<TimingRecord>>` push at the end of every `apply()` is cheap but not free — it contends with the hot path for cache and has a (small) allocation cost. Options:

- **`mpsc::Sender` + background consumer thread** — push timing records into a channel; a dedicated thread drains them and batches the `eprintln!`. Removes all timing overhead from the hot path at the cost of one atomic write per `apply()`.
- **Per-core sharded counters** — use a sharded atomics approach (like `metrics` crate) to avoid any shared-memory contention. Overkill unless the book is shared across threads.
- **Simple flag** — if not profiling, skip the timing log entirely at compile time via `cfg!(feature = "profiling")`. Zero cost when disabled.

**Trade-off:** Adding a channel send (~20–50 ns) for every `apply()` to save a `Vec::push` (~5–15 ns) only makes sense if the consumer thread does expensive I/O that would otherwise block the hot path. The current batched approach (flush every 5s) already keeps per-call cost minimal.

### Slot-map for levels (pre-allocated)

If the book stabilizes at ~100-200 levels per side, a slot-map or `Vec<(u64, LevelMeta)>` with binary search could outperform BTreeMap by avoiding pointer chasing and allocation. Only worth exploring if BTreeMap becomes a measured bottleneck in release mode.

### Refine `PriceLevel` representation

`f64` for price → consider storing price directly as `u64` ticks throughout the public API to avoid repeated `price / tick_size` conversions. This is an API-level change affecting `BookUpdate`, `PriceLevel`, and all callers.

---

## Verification

After any optimization:
1. `cargo build` — no new warnings
2. `cargo test` — all 26 tests pass
3. `cargo run` — timing output shows expected improvement
4. `cargo run --release` — verify optimized perf (debug builds exaggerate BTreeMap overhead due to no inlining)
