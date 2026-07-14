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
├── bids: BTreeMap<u64, LevelMeta>    # price in ticks -> metadata
├── asks: BTreeMap<u64, LevelMeta>    # price in ticks -> metadata
├── last_update_source: Option<StreamSource>
├── last_exch_ts: i64
├── last_local_ts: i64
├── update_count: u64
└── (metadata: symbol, tick_size, lot_size)

LevelMeta { qty: f64, source: StreamSource, last_exch_ts: i64 }
```

- **Bids sorted descending** (highest price first). `BTreeMap` iterates ascending, so `bids.iter().rev()`.
- **Asks sorted ascending** (lowest price first). `BTreeMap` iterates ascending natively.
- **Price → tick** via `(price / tick_size).round() as u64` for deterministic integer keys.

### `apply()` flow (current)

1. Record metadata: `last_update_source`, `last_exch_ts`, `last_local_ts`, `update_count++`
2. If snapshot, `clear()` the relevant side(s)
3. For each bid/ask level: `store()` closure — compute tick, `remove` if qty==0, else `insert`
4. `eprintln!` timing

### Stream sources & frequencies

| Source | Frequency | Levels per update | Usage |
|---|---|---|---|
| `BookTicker` | Every BBO change (~tens of ms) | 1 bid + 1 ask | Fast BBO |
| `DiffBookDepth` | Every 100ms or 250ms | Variable (N changed levels) | Full book diffs |
| `PartialBookDepth` | Every 100ms or 250ms | Top N levels (snapshot) | Full book snapshots |

### Profiling baseline (debug mode, mock 3-update scenario)

```
[apply]  22458 ns  |  3 bids, 3 asks  |  source=partial_book_depth   # first call: cold cache + alloc
[apply]    542 ns  |  1 bids, 1 asks  |  source=book_ticker           # hot cache, update existing
[apply]   1708 ns  |  2 bids, 1 asks  |  source=diff_book_depth       # mix of insert/remove
```

First call overhead: cold instruction/data cache + `BTreeMap` root node heap allocation.

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
