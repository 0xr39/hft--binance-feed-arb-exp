use std::cell::RefCell;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::time::Instant;

pub use crate::stream::StreamSource;

// ---------------------------------------------------------------------------
// Price level
// ---------------------------------------------------------------------------

/// A single price level in the order book.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PriceLevel {
    pub price: f64,
    pub qty: f64,
}

impl PriceLevel {
    /// Create a new level with only price/qty.
    pub fn new(price: f64, qty: f64) -> Self {
        Self { price, qty }
    }
}

// ---------------------------------------------------------------------------
// Book update event
// ---------------------------------------------------------------------------

/// An incoming order-book update, tagged with its origin stream.
#[derive(Debug, Clone)]
pub struct BookUpdate {
    /// Which stream produced this update.
    pub source: StreamSource,
    /// Exchange timestamp (nanoseconds since epoch).
    pub exch_ts: i64,
    /// Local receive timestamp (nanoseconds since epoch).
    pub local_ts: i64,
    /// Bid levels to apply (empty = no bids in this update).
    pub bids: Vec<PriceLevel>,
    /// Ask levels to apply (empty = no asks in this update).
    pub asks: Vec<PriceLevel>,
    /// If `true`, `bids`/`asks` replace the entire book (snapshot).
    /// If `false`, they are incremental diffs.
    pub is_snapshot: bool,
}

// ---------------------------------------------------------------------------
// Local order book
// ---------------------------------------------------------------------------

/// Maintains a local replica of the Binance order book by consuming updates
/// from multiple conflated streams and tracking which stream each level came
/// from.
///
/// # Design
///
/// Bids are sorted **descending** (highest price first).
/// Asks are sorted **ascending** (lowest price first).
/// Quantities of `0.0` signal that a level should be removed (per Binance
/// diff-book semantics).
#[derive(Debug, Clone)]
pub struct LocalOrderBook {
    /// Trading pair, e.g. `"BTCUSDT"`.
    pub symbol: String,
    /// Tick size (minimum price increment).
    pub tick_size: f64,
    /// Lot size (minimum quantity increment).
    pub lot_size: f64,

    // --- Book state -------------------------------------------------------
    bids: BTreeMap<u64, LevelMeta>,   // price in ticks -> metadata
    asks: BTreeMap<u64, LevelMeta>,   // price in ticks -> metadata

    // --- Profiling (batched timing log) -----------------------------------
    timing_log: RefCell<Vec<TimingRecord>>,

    // --- Stream provenance tracking ---------------------------------------
    /// Updated on every `apply()`.
    last_update_source: Option<StreamSource>,
    /// The exchange timestamp of the most recent update.
    last_exch_ts: i64,
    /// The local timestamp of the most recent update.
    last_local_ts: i64,
    /// Total number of updates applied since creation.
    update_count: u64,
}

/// A single timing measurement captured on each `apply()` call.
#[derive(Debug, Clone, Copy)]
pub struct TimingRecord {
    pub elapsed_ns: u128,
    pub bids: u32,
    pub asks: u32,
    pub source: StreamSource,
}

/// Per-level metadata, tacked onto the quantity stored in the book.
#[derive(Debug, Clone, Copy)]
struct LevelMeta {
    qty: f64,
    /// Which stream last touched this level.
    source: StreamSource,
    /// Exchange timestamp of the last update to this level.
    last_exch_ts: i64,
    /// Local receive timestamp of the last update to this level.
    last_local_ts: i64,
    /// Network delay at the time of apply: now - exch_ts (ns).
    delay_ns: i64,
}

impl LocalOrderBook {
    /// Create a new empty order book for `symbol`.
    pub fn new(symbol: impl Into<String>, tick_size: f64, lot_size: f64) -> Self {
        Self {
            symbol: symbol.into(),
            tick_size,
            lot_size,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            timing_log: RefCell::new(Vec::with_capacity(256)),
            last_update_source: None,
            last_exch_ts: 0,
            last_local_ts: 0,
            update_count: 0,
        }
    }

    // -----------------------------------------------------------------------
    // Public accessors
    // -----------------------------------------------------------------------

    pub fn best_bid(&self) -> Option<PriceLevel> {
        self.bids.last_key_value().map(|(tick, m)| PriceLevel {
            price: *tick as f64 * self.tick_size,
            qty: m.qty,
        })
    }

    pub fn best_ask(&self) -> Option<PriceLevel> {
        self.asks.first_key_value().map(|(tick, m)| PriceLevel {
            price: *tick as f64 * self.tick_size,
            qty: m.qty,
        })
    }

    pub fn bids(&self) -> impl Iterator<Item = PriceLevel> + '_ {
        self.bids.iter().rev().map(|(tick, m)| PriceLevel {
            price: *tick as f64 * self.tick_size,
            qty: m.qty,
        })
    }

    pub fn asks(&self) -> impl Iterator<Item = PriceLevel> + '_ {
        self.asks.iter().map(|(tick, m)| PriceLevel {
            price: *tick as f64 * self.tick_size,
            qty: m.qty,
        })
    }

    /// The stream source of the most recently applied update.
    pub fn last_source(&self) -> Option<StreamSource> {
        self.last_update_source
    }

    /// Total number of updates applied since the book was created.
    pub fn update_count(&self) -> u64 {
        self.update_count
    }

    /// Query which stream last updated a specific price level.
    pub fn source_at_price(&self, price: f64) -> Option<StreamSource> {
        let tick = (price / self.tick_size).round() as u64;
        self.bids
            .get(&tick)
            .or_else(|| self.asks.get(&tick))
            .map(|m| m.source)
    }

    // -----------------------------------------------------------------------
    // Apply an update
    // -----------------------------------------------------------------------

    /// Apply a `BookUpdate` to the local book.
    ///
    /// * **Snapshot** – clears the relevant side(s) and inserts the provided
    ///   levels. Use for initial snapshots or periodic resyncs.
    /// * **Incremental (diff)** – upserts bid/ask levels. A level with
    ///   `qty == 0.0` is removed (Binance diff-book convention).
    pub fn apply(&mut self, update: &BookUpdate) {
        let start = Instant::now();

        self.last_update_source = Some(update.source);
        self.last_exch_ts = update.exch_ts;
        self.last_local_ts = update.local_ts;
        self.update_count += 1;

        // Wall-clock "now" for delay computation.
        // let now_ns = crate::util::now_nanos();
        let delay_ns = update.local_ts - update.exch_ts;

        let store = |map: &mut BTreeMap<u64, LevelMeta>,
                     level: &PriceLevel,
                     source: StreamSource,
                     exch_ts: i64,
                     local_ts: i64,
                     dly_ns: i64| {
            let tick = (level.price / self.tick_size).round() as u64;
            // Single O(log n) lookup via Entry API — checks staleness and
            // inserts/removes in one tree traversal.
            match map.entry(tick) {
                Entry::Vacant(entry) => {
                    if level.qty != 0.0 {
                        entry.insert(LevelMeta {
                            qty: level.qty,
                            source,
                            last_exch_ts: exch_ts,
                            last_local_ts: local_ts,
                            delay_ns: dly_ns,
                        });
                    }
                }
                Entry::Occupied(mut entry) => {
                    if exch_ts > entry.get().last_exch_ts {
                        if level.qty == 0.0 {
                            entry.remove();
                        } else {
                            entry.insert(LevelMeta {
                                qty: level.qty,
                                source,
                                last_exch_ts: exch_ts,
                                last_local_ts: local_ts,
                                delay_ns: dly_ns,
                            });
                        }
                    }
                }
            }
        };

        if update.is_snapshot {
            // Replace entire side(s).
            if !update.bids.is_empty() {
                self.bids.clear();
            }
            if !update.asks.is_empty() {
                self.asks.clear();
            }
        }

        for bid in &update.bids {
            store(&mut self.bids, bid, update.source, update.exch_ts, update.local_ts, delay_ns);
        }
        for ask in &update.asks {
            store(&mut self.asks, ask, update.source, update.exch_ts, update.local_ts, delay_ns);
        }

        self.timing_log.borrow_mut().push(TimingRecord {
            elapsed_ns: start.elapsed().as_nanos(),
            bids: update.bids.len() as u32,
            asks: update.asks.len() as u32,
            source: update.source,
        });
    }

    // -----------------------------------------------------------------------
    // Utility
    // -----------------------------------------------------------------------

    /// Number of bid levels currently tracked.
    pub fn bid_depth(&self) -> usize {
        self.bids.len()
    }

    /// Number of ask levels currently tracked.
    pub fn ask_depth(&self) -> usize {
        self.asks.len()
    }

    /// Clear the entire book (e.g. before a reconnect + re-snapshot).
    pub fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.last_update_source = None;
        self.timing_log.borrow_mut().clear();
    }

    /// Drain all buffered timing records and print them to stderr.
    /// Call this periodically (e.g. every 5 seconds) from the stream callback.
    pub fn flush_timing_log(&self) {
        let mut log = self.timing_log.borrow_mut();
        if log.is_empty() {
            return;
        }
        let mut buf = String::with_capacity(log.len() * 64);
        for rec in log.iter() {
            use std::fmt::Write;
            let _ = write!(
                buf,
                "[apply] {:>6} ns  |  {} bids, {} asks  |  source={}\n",
                rec.elapsed_ns, rec.bids, rec.asks, rec.source,
            );
        }
        eprintln!("{buf}");
        log.clear();
    }
}

// ---------------------------------------------------------------------------
// Display
// ---------------------------------------------------------------------------

impl LocalOrderBook {
    /// Write all ask levels with delay info to a formatter.
    fn write_asks(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (_tick, meta) in self.asks.iter() {
            let price = *_tick as f64 * self.tick_size;
            writeln!(f, "  {:.1} @ {:.1}  delay={}µs", meta.qty, price, meta.delay_ns / 1000)?;
        }
        Ok(())
    }

    /// Write all bid levels with delay info to a formatter.
    fn write_bids(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (_tick, meta) in self.bids.iter().rev() {
            let price = *_tick as f64 * self.tick_size;
            writeln!(f, "  {:.1} @ {:.1}  delay={}µs", meta.qty, price, meta.delay_ns / 1000)?;
        }
        Ok(())
    }
}

impl std::fmt::Display for LocalOrderBook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "── {} ──", self.symbol)?;
        writeln!(
            f,
            "  Asks ({}) | Bids ({})",
            self.ask_depth(),
            self.bid_depth()
        )?;
        if let Some(src) = self.last_source() {
            writeln!(f, "  Last update: {src} ({})", self.update_count())?;
        }
        // All asks (printed first)
        self.write_asks(f)?;
        writeln!(f, "  ───────")?;
        // All bids
        self.write_bids(f)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_replaces_book() {
        let mut book = LocalOrderBook::new("BTCUSDT", 0.1, 0.001);

        // Apply a snapshot
        let snap = BookUpdate {
            source: StreamSource::PartialBookDepth,
            exch_ts: 1_000_000,
            local_ts: 1_000_001,
            bids: vec![PriceLevel::new(100.0, 1.0)],
            asks: vec![PriceLevel::new(101.0, 2.0)],
            is_snapshot: true,
        };
        book.apply(&snap);
        assert_eq!(book.best_bid().unwrap().price, 100.0);
        assert_eq!(book.best_ask().unwrap().price, 101.0);

        // A second snapshot replaces only the bid side.
        let snap2 = BookUpdate {
            source: StreamSource::DiffBookDepth,
            exch_ts: 2_000_000,
            local_ts: 2_000_001,
            bids: vec![PriceLevel::new(99.0, 3.0)],
            asks: vec![PriceLevel::new(102.0, 1.0)],
            is_snapshot: true,
        };
        book.apply(&snap2);
        assert_eq!(book.best_bid().unwrap().price, 99.0);
        assert_eq!(book.best_ask().unwrap().price, 102.0);
        // Old levels gone
        assert_eq!(book.bid_depth(), 1);
        assert_eq!(book.ask_depth(), 1);
    }

    #[test]
    fn diff_upserts_and_removes() {
        let mut book = LocalOrderBook::new("ETHUSDT", 0.01, 0.001);

        // Seed with a snapshot
        book.apply(&BookUpdate {
            source: StreamSource::PartialBookDepth,
            exch_ts: 1,
            local_ts: 1,
            bids: vec![PriceLevel::new(2000.0, 10.0)],
            asks: vec![PriceLevel::new(2001.0, 5.0)],
            is_snapshot: true,
        });

        // Diff: update bid qty, remove ask, add a new ask level
        book.apply(&BookUpdate {
            source: StreamSource::DiffBookDepth,
            exch_ts: 2,
            local_ts: 2,
            bids: vec![PriceLevel::new(2000.0, 15.0)],
            asks: vec![
                PriceLevel::new(2001.0, 0.0),  // remove
                PriceLevel::new(2002.0, 3.0),  // add
            ],
            is_snapshot: false,
        });

        assert_eq!(book.best_bid().unwrap().qty, 15.0);
        assert_eq!(book.best_ask().unwrap().price, 2002.0);
    }

    #[test]
    fn tracks_stream_source_per_level() {
        let mut book = LocalOrderBook::new("BTCUSDT", 0.1, 0.001);

        book.apply(&BookUpdate {
            source: StreamSource::BookTicker,
            exch_ts: 1,
            local_ts: 1,
            bids: vec![PriceLevel::new(100.0, 1.0)],
            asks: vec![],
            is_snapshot: true,
        });

        assert_eq!(book.source_at_price(100.0), Some(StreamSource::BookTicker));
        assert_eq!(book.last_source(), Some(StreamSource::BookTicker));
    }
}
