use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::book::PriceLevel;

/// Wall-clock now in nanoseconds since the Unix epoch.
pub fn now_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_nanos() as i64
}

/// Convert `[price_str, qty_str]` pairs into `PriceLevel`s.
///
/// Used by both WebSocket parse helpers and REST snapshot fetch.
pub fn parse_levels(pairs: &[[String; 2]]) -> Vec<PriceLevel> {
    pairs
        .iter()
        .map(|[p, q]| PriceLevel::new(
            p.parse().unwrap_or(0.0),
            q.parse().unwrap_or(0.0),
        ))
        .collect()
}
