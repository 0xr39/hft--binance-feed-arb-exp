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



/// Errors that can occur during HTTP-based data fetches.
#[derive(Debug)]
pub enum HttpError {
    /// HTTP request failure.
    Request(String),
    /// HTTP response parsing failure.
    Parse(String),
}

impl std::fmt::Display for HttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Request(msg) => write!(f, "HTTP error: {msg}"),
            Self::Parse(msg) => write!(f, "HTTP parse error: {msg}"),
        }
    }
}

impl std::error::Error for HttpError {}

impl From<reqwest::Error> for HttpError {
    fn from(e: reqwest::Error) -> Self {
        Self::Request(e.to_string())
    }
}

impl From<serde_json::Error> for HttpError {
    fn from(e: serde_json::Error) -> Self {
        Self::Parse(e.to_string())
    }
}

