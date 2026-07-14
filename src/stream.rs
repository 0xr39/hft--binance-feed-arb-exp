use crate::util;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use serde::Deserialize;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use crate::book::{BookUpdate, LocalOrderBook, PriceLevel};

// ---------------------------------------------------------------------------
// Stream source identification
// ---------------------------------------------------------------------------

/// Identifies which Binance WebSocket stream produced an update.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StreamSource {
    /// ## Partial Book Depth Stream (`<symbol>@depth<levels>@<speed>ms`)
    /// e.g. `btcusdt@depth20@100ms`
    /// Periodic snapshots of the top N price levels.
    PartialBookDepth,
    /// ## Individual Symbol Book Ticker Stream (`<symbol>@bookTicker`)
    /// Real-time BBO (best bid/ask) — fires on every quote change.
    BookTicker,
    /// ## Diff. Book Depth Stream (`<symbol>@depth@<speed>ms`)
    /// Incremental delta updates: which price levels changed and how.
    DiffBookDepth,
    /// Placeholder for newly discovered streams added later.
    Other(&'static str),
}

// Cosmetic
impl std::fmt::Display for StreamSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PartialBookDepth => write!(f, "partial_book_depth"),
            Self::BookTicker => write!(f, "book_ticker"),
            Self::DiffBookDepth => write!(f, "diff_book_depth"),
            Self::Other(name) => write!(f, "other({name})"),
        }
    }
}

// ---------------------------------------------------------------------------
// Stream config
// ---------------------------------------------------------------------------

/// Configuration for a single Binance WebSocket stream subscription.
#[derive(Debug, Clone)]
pub struct StreamConfig {
    /// Which stream type to subscribe to.
    pub stream_type: StreamSource,
    /// Trading pair symbol, e.g. `"BTCUSDT"`.
    pub symbol: String,
    /// Number of levels for partial depth streams (e.g. `Some(20)` for `depth20`).
    /// `None` for non-partial-depth streams.
    pub levels: Option<u32>,
    /// Update speed in milliseconds (e.g. `Some(100)` or `Some(250)`).
    /// `None` for book ticker (which has no speed param).
    pub speed_ms: Option<u32>,
}

impl StreamConfig {
    /// Build the Binance stream name for combined URLs, e.g. `btcusdt@bookTicker`.
    fn stream_name(&self) -> String {
        let symbol = self.symbol.to_lowercase();
        match self.stream_type {
            StreamSource::BookTicker => format!("{symbol}@bookTicker"),
            StreamSource::DiffBookDepth => {
                let speed = self.speed_ms.unwrap_or(100);
                format!("{symbol}@depth@{speed}ms")
            }
            StreamSource::PartialBookDepth => {
                let levels = self.levels.unwrap_or(20);
                let speed = self.speed_ms.unwrap_or(100);
                format!("{symbol}@depth{levels}@{speed}ms")
            }
            StreamSource::Other(name) => name.to_string(),
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers: partial depth config builders
// ---------------------------------------------------------------------------

impl StreamConfig {
    /// Build a `StreamConfig` for `@bookTicker`.
    pub fn book_ticker(symbol: impl Into<String>) -> Self {
        Self {
            stream_type: StreamSource::BookTicker,
            symbol: symbol.into(),
            levels: None,
            speed_ms: None,
        }
    }

    /// Build a `StreamConfig` for `@depth<levels>@<speed>ms` (partial snapshot).
    pub fn partial_depth(symbol: impl Into<String>, levels: u32, speed_ms: u32) -> Self {
        Self {
            stream_type: StreamSource::PartialBookDepth,
            symbol: symbol.into(),
            levels: Some(levels),
            speed_ms: Some(speed_ms),
        }
    }

    /// Build a `StreamConfig` for `@depth@<speed>ms` (diff book depth).
    pub fn diff_depth(symbol: impl Into<String>, speed_ms: u32) -> Self {
        Self {
            stream_type: StreamSource::DiffBookDepth,
            symbol: symbol.into(),
            levels: None,
            speed_ms: Some(speed_ms),
        }
    }
}

// ---------------------------------------------------------------------------
// Binance JSON event types
// ---------------------------------------------------------------------------

/// Top-level combined-stream payload.
/// Binance wraps multiple streams into one connection with this envelope.
#[derive(Deserialize)]
struct CombinedPayload {
    stream: String,
    data: serde_json::Value,
}

/// Diff. Book Depth event (`depthUpdate`).
#[derive(Deserialize)]
struct DiffDepthEvent {
    #[serde(rename = "E")]
    event_time: i64,
    #[serde(rename = "T")]
    transaction_time: i64,
    #[serde(rename = "b")]
    bids: Vec<[String; 2]>,
    #[serde(rename = "a")]
    asks: Vec<[String; 2]>,
}

/// Individual Symbol Book Ticker event.
#[derive(Deserialize)]
struct BookTickerEvent {
    #[serde(rename = "b")]
    best_bid_price: String,
    #[serde(rename = "B")]
    best_bid_qty: String,
    #[serde(rename = "a")]
    best_ask_price: String,
    #[serde(rename = "A")]
    best_ask_qty: String,
    /// Event time (milliseconds). Present on futures, may be absent on spot.
    #[serde(rename = "E")]
    event_time: Option<i64>,
    #[serde(rename = "T")]
    transaction_time: i64,
}

/// Partial Book Depth snapshot event.
#[derive(Deserialize)]
struct PartialDepthEvent {
    #[serde(rename = "E")]
    event_time: i64,
    #[serde(rename = "T")]
    transaction_time: i64,
    #[serde(rename = "b")]
    bids: Vec<[String; 2]>,
    #[serde(rename = "a")]
    asks: Vec<[String; 2]>,
}

// ---------------------------------------------------------------------------
// Stream error
// ---------------------------------------------------------------------------

/// Errors that can occur during stream processing.
#[derive(Debug)]
pub enum StreamError {
    /// WebSocket connection failure.
    WsConnect(String),
    /// WebSocket read failure.
    WsRead(String),
    /// JSON parse failure.
    JsonParse(String),
    /// Unknown or unsupported stream name.
    UnknownStream(String),
}

impl std::fmt::Display for StreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WsConnect(msg) => write!(f, "WebSocket connect error: {msg}"),
            Self::WsRead(msg) => write!(f, "WebSocket read error: {msg}"),
            Self::JsonParse(msg) => write!(f, "JSON parse error: {msg}"),
            Self::UnknownStream(name) => write!(f, "unknown stream: {name}"),
        }
    }
}

impl std::error::Error for StreamError {}

impl From<tokio_tungstenite::tungstenite::Error> for StreamError {
    fn from(e: tokio_tungstenite::tungstenite::Error) -> Self {
        Self::WsRead(e.to_string())
    }
}

impl From<serde_json::Error> for StreamError {
    fn from(e: serde_json::Error) -> Self {
        Self::JsonParse(e.to_string())
    }
}

// ---------------------------------------------------------------------------
// Parse helpers
// ---------------------------------------------------------------------------

/// Convert `[price_str, qty_str]` pairs into `PriceLevel`s.
fn parse_levels(pairs: &[[String; 2]]) -> Vec<PriceLevel> {
    pairs
        .iter()
        .map(|[p, q]| PriceLevel::new(
            p.parse().unwrap_or(0.0),
            q.parse().unwrap_or(0.0),
        ))
        .collect()
}

/// Identify the stream source from a Binance combined-stream name.
///
/// The order of checks matters:
/// 1. `@bookTicker` → BookTicker
/// 2. `@depth@` → DiffBookDepth  (e.g. `btcusdt@depth@100ms`)
/// 3. `@depth` + digits → PartialBookDepth (e.g. `btcusdt@depth20@100ms`)
fn identify_source(stream_name: &str) -> Option<StreamSource> {
    if stream_name.contains("@bookTicker") {
        Some(StreamSource::BookTicker)
    } else if stream_name.contains("@depth@") {
        Some(StreamSource::DiffBookDepth)
    } else if stream_name.contains("@depth") {
        Some(StreamSource::PartialBookDepth)
    } else {
        None
    }
}

/// Parse a combined-stream JSON text into a `BookUpdate`.
fn parse_book_update(text: &str, local_ts: i64) -> Result<BookUpdate, StreamError> {
    let payload: CombinedPayload = serde_json::from_str(text)?;
    let source = identify_source(&payload.stream)
        .ok_or_else(|| StreamError::UnknownStream(payload.stream.clone()))?;

    match source {
        StreamSource::BookTicker => {
            let ev: BookTickerEvent = serde_json::from_value(payload.data)?;
            let exch_ts = ev.transaction_time * 1_000_000;
            let bid_price: f64 = ev.best_bid_price.parse().unwrap_or(0.0);
            let bid_qty: f64 = ev.best_bid_qty.parse().unwrap_or(0.0);
            let ask_price: f64 = ev.best_ask_price.parse().unwrap_or(0.0);
            let ask_qty: f64 = ev.best_ask_qty.parse().unwrap_or(0.0);
            Ok(BookUpdate {
                source,
                exch_ts,
                local_ts,
                bids: vec![PriceLevel::new(bid_price, bid_qty)],
                asks: vec![PriceLevel::new(ask_price, ask_qty)],
                is_snapshot: false,
            })
        }
        StreamSource::DiffBookDepth => {
            let ev: DiffDepthEvent = serde_json::from_value(payload.data)?;
            Ok(BookUpdate {
                source,
                exch_ts: ev.transaction_time * 1_000_000,
                local_ts,
                bids: parse_levels(&ev.bids),
                asks: parse_levels(&ev.asks),
                is_snapshot: false,
            })
        }
        StreamSource::PartialBookDepth => {
            let ev: PartialDepthEvent = serde_json::from_value(payload.data)?;
            Ok(BookUpdate {
                source,
                exch_ts: ev.transaction_time * 1_000_000,
                local_ts,
                bids: parse_levels(&ev.bids),
                asks: parse_levels(&ev.asks),
                is_snapshot: false,
            })
        }
        StreamSource::Other(_) => Err(StreamError::UnknownStream(payload.stream)),
    }
}

/// 3 version of endpoint
/// wss://stream.binance.com:9443/stream
/// wss://fstream.binance.com/public/stream
/// wss://stream.binancefuture.com/public/stream
/// This multiplexed
pub mod urls {
    pub const WS_A: &str = "wss://stream.binance.com:9443/stream";
    pub const WS_B: &str = "wss://fstream.binance.com/public/stream";
    pub const WS_C: &str = "wss://stream.binancefuture.com/public/stream";
}

// ---------------------------------------------------------------------------
// Stream receiver
// ---------------------------------------------------------------------------

/// Maintains a multiplexed WebSocket connection to Binance combined streams,
/// parses updates into `BookUpdate`s, and applies them to an internal
/// [`LocalOrderBook`].
///
/// # Design
///
/// A single WebSocket connection subscribes to **all configured streams**
/// via Binance's combined-streams endpoint
/// approach minimises connection overhead and keeps messages arriving in
/// true chronological order, which avoids ordering ambiguity that can occur
/// with per-stream channels.
///
/// On disconnect, the receiver automatically reconnects with exponential
/// backoff.
pub struct StreamReceiver {
    /// Internal order book being maintained.
    book: LocalOrderBook,
    /// Stream subscription configurations.
    configs: Vec<StreamConfig>,
    /// Total reconnection attempts (used for backoff).
    reconnect_attempt: u64,
}

impl StreamReceiver {
    /// Create a new `StreamReceiver` for the given symbol and stream configs.
    ///
    /// `tick_size` and `lot_size` are forwarded to [`LocalOrderBook::new`].
    pub fn new(
        symbol: impl Into<String>,
        tick_size: f64,
        lot_size: f64,
        configs: Vec<StreamConfig>,
    ) -> Self {
        Self {
            book: LocalOrderBook::new(symbol, tick_size, lot_size),
            configs,
            reconnect_attempt: 0,
        }
    }

    // -----------------------------------------------------------------------
    // Public accessors
    // -----------------------------------------------------------------------

    /// Immutable reference to the internal order book.
    pub fn book(&self) -> &LocalOrderBook {
        &self.book
    }

    /// Mutable reference to the internal order book.
    pub fn book_mut(&mut self) -> &mut LocalOrderBook {
        &mut self.book
    }

    // -----------------------------------------------------------------------
    // URL construction
    // -----------------------------------------------------------------------

    /// Build the combined-streams WebSocket URL from all configs.
    fn build_ws_url(&self) -> String {
        let stream_names: Vec<String> =
            self.configs.iter().map(|c| c.stream_name()).collect();
        format!(
            "{}?streams={}",
            urls::WS_B,
            stream_names.join("/")
        )
    }

    // -----------------------------------------------------------------------
    // Dry-run: print received messages without touching the book
    // -----------------------------------------------------------------------

    /// Connect to the Binance streams and print every raw message without
    /// parsing or updating the order book. Reconnects with exponential backoff.
    pub async fn dry_run(&self) {
        // Install the default rustls CryptoProvider (ring) so TLS setup
        // doesn't panic. If already installed, this is a no-op.
        let _ = rustls::crypto::ring::default_provider().install_default();

        loop {
            let url = self.build_ws_url();
            eprintln!("[dry-run] Connecting to {url}");

            let (ws, _) = match connect_async(&url).await {
                Ok(conn) => conn,
                Err(e) => {
                    eprintln!("[dry-run] Connect error: {e} — retrying in 5s");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };

            let (_, mut read) = ws.split();

            while let Some(msg) = read.next().await {
                match msg {
                    Ok(Message::Text(text)) => {
                        match parse_book_update(&text, util::now_nanos()) {
                            Ok(update) => {
                                // println!("[dry-run] {text}");
                                let bid_str = if update.bids.is_empty() {
                                    "no bids".into()
                                } else {
                                    format!("Bid: {} levels", update.bids.len())
                                };
                                let ask_str = if update.asks.is_empty() {
                                    "no asks".into()
                                } else {
                                    format!("Ask: {} levels", update.asks.len())
                                };
                                let kind = if update.is_snapshot { "snapshot" } else { "diff" };
                                eprintln!(
                                    "[dry-run] {}  {}  {}  {} delay: {} ms",
                                    update.source, kind, bid_str, ask_str, (update.exch_ts - update.local_ts)/1_000_000 
                                );
                                // eprintln!("[dry-run] exch_ts: {}, local_ts: {}", update.exch_ts, update.local_ts);
                            }
                            Err(StreamError::UnknownStream(_)) => {}
                            Err(e) => {
                                eprintln!("[dry-run] Parse error: {e}");
                            }
                        }
                    }
                    Ok(Message::Close(frame)) => {
                        eprintln!("[dry-run] Connection closed: {frame:?}");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        break;
                    }
                    Ok(Message::Ping(_)) => {}
                    Err(e) => {
                        eprintln!("[dry-run] Read error: {e}");
                        break;
                    }
                    _ => {}
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Persistent run with reconnection
    // -----------------------------------------------------------------------

    /// Run the stream receiver indefinitely, connecting to the Binance
    /// combined-streams endpoint and reconnecting on failure with exponential
    /// backoff capped at 30 seconds.
    ///
    /// The `on_update` callback is invoked after every successfully parsed
    /// and applied book update.
    pub async fn run(&mut self, mut on_update: Box<dyn FnMut(&LocalOrderBook) + Send>) {
        let _ = rustls::crypto::ring::default_provider().install_default();
        loop {
            let url = self.build_ws_url();
            eprintln!("[stream] Connecting to {url}");

            // ── Connect ────────────────────────────────────────────────────
            let (ws, _) = match connect_async(&url).await {
                Ok(conn) => conn,
                Err(e) => {
                    self.reconnect_attempt += 1;
                    let delay = Duration::from_secs(
                        std::cmp::min(self.reconnect_attempt * 2, 30),
                    );
                    eprintln!(
                        "[stream] Connect error: {e} — reconnecting in {delay:?}"
                    );
                    tokio::time::sleep(delay).await;
                    continue;
                }
            };

            let (_, mut read) = ws.split();
            let mut clean_close = false;

            // ── Message loop ───────────────────────────────────────────────
            while let Some(msg) = read.next().await {
                match msg {
                    Ok(Message::Text(text)) => {
                        match parse_book_update(&text, util::now_nanos()) {
                            Ok(update) => {
                                self.book.apply(&update);
                                on_update(&self.book);
                            }
                            Err(StreamError::UnknownStream(_)) => {
                                // Silently skip unrecognised stream payloads
                            }
                            Err(e) => {
                                eprintln!("[stream] Parse error: {e}");
                            }
                        }
                    }
                    Ok(Message::Close(frame)) => {
                        eprintln!("[stream] Connection closed: {frame:?}");
                        clean_close = true;
                        break;
                    }
                    Ok(Message::Ping(_)) => {
                        // tungstenite responds to pings automatically
                    }
                    Err(e) => {
                        eprintln!("[stream] Read error: {e}");
                        break;
                    }
                    _ => {}
                }
            }

            // ── Reconnect logic ────────────────────────────────────────────
            if clean_close {
                self.reconnect_attempt = 0;
                tokio::time::sleep(Duration::from_millis(100)).await;
            } else {
                self.reconnect_attempt += 1;
                let delay = Duration::from_secs(
                    std::cmp::min(self.reconnect_attempt * 2, 30),
                );
                eprintln!("[stream] Reconnecting in {delay:?}");
                tokio::time::sleep(delay).await;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identify_book_ticker() {
        assert_eq!(
            identify_source("btcusdt@bookTicker"),
            Some(StreamSource::BookTicker),
        );
    }

    #[test]
    fn identify_diff_depth() {
        assert_eq!(
            identify_source("btcusdt@depth@100ms"),
            Some(StreamSource::DiffBookDepth),
        );
    }

    #[test]
    fn identify_partial_depth() {
        assert_eq!(
            identify_source("btcusdt@depth20@100ms"),
            Some(StreamSource::PartialBookDepth),
        );
        assert_eq!(
            identify_source("btcusdt@depth20@250ms"),
            Some(StreamSource::PartialBookDepth),
        );
    }

    #[test]
    fn unknown_stream_returns_none() {
        assert_eq!(identify_source("btcusdt@trade"), None);
    }

    #[test]
    fn display_partial_book_depth() {
        assert_eq!(
            StreamSource::PartialBookDepth.to_string(),
            "partial_book_depth",
        );
    }

    #[test]
    fn display_book_ticker() {
        assert_eq!(StreamSource::BookTicker.to_string(), "book_ticker");
    }

    #[test]
    fn display_diff_book_depth() {
        assert_eq!(StreamSource::DiffBookDepth.to_string(), "diff_book_depth");
    }

    #[test]
    fn config_stream_name_book_ticker() {
        let cfg = StreamConfig::book_ticker("BTCUSDT");
        assert_eq!(cfg.stream_name(), "btcusdt@bookTicker");
    }

    #[test]
    fn config_stream_name_partial_depth() {
        let cfg = StreamConfig::partial_depth("ETHUSDT", 20, 100);
        assert_eq!(cfg.stream_name(), "ethusdt@depth20@100ms");
    }

    #[test]
    fn config_stream_name_partial_depth_250() {
        let cfg = StreamConfig::partial_depth("BTCUSDT", 20, 250);
        assert_eq!(cfg.stream_name(), "btcusdt@depth20@250ms");
    }

    #[test]
    fn config_stream_name_diff_depth() {
        let cfg = StreamConfig::diff_depth("BTCUSDT", 100);
        assert_eq!(cfg.stream_name(), "btcusdt@depth@100ms");
    }

    #[test]
    fn parse_book_ticker_update() {
        let json = r#"{
            "stream": "btcusdt@bookTicker",
            "data": {
                "u": 400900217,
                "T": 1746057600000,
                "s": "BTCUSDT",
                "b": "96351.4",
                "B": "6.344",
                "a": "96351.5",
                "A": "7.159"
            }
        }"#;

        let update = parse_book_update(json, 0).unwrap();
        assert_eq!(update.source, StreamSource::BookTicker);
        assert!(!update.is_snapshot);
        assert_eq!(update.bids.len(), 1);
        assert_eq!(update.bids[0].price, 96351.4);
        assert_eq!(update.bids[0].qty, 6.344);
        assert_eq!(update.asks.len(), 1);
        assert_eq!(update.asks[0].price, 96351.5);
        assert_eq!(update.asks[0].qty, 7.159);
    }

    #[test]
    fn parse_diff_depth_update() {
        let json = r#"{
            "stream": "btcusdt@depth@100ms",
            "data": {
                "e": "depthUpdate",
                "E": 1746057600000,
                "T": 1746057600000,
                "s": "BTCUSDT",
                "U": 157,
                "u": 170,
                "b": [["96351.4","5.001"]],
                "a": [["96355.0","0.000"]]
            }
        }"#;

        let update = parse_book_update(json, 0).unwrap();
        assert_eq!(update.source, StreamSource::DiffBookDepth);
        assert!(!update.is_snapshot);
        assert_eq!(update.bids.len(), 1);
        assert_eq!(update.bids[0].price, 96351.4);
        assert_eq!(update.bids[0].qty, 5.001);
        assert_eq!(update.asks.len(), 1);
        assert_eq!(update.asks[0].price, 96355.0);
        assert_eq!(update.asks[0].qty, 0.0);
    }

    #[test]
    fn parse_partial_depth_update() {
        let json = r#"{
            "stream": "btcusdt@depth20@100ms",
            "data": {
                "e": "depthUpdate",
                "E": 1746057600000,
                "T": 1746057600000,
                "s": "BTCUSDT",
                "U": 157,
                "u": 170,
                "b": [["96351.4","5.001"]],
                "a": [["96355.0","0.000"]]
            }
        }"#;

        let update = parse_book_update(json, 0).unwrap();
        assert_eq!(update.source, StreamSource::PartialBookDepth);
        assert_eq!(update.bids.len(), 1);
        assert_eq!(update.asks.len(), 1);
    }

    #[test]
    fn parse_unknown_stream_errors() {
        let json = r#"{
            "stream": "btcusdt@trade",
            "data": {}
        }"#;
        assert!(parse_book_update(json, 0).is_err());
    }

    #[test]
    fn parse_invalid_json_errors() {
        assert!(parse_book_update("not json", 0).is_err());
    }

    #[test]
    fn stream_config_builders() {
        let bt = StreamConfig::book_ticker("BTCUSDT");
        assert_eq!(bt.stream_name(), "btcusdt@bookTicker");

        let pd = StreamConfig::partial_depth("ETHUSDT", 20, 100);
        assert_eq!(pd.stream_name(), "ethusdt@depth20@100ms");

        let dd = StreamConfig::diff_depth("BTCUSDT", 100);
        assert_eq!(dd.stream_name(), "btcusdt@depth@100ms");
    }

    #[test]
    fn parse_levels_empty() {
        let levels: Vec<[String; 2]> = vec![];
        let result = parse_levels(&levels);
        assert!(result.is_empty());
    }

    #[test]
    fn parse_levels_with_data() {
        let levels = vec![
            ["100.5".into(), "2.5".into()],
            ["200.0".into(), "0.0".into()],
        ];
        let result = parse_levels(&levels);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].price, 100.5);
        assert_eq!(result[0].qty, 2.5);
        assert_eq!(result[1].price, 200.0);
        assert_eq!(result[1].qty, 0.0);
    }

    #[test]
    fn parse_levels_bad_number_defaults_to_zero() {
        let levels = vec![["not_a_number".into(), "5.0".into()]];
        let result = parse_levels(&levels);
        assert_eq!(result[0].price, 0.0);
    }

    #[test]
    fn display_stream_source() {
        assert_eq!(format!("{}", StreamSource::PartialBookDepth), "partial_book_depth");
        assert_eq!(format!("{}", StreamSource::BookTicker), "book_ticker");
        assert_eq!(format!("{}", StreamSource::DiffBookDepth), "diff_book_depth");
        assert_eq!(format!("{}", StreamSource::Other("custom")), "other(custom)");
    }

    #[test]
    fn stream_error_display() {
        let err = StreamError::WsConnect("refused".into());
        assert_eq!(err.to_string(), "WebSocket connect error: refused");

        let err = StreamError::UnknownStream("test".into());
        assert_eq!(err.to_string(), "unknown stream: test");
    }

    #[test]
    fn stream_config_other() {
        let cfg = StreamConfig {
            stream_type: StreamSource::Other("custom_stream"),
            symbol: "BTCUSDT".into(),
            levels: None,
            speed_ms: None,
        };
        assert_eq!(cfg.stream_name(), "custom_stream");
    }
}
