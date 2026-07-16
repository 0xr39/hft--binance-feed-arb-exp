mod book;
mod stream;
mod util;

use std::sync::Arc;
use std::time::Instant;

use tokio::sync::Mutex;
use stream::{StreamConfig, StreamReceiver};

const SYMBOL: &str = "BTCUSDT";

async fn stream_to_book() {
    println!("=== hft--binance-feed-arb-exp  |  Async Stream Receiver ===\n");

    // Create a shared order book, initialised from a REST snapshot.
    let rest_base = stream::urls::REST_FAPI;
    let snapshot_url = format!("{rest_base}?symbol={SYMBOL}&limit=1000");
    let book = Arc::new(Mutex::new(
        match book::LocalOrderBook::from_snapshot(SYMBOL, 0.1, 0.001, &snapshot_url).await {
            Ok(b) => {
                println!("[snapshot] Initial snapshot applied");
                b
            }
            Err(e) => {
                eprintln!("[snapshot] Initial fetch failed: {e} — starting empty");
                book::LocalOrderBook::new(SYMBOL, 0.1, 0.001)
            }
        },
    ));
    println!("{:.10}", book.lock().await);

    // Configure streams. 100ms and 250ms just wont be accepted together, but different depth level can coexist
    let streams_configs = vec![
        StreamConfig::book_ticker(SYMBOL),
        StreamConfig::partial_depth(SYMBOL, 5, 100),
        StreamConfig::partial_depth(SYMBOL, 10, 100),
        StreamConfig::partial_depth(SYMBOL, 20, 100),
        // StreamConfig::partial_depth(SYMBOL, 20, 250),
        StreamConfig::diff_depth(SYMBOL, 100),
        // StreamConfig::diff_depth(SYMBOL, 250),
    ];

    let mut receiver: StreamReceiver = StreamReceiver::new(
        book,
        streams_configs,
        rest_base.to_string(),
        SYMBOL,
    );

    // Print book state every 5s, flush timing log every 5s.
    let mut last_print = Instant::now();
    let mut last_flush = Instant::now();

    receiver
        .run(Box::new(move |book| {
            if last_flush.elapsed().as_secs() >= 5 {
                book.flush_timing_log();
                last_flush = Instant::now();
            }
            if last_print.elapsed().as_secs() >= 5 {
                println!("{:.10}", book);
                last_print = Instant::now();
            }
        }))
        .await;
}

async fn stream_dry_run() {
    println!("=== hft--binance-feed-arb-exp  |  Async Stream Receiver (dry-run) ===\n");

    let book = Arc::new(Mutex::new(book::LocalOrderBook::new(SYMBOL, 0.1, 0.001)));

    let configs = vec![
        StreamConfig::book_ticker(SYMBOL),
        StreamConfig::partial_depth(SYMBOL, 5, 100),
        StreamConfig::partial_depth(SYMBOL, 10, 100),
        StreamConfig::partial_depth(SYMBOL, 20, 100),
        StreamConfig::diff_depth(SYMBOL, 100),
    ];

    let receiver = StreamReceiver::new(
        book,
        configs,
        stream::urls::REST_FAPI.to_string(),
        SYMBOL,
    );

    receiver.dry_run().await;
}

fn mock_book() {
    // ── Create a book for BTCUSDT ──────────────────────────────────────
    let mut book = book::LocalOrderBook::new(SYMBOL, 0.01, 0.001);

    // ── 1. Initial snapshot from Partial Book Depth Stream ─────────────
    println!("[1] Snapshot  (source: partial_book_depth)");
    book.apply(&book::BookUpdate {
        source: stream::StreamSource::PartialBookDepth { levels: 20, speed_ms: 100 },
        exch_ts: 1746057600003000000,
        local_ts: util::now_nanos(),
        bids: vec![
            book::PriceLevel::new(96351.4, 6.344),
            book::PriceLevel::new(96350.0, 2.100),
            book::PriceLevel::new(96348.2, 0.500),
        ],
        asks: vec![
            book::PriceLevel::new(96351.5, 7.159),
            book::PriceLevel::new(96352.0, 3.200),
            book::PriceLevel::new(96355.0, 1.000),
        ],
        is_snapshot: false,
    });
    println!("  Best bid: {:?}", book.best_bid());
    println!("  Best ask: {:?}", book.best_ask());
    println!("  Source at best bid: {:?}", book.source_at_price(96351.4));
    println!();

    // ── 2. bookTicker update (more frequent BBO) ──────────────────────
    println!("[2] bookTicker (source: book_ticker) — qty change on best bid");
    book.apply(&book::BookUpdate {
        source: stream::StreamSource::BookTicker,
        exch_ts: 1746057600300000000,
        local_ts: util::now_nanos(),
        bids: vec![book::PriceLevel::new(96351.4, 6.878)],
        asks: vec![book::PriceLevel::new(96351.5, 0.178)],
        is_snapshot: false,
    });
    println!("  Best bid qty: {}", book.best_bid().unwrap().qty);
    println!("  Source at best bid: {:?}", book.source_at_price(96351.4));
    println!();

    // ── 3. Diff. Book Depth stream — new levels appear ────────────────
    println!("[3] Diff book depth (source: diff_book_depth) — add/remove levels");
    book.apply(&book::BookUpdate {
        source: stream::StreamSource::DiffBookDepth { speed_ms: 100 },
        exch_ts: 1746057600600000000,
        local_ts: util::now_nanos(),
        bids: vec![
            book::PriceLevel::new(96351.4, 5.001),  // updated qty
            book::PriceLevel::new(96349.0, 1.200),  // new level
        ],
        asks: vec![
            book::PriceLevel::new(96355.0, 0.0),    // removed (qty=0)
        ],
        is_snapshot: false,
    });
    println!("  Book now:");

    println!("{book}");

    // ── Summary ────────────────────────────────────────────────────────
    println!("  Update count : {}", book.update_count());
    println!("  Last source  : {:?}", book.last_source());
    println!("  Bid depth    : {}", book.bid_depth());
    println!("  Ask depth    : {}", book.ask_depth());
}

#[tokio::main]
async fn main() {
    // Run the async stream receiver and print book state every 5 seconds.
    stream_to_book().await;

    // Run the async stream receiver in dry-run mode (print messages only).
    // stream_dry_run().await;

    // Run a mock book update sequence to demonstrate book behavior.
    // mock_book();
}