mod book;
mod stream;

use std::time::Instant;

use stream::{StreamConfig, StreamReceiver};

#[tokio::main]
async fn main() {
    println!("=== hft--binance-feed-arb-exp  |  Async Stream Receiver ===\n");

    // Configure all four streams for a single combined WebSocket connection.
    let configs = vec![
        StreamConfig::book_ticker("BTCUSDT"),
        StreamConfig::partial_depth("BTCUSDT", 20, 100),
        StreamConfig::partial_depth("BTCUSDT", 20, 250),
        StreamConfig::diff_depth("BTCUSDT", 100),
    ];

    let mut receiver = StreamReceiver::new("BTCUSDT", 0.1, 0.001, configs);

    // Print book state every 5 seconds.
    let mut last_print = Instant::now();

    receiver
        .run(Box::new(move |book| {
            if last_print.elapsed().as_secs() >= 5 {
                println!("{book}");
                last_print = Instant::now();
            }
        }))
        .await;
}

fn mock_book() {
    // ── Create a book for BTCUSDT ──────────────────────────────────────
    let mut book = book::LocalOrderBook::new("BTCUSDT", 0.1, 0.001);

    // ── 1. Initial snapshot from Partial Book Depth Stream ─────────────
    println!("[1] Snapshot  (source: partial_book_depth)");
    book.apply(&book::BookUpdate {
        source: stream::StreamSource::PartialBookDepth,
        exch_ts: 1746057600003000000,
        local_ts: 1746057600003500000,
        bids: vec![
            book::PriceLevel { price: 96351.4, qty: 6.344 },
            book::PriceLevel { price: 96350.0, qty: 2.100 },
            book::PriceLevel { price: 96348.2, qty: 0.500 },
        ],
        asks: vec![
            book::PriceLevel { price: 96351.5, qty: 7.159 },
            book::PriceLevel { price: 96352.0, qty: 3.200 },
            book::PriceLevel { price: 96355.0, qty: 1.000 },
        ],
        is_snapshot: true,
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
        local_ts: 1746057600300500000,
        bids: vec![book::PriceLevel { price: 96351.4, qty: 6.878 }],
        asks: vec![book::PriceLevel { price: 96351.5, qty: 0.178 }],
        is_snapshot: false,
    });
    println!("  Best bid qty: {}", book.best_bid().unwrap().qty);
    println!("  Source at best bid: {:?}", book.source_at_price(96351.4));
    println!();

    // ── 3. Diff. Book Depth stream — new levels appear ────────────────
    println!("[3] Diff book depth (source: diff_book_depth) — add/remove levels");
    book.apply(&book::BookUpdate {
        source: stream::StreamSource::DiffBookDepth,
        exch_ts: 1746057600600000000,
        local_ts: 1746057600600500000,
        bids: vec![
            book::PriceLevel { price: 96351.4, qty: 5.001 },  // updated qty
            book::PriceLevel { price: 96349.0, qty: 1.200 },  // new level
        ],
        asks: vec![
            book::PriceLevel { price: 96355.0, qty: 0.0 },    // removed (qty=0)
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