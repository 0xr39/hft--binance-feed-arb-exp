mod book;

use book::{
    BookUpdate,
    LocalOrderBook,
    PriceLevel,
    StreamSource,
};

fn main() {
    println!("=== hft--binance-feed-arb-exp  |  Local Order Book Prototype ===\n");

    // ── Create a book for BTCUSDT ──────────────────────────────────────
    let mut book = LocalOrderBook::new("BTCUSDT", 0.1, 0.001);

    // ── 1. Initial snapshot from Partial Book Depth Stream ─────────────
    println!("[1] Snapshot  (source: partial_book_depth)");
    book.apply(&BookUpdate {
        source: StreamSource::PartialBookDepth,
        exch_ts: 1746057600003000000,
        local_ts: 1746057600003500000,
        bids: vec![
            PriceLevel { price: 96351.4, qty: 6.344 },
            PriceLevel { price: 96350.0, qty: 2.100 },
            PriceLevel { price: 96348.2, qty: 0.500 },
        ],
        asks: vec![
            PriceLevel { price: 96351.5, qty: 7.159 },
            PriceLevel { price: 96352.0, qty: 3.200 },
            PriceLevel { price: 96355.0, qty: 1.000 },
        ],
        is_snapshot: true,
    });
    println!("  Best bid: {:?}", book.best_bid());
    println!("  Best ask: {:?}", book.best_ask());
    println!("  Source at best bid: {:?}", book.source_at_price(96351.4));
    println!();

    // ── 2. bookTicker update (more frequent BBO) ──────────────────────
    println!("[2] bookTicker (source: book_ticker) — qty change on best bid");
    book.apply(&BookUpdate {
        source: StreamSource::BookTicker,
        exch_ts: 1746057600300000000,
        local_ts: 1746057600300500000,
        bids: vec![PriceLevel { price: 96351.4, qty: 6.878 }],
        asks: vec![PriceLevel { price: 96351.5, qty: 0.178 }],
        is_snapshot: false,
    });
    println!("  Best bid qty: {}", book.best_bid().unwrap().qty);
    println!("  Source at best bid: {:?}", book.source_at_price(96351.4));
    println!();

    // ── 3. Diff. Book Depth stream — new levels appear ────────────────
    println!("[3] Diff book depth (source: diff_book_depth) — add/remove levels");
    book.apply(&BookUpdate {
        source: StreamSource::DiffBookDepth,
        exch_ts: 1746057600600000000,
        local_ts: 1746057600600500000,
        bids: vec![
            PriceLevel { price: 96351.4, qty: 5.001 },  // updated qty
            PriceLevel { price: 96349.0, qty: 1.200 },  // new level
        ],
        asks: vec![
            PriceLevel { price: 96355.0, qty: 0.0 },    // removed (qty=0)
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
