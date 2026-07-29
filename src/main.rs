mod book;
mod stream;
mod util;

use std::sync::Arc;
use std::time::Instant;

use tokio::sync::Mutex;

use crate::book::LocalOrderBook;
use stream::{StreamConfig, StreamReceiver, build_book_from_snapshot};

const SYMBOL: &str = "BTCUSDT";

async fn stream_to_book() {
    println!("=== Async Stream Receiver ===\n");

    let rest_base = stream::urls::REST_FAPI;;
    let snapshot_url = format!("{rest_base}?symbol={SYMBOL}&limit=1000");

    let book: Arc<Mutex<LocalOrderBook>> = Arc::new(Mutex::new(
        match build_book_from_snapshot(SYMBOL, 1.0, 0.001, Some(250), &snapshot_url).await {
            Ok(book) => {
                println!("[snapshot] Initial snapshot applied");
                book
            }
            Err(e) => {
                eprintln!("[snapshot] Initial fetch failed: {e} — starting empty");
                LocalOrderBook::new(SYMBOL, 1.0, 0.001, Some(250))
            }
        }));

    println!("{:.10}", book.lock().await);

    let stream_configs = vec![
        StreamConfig::book_ticker(SYMBOL),
        StreamConfig::partial_depth(SYMBOL, 5, 100),
        StreamConfig::partial_depth(SYMBOL, 10, 100),
        StreamConfig::partial_depth(SYMBOL, 20, 100),
        StreamConfig::diff_depth(SYMBOL, 100),
    ];

    let mut receiver = StreamReceiver::new(book, stream_configs, rest_base.to_string(), SYMBOL);

    let mut last_print = Instant::now();
    let mut last_flush = Instant::now();

    receiver
        .run(Box::new(move |book| {
            if last_flush.elapsed().as_secs() >= 5 {
                book.flush_timing_log();
                last_flush = Instant::now();
            }
            if last_print.elapsed().as_secs() >= 5 {
                println!("{book:.30}");
                last_print = Instant::now();
            }
        }))
        .await;
}

async fn stream_dry_run() {
    println!("=== Async Stream Receiver (dry-run) ===\n");

    let book = Arc::new(Mutex::new(LocalOrderBook::new(SYMBOL, 0.1, 0.001, None)));

    let configs = vec![
        StreamConfig::book_ticker(SYMBOL),
        StreamConfig::partial_depth(SYMBOL, 5, 100),
        StreamConfig::partial_depth(SYMBOL, 10, 100),
        StreamConfig::partial_depth(SYMBOL, 20, 100),
        StreamConfig::diff_depth(SYMBOL, 100),
    ];

    let receiver = StreamReceiver::new(book, configs, stream::urls::REST_FAPI.to_string(), SYMBOL);
    receiver.dry_run().await;
}

#[tokio::main]
async fn main() {
    stream_to_book().await;
    // stream_dry_run().await;
}
