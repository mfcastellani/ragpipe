//! Run with:
//!   cargo run --example ndjson_source --features ndjson

use serde_json::Value;

use ragpipe::error::{Error, Result};
use ragpipe::pipeline::runtime::Runtime;
use ragpipe::source::ndjson::NdjsonSource;

#[tokio::main]
async fn main() -> Result<()> {
    let source = NdjsonSource::<Value>::from_file("mocks/data.ndjson")
        .allow_empty_lines(true)
        .max_line_bytes(1024 * 1024);

    let rt = Runtime::new().buffer(16);
    let (tx, mut rx, _cancel, handle) = rt.spawn(source);

    tx.send(())
        .await
        .map_err(|_| Error::pipeline("failed to start pipeline"))?;
    drop(tx);

    let mut out = Vec::new();
    while let Some(item) = rx.recv().await {
        out.push(item);
    }

    handle.await??;

    println!("Collected {} items:", out.len());
    for (idx, value) in out.iter().enumerate() {
        println!("  {}: {}", idx + 1, value);
    }

    Ok(())
}
