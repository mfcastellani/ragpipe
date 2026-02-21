//! Demonstrates `Runtime::buffer_stage`, `Runtime::concurrency_stage`, and
//! `Runtime::spawn_sink` working together.
//!
//! Run with:
//! ```bash
//! cargo run --example runtime_config
//! ```

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use ragpipe::error::{Error, Result};
use ragpipe::pipeline::cancel::CancelToken;
use ragpipe::pipeline::chain::PipeExt;
use ragpipe::pipeline::pipe::Pipe;
use ragpipe::pipeline::runtime::Runtime;
use tokio::sync::mpsc::{Receiver, Sender};

// ── Source that emits document IDs ─────────────────────────────────────────

struct DocSource {
    count: u32,
}

#[async_trait]
impl Pipe<(), String> for DocSource {
    fn stage_name(&self) -> &'static str {
        "doc_source"
    }

    async fn process(
        &self,
        mut input: Receiver<()>,
        output: Sender<String>,
        _buffer: usize,
        cancel: CancelToken,
    ) -> Result<()> {
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            _ = input.recv() => {}
        }

        for i in 1..=self.count {
            if cancel.is_cancelled() {
                break;
            }
            if output.send(format!("doc-{i:02}")).await.is_err() {
                break;
            }
        }
        Ok(())
    }
}

// ── Sink that counts received items ────────────────────────────────────────

struct CountSink {
    received: Arc<AtomicUsize>,
}

#[async_trait]
impl Pipe<String, ()> for CountSink {
    fn stage_name(&self) -> &'static str {
        "count_sink"
    }

    async fn process(
        &self,
        mut input: Receiver<String>,
        _output: Sender<()>,
        _buffer: usize,
        cancel: CancelToken,
    ) -> Result<()> {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                msg = input.recv() => {
                    let Some(v) = msg else { break; };
                    let n = self.received.fetch_add(1, Ordering::Relaxed) + 1;
                    println!("  [sink] stored #{n}: {v}");
                }
            }
        }
        Ok(())
    }
}

// ── Main ───────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let embedded = Arc::new(AtomicUsize::new(0));
    let stored = Arc::new(AtomicUsize::new(0));

    let embedded_c = embedded.clone();

    // Pipeline:
    //   DocSource → try_map("embed") → inspect → CountSink
    //
    // Runtime config:
    //   - global buffer:          64 items
    //   - "embed" channel buffer:  8 items  (narrow for backpressure demo)
    //   - "embed" concurrency:     4 workers (parallel embedding)
    let pipeline = DocSource { count: 20 }
        .try_map("embed", move |doc: String| {
            let embedded = embedded_c.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(10)).await;
                let n = embedded.fetch_add(1, Ordering::Relaxed) + 1;
                println!("  [embed worker] embedding #{n}: {doc}");
                Ok::<String, Error>(format!("vec({doc})"))
            }
        })
        .inspect(|v| println!("  [inspect] passing downstream: {v}"))
        .pipe(CountSink {
            received: stored.clone(),
        });

    let rt = Runtime::new()
        .buffer(64)
        .buffer_stage("embed", 8) // narrow channel into the slow stage
        .concurrency_stage("embed", 4); // 4 parallel embed workers

    println!("Starting pipeline (4 concurrent embed workers, 8-slot buffer)…\n");

    // spawn_sink: no manual drain of a final Receiver<()> needed.
    let (tx, _cancel, handle) = rt.spawn_sink(pipeline);

    tx.send(()).await.unwrap();
    drop(tx);

    handle.await??;

    println!(
        "\nDone: embedded {}, stored {}.",
        embedded.load(Ordering::Relaxed),
        stored.load(Ordering::Relaxed),
    );
    Ok(())
}
