//! Error Recovery Patterns Example
//!
//! Run with:
//!   cargo run --example error_recovery
//!
//! This example demonstrates various error handling patterns in ragpipe:
//! - Retry transient errors with exponential backoff
//! - Skip malformed items and continue processing
//! - Fail fast on critical errors
//! - Combine multiple error strategies in one pipeline

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use ragpipe::error::{Error, Result};
use ragpipe::pipeline::cancel::CancelToken;
use ragpipe::pipeline::chain::PipeExt;
use ragpipe::pipeline::pipe::Pipe;
use ragpipe::pipeline::retry::{ErrorAction, RetryPolicy};
use ragpipe::pipeline::runtime::Runtime;
use tokio::sync::mpsc::{Receiver, Sender};

/// Simulates a data source with mixed quality items
struct MixedQualitySource {
    items: Vec<(u32, &'static str)>,
}

impl MixedQualitySource {
    fn new() -> Self {
        Self {
            items: vec![
                (1, "good"),
                (2, "transient"), // Will fail temporarily
                (3, "good"),
                (4, "malformed"), // Should be skipped
                (5, "good"),
                (6, "transient"),
                (7, "good"),
                (8, "malformed"),
                (9, "good"),
                (10, "critical"), // Should stop pipeline
                (11, "good"),     // Won't be processed
            ],
        }
    }
}

#[async_trait]
impl Pipe<(), (u32, &'static str)> for MixedQualitySource {
    fn stage_name(&self) -> &'static str {
        "mixed_source"
    }

    async fn process(
        &self,
        mut input: Receiver<()>,
        output: Sender<(u32, &'static str)>,
        _buffer: usize,
        cancel: CancelToken,
    ) -> Result<()> {
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            _ = input.recv() => {}
        }

        for &item in &self.items {
            if cancel.is_cancelled() {
                break;
            }

            if output.send(item).await.is_err() {
                break;
            }
        }

        Ok(())
    }
}

/// Statistics tracker for the pipeline
#[derive(Clone)]
struct Stats {
    processed: Arc<AtomicUsize>,
    retried: Arc<AtomicUsize>,
    skipped: Arc<AtomicUsize>,
}

impl Stats {
    fn new() -> Self {
        Self {
            processed: Arc::new(AtomicUsize::new(0)),
            retried: Arc::new(AtomicUsize::new(0)),
            skipped: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn record_processed(&self) {
        self.processed.fetch_add(1, Ordering::SeqCst);
    }

    fn record_retry(&self) {
        self.retried.fetch_add(1, Ordering::SeqCst);
    }

    fn record_skip(&self) {
        self.skipped.fetch_add(1, Ordering::SeqCst);
    }

    fn report(&self) {
        println!("\n📊 Pipeline Statistics:");
        println!(
            "   ✓ Successfully processed: {}",
            self.processed.load(Ordering::SeqCst)
        );
        println!(
            "   🔄 Retried (transient errors): {}",
            self.retried.load(Ordering::SeqCst)
        );
        println!(
            "   ⏭  Skipped (malformed items): {}",
            self.skipped.load(Ordering::SeqCst)
        );
    }
}

/// Sink that collects successfully processed items
struct CollectingSink {
    results: Arc<tokio::sync::Mutex<Vec<u32>>>,
    stats: Stats,
}

#[async_trait]
impl Pipe<u32, ()> for CollectingSink {
    fn stage_name(&self) -> &'static str {
        "collector"
    }

    async fn process(
        &self,
        mut input: Receiver<u32>,
        _output: Sender<()>,
        _buffer: usize,
        cancel: CancelToken,
    ) -> Result<()> {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                msg = input.recv() => {
                    let Some(id) = msg else { break; };
                    self.results.lock().await.push(id);
                    self.stats.record_processed();
                    println!("   ✓ Processed item {}", id);
                }
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("🔧 Error Recovery Patterns Demo\n");
    println!("This example shows how to handle different types of errors:\n");
    println!("  1️⃣  Retry: Transient network/API errors");
    println!("  2️⃣  Skip: Malformed data that can't be processed");
    println!("  3️⃣  Fail: Critical errors that should stop the pipeline\n");

    let stats = Stats::new();
    let results = Arc::new(tokio::sync::Mutex::new(Vec::new()));

    // Track retry attempts per item
    let retry_tracker: Arc<tokio::sync::Mutex<std::collections::HashMap<u32, usize>>> =
        Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new()));

    let stats_for_processor = stats.clone();
    let stats_for_error_handler = stats.clone();
    let stats_for_sink = stats.clone();
    let retry_tracker_clone = retry_tracker.clone();

    // Configure retry policy for transient errors
    let retry_policy = RetryPolicy::new(3)
        .base_delay(std::time::Duration::from_millis(50))
        .max_delay(std::time::Duration::from_millis(200))
        .retry_if(|err| {
            // Only retry errors marked as "transient"
            matches!(err, Error::Pipeline { context } if *context == "transient")
        });

    let pipeline = MixedQualitySource::new()
        .try_map("process", move |(id, quality)| {
            let stats = stats_for_processor.clone();
            let tracker = retry_tracker_clone.clone();
            async move {
                match quality {
                    "good" => {
                        // Normal processing
                        Ok(id)
                    }
                    "transient" => {
                        // Simulate transient failure that can be retried
                        let mut map = tracker.lock().await;
                        let attempts = map.entry(id).or_insert(0);
                        *attempts += 1;

                        if *attempts <= 2 {
                            println!(
                                "   ⚠️  Item {} failed (attempt {}) - will retry",
                                id, attempts
                            );
                            stats.record_retry();
                            Err(Error::pipeline("transient"))
                        } else {
                            println!("   ✅ Item {} succeeded after {} attempts", id, attempts);
                            Ok(id)
                        }
                    }
                    "malformed" => {
                        // Data that can't be fixed by retrying
                        Err(Error::pipeline("malformed"))
                    }
                    "critical" => {
                        // Critical error that should stop everything
                        Err(Error::pipeline("critical"))
                    }
                    _ => Err(Error::pipeline("unknown")),
                }
            }
        })
        .with_retry(retry_policy)
        .on_error(move |ctx| {
            // Custom error handler to decide what to do with each error
            match ctx.error {
                Error::Pipeline {
                    context: "malformed",
                } => {
                    println!("   ⏭  Skipping malformed item (stage: {})", ctx.stage);
                    stats_for_error_handler.record_skip();
                    ErrorAction::Skip
                }
                Error::Pipeline {
                    context: "critical",
                } => {
                    println!("\n   🛑 Critical error encountered - stopping pipeline");
                    ErrorAction::Fail(Error::stage(ctx.stage, "critical error - aborting"))
                }
                _ => {
                    // Default: let retry policy handle it
                    ErrorAction::Retry
                }
            }
        })
        .pipe(CollectingSink {
            results: results.clone(),
            stats: stats_for_sink,
        });

    println!("🔄 Starting pipeline...\n");

    let rt = Runtime::new().buffer(16);
    let (tx, mut rx, _cancel, handle) = rt.spawn(pipeline);

    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });

    tx.send(())
        .await
        .map_err(|_| Error::pipeline("failed to start"))?;
    drop(tx);

    // Wait for pipeline to complete (will fail due to critical error)
    let result = handle.await.unwrap();
    drain.await.map_err(Error::Join)?;

    // Show results
    println!("\n📋 Results:");

    if let Err(e) = result {
        println!("   Pipeline stopped: {}", e);
    }

    let final_results = results.lock().await;
    println!("\n   Successfully processed items: {:?}", *final_results);

    stats.report();

    println!("\n💡 Key Takeaways:");
    println!("   - Transient errors were retried and eventually succeeded");
    println!("   - Malformed items were skipped, allowing pipeline to continue");
    println!("   - Critical error stopped the pipeline immediately");
    println!("   - Items after the critical error were never processed");

    Ok(())
}
