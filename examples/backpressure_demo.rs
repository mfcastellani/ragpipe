//! Backpressure Demonstration
//!
//! Run with:
//!   cargo run --example backpressure_demo
//!
//! This example demonstrates how ragpipe manages backpressure with bounded channels.
//! It shows the difference between small and large buffer sizes and how
//! the pipeline naturally regulates flow when downstream is slower.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use ragpipe::error::{Error, Result};
use ragpipe::pipeline::cancel::CancelToken;
use ragpipe::pipeline::chain::PipeExt;
use ragpipe::pipeline::pipe::Pipe;
use ragpipe::pipeline::runtime::Runtime;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Notify;

/// Fast data source that produces items quickly
struct FastSource {
    count: usize,
    produced: Arc<AtomicUsize>,
}

impl FastSource {
    fn new(count: usize) -> Self {
        Self {
            count,
            produced: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn get_produced_count(&self) -> Arc<AtomicUsize> {
        self.produced.clone()
    }
}

#[async_trait]
impl Pipe<(), u32> for FastSource {
    fn stage_name(&self) -> &'static str {
        "fast_source"
    }

    async fn process(
        &self,
        mut input: Receiver<()>,
        output: Sender<u32>,
        _buffer: usize,
        cancel: CancelToken,
    ) -> Result<()> {
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            _ = input.recv() => {}
        }

        for i in 0..self.count {
            if cancel.is_cancelled() {
                break;
            }

            // Try to send, but this will block if buffer is full (backpressure!)
            if output.send(i as u32).await.is_err() {
                break;
            }

            self.produced.fetch_add(1, Ordering::SeqCst);
        }

        Ok(())
    }
}

/// Slow consumer that takes time to process each item
struct SlowSink {
    delay_ms: u64,
    processed: Arc<AtomicUsize>,
    started: Arc<Notify>,
}

impl SlowSink {
    fn new(delay_ms: u64) -> Self {
        Self {
            delay_ms,
            processed: Arc::new(AtomicUsize::new(0)),
            started: Arc::new(Notify::new()),
        }
    }

    fn get_processed_count(&self) -> Arc<AtomicUsize> {
        self.processed.clone()
    }

    fn get_started_signal(&self) -> Arc<Notify> {
        self.started.clone()
    }
}

#[async_trait]
impl Pipe<u32, ()> for SlowSink {
    fn stage_name(&self) -> &'static str {
        "slow_sink"
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
                    let Some(_v) = msg else { break; };

                    self.started.notify_waiters();
                    self.processed.fetch_add(1, Ordering::SeqCst);

                    // Simulate slow processing
                    tokio::time::sleep(Duration::from_millis(self.delay_ms)).await;
                }
            }
        }
        Ok(())
    }
}

async fn run_with_buffer(buffer_size: usize, item_count: usize, delay_ms: u64) -> Result<Duration> {
    let source = FastSource::new(item_count);
    let sink = SlowSink::new(delay_ms);

    let produced = source.get_produced_count();
    let processed = sink.get_processed_count();
    let started = sink.get_started_signal();

    let pipeline = source.pipe(sink);

    let rt = Runtime::new().buffer(buffer_size);
    let (tx, mut rx, _cancel, handle) = rt.spawn(pipeline);

    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });

    let start = Instant::now();
    tx.send(())
        .await
        .map_err(|_| Error::pipeline("failed to start"))?;

    // Wait for processing to start
    started.notified().await;

    // Sample progress
    tokio::time::sleep(Duration::from_millis(200)).await;

    println!(
        "  Buffer: {:>3} | After 200ms: produced={:>3}, processed={:>3}",
        buffer_size,
        produced.load(Ordering::SeqCst),
        processed.load(Ordering::SeqCst)
    );

    drop(tx);
    handle.await??;
    drain.await.map_err(Error::Join)?;

    let elapsed = start.elapsed();

    println!(
        "  Buffer: {:>3} | Completed in {:>4}ms | Total: produced={}, processed={}",
        buffer_size,
        elapsed.as_millis(),
        produced.load(Ordering::SeqCst),
        processed.load(Ordering::SeqCst)
    );

    Ok(elapsed)
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("🌊 Backpressure Demonstration\n");
    println!("This demo shows how buffer size affects pipeline behavior");
    println!("when upstream is fast and downstream is slow.\n");

    let item_count = 50;
    let delay_ms = 20; // Each item takes 20ms to process

    println!("📊 Configuration:");
    println!("  - Items to process: {}", item_count);
    println!("  - Processing delay: {}ms per item", delay_ms);
    println!(
        "  - Theoretical minimum time: {}ms\n",
        item_count * delay_ms as usize
    );

    println!("🔬 Experiment 1: Very Small Buffer (size=2)");
    println!("Expected: Strong backpressure, upstream blocked frequently\n");
    run_with_buffer(2, item_count, delay_ms).await?;

    println!("\n🔬 Experiment 2: Medium Buffer (size=10)");
    println!("Expected: Moderate backpressure, some buffering\n");
    run_with_buffer(10, item_count, delay_ms).await?;

    println!("\n🔬 Experiment 3: Large Buffer (size=100)");
    println!("Expected: Minimal backpressure, upstream completes quickly\n");
    run_with_buffer(100, item_count, delay_ms).await?;

    println!("\n💡 Key Observations:");
    println!("   1. Small buffers (2): Upstream blocks frequently, memory stays bounded");
    println!("      → Good for memory-constrained environments");
    println!("\n   2. Medium buffers (10): Balance between throughput and memory");
    println!("      → Good for most use cases");
    println!("\n   3. Large buffers (100): Upstream completes fast, more memory used");
    println!("      → Good when downstream variance is high");

    println!("\n🎯 Backpressure Benefits:");
    println!("   ✓ Prevents unbounded memory growth");
    println!("   ✓ Natural flow control without explicit coordination");
    println!("   ✓ Upstream automatically adjusts to downstream capacity");
    println!("   ✓ No manual throttling logic needed");

    println!("\n⚙️  Tuning Recommendations:");
    println!("   - Start with buffer size ~10-50 for typical pipelines");
    println!("   - Increase buffer if stages have high latency variance");
    println!("   - Decrease buffer in memory-constrained environments");
    println!("   - Monitor actual memory usage in production");

    Ok(())
}
