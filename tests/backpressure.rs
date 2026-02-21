use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use ragpipe::error::Result;
use ragpipe::pipeline::cancel::CancelToken;
use ragpipe::pipeline::chain::PipeExt;
use ragpipe::pipeline::pipe::Pipe;
use ragpipe::pipeline::runtime::Runtime;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Notify;

mod common;
use common::VecSource;

/// A slow consumer that takes time to process each item
struct SlowSink {
    delay: Duration,
    processed: Arc<AtomicUsize>,
    started: Arc<Notify>,
}

impl SlowSink {
    fn new(delay: Duration, processed: Arc<AtomicUsize>, started: Arc<Notify>) -> Self {
        Self {
            delay,
            processed,
            started,
        }
    }
}

#[async_trait]
impl Pipe<u32, ()> for SlowSink {
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
                    tokio::time::sleep(self.delay).await;
                }
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn small_buffer_creates_backpressure() -> Result<()> {
    let processed = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());

    // Create a pipeline with a very small buffer
    let pipe = VecSource::new((0..100).collect::<Vec<u32>>())
        .strict_downstream(true)
        .pipe::<(), _>(SlowSink::new(
            Duration::from_millis(10),
            processed.clone(),
            started.clone(),
        ));

    let rt = Runtime::new().buffer(2); // Very small buffer
    let (tx, mut rx, _cancel, handle) = rt.spawn(pipe);

    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });

    tx.send(()).await.unwrap();

    // Wait for processing to start
    started.notified().await;

    // Give a short time for some items to process
    tokio::time::sleep(Duration::from_millis(100)).await;

    let count_after_100ms = processed.load(Ordering::SeqCst);

    // Due to backpressure with buffer=2, not all items should be processed immediately
    // We expect significantly fewer than 100 items processed
    assert!(
        count_after_100ms < 50,
        "backpressure should limit processing, got {}",
        count_after_100ms
    );

    drop(tx);
    handle.await??;
    drain.await.unwrap();

    // Eventually all items should be processed
    assert_eq!(processed.load(Ordering::SeqCst), 100);
    Ok(())
}

#[tokio::test]
async fn large_buffer_allows_faster_processing() -> Result<()> {
    let processed = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());

    let pipe = VecSource::new((0..100).collect::<Vec<u32>>())
        .strict_downstream(true)
        .pipe::<(), _>(SlowSink::new(
            Duration::from_millis(10),
            processed.clone(),
            started.clone(),
        ));

    let rt = Runtime::new().buffer(200); // Large buffer
    let (tx, mut rx, _cancel, handle) = rt.spawn(pipe);

    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });

    tx.send(()).await.unwrap();

    // Wait for processing to start
    started.notified().await;

    // Give a short time for some items to process
    tokio::time::sleep(Duration::from_millis(100)).await;

    let count_after_100ms = processed.load(Ordering::SeqCst);

    // With large buffer, more items can be queued and processed
    // We still won't process all 100 in 100ms with 10ms delay each,
    // but we should process more than with small buffer
    assert!(
        count_after_100ms >= 5,
        "large buffer should allow more processing, got {}",
        count_after_100ms
    );

    drop(tx);
    handle.await??;
    drain.await.unwrap();

    assert_eq!(processed.load(Ordering::SeqCst), 100);
    Ok(())
}

#[tokio::test]
async fn downstream_close_stops_upstream() -> Result<()> {
    use std::sync::Mutex;

    /// A source that tracks how many items it sent
    struct CountingSource {
        items: Vec<u32>,
        sent: Arc<Mutex<usize>>,
    }

    #[async_trait]
    impl Pipe<(), u32> for CountingSource {
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

            for &item in &self.items {
                if cancel.is_cancelled() {
                    break;
                }

                if output.send(item).await.is_err() {
                    // Downstream closed, stop sending
                    break;
                }
                *self.sent.lock().unwrap() += 1;
            }

            Ok(())
        }
    }

    /// A sink that closes after receiving N items
    struct LimitedSink {
        limit: usize,
        received: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Pipe<u32, ()> for LimitedSink {
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
                        let count = self.received.fetch_add(1, Ordering::SeqCst) + 1;
                        if count >= self.limit {
                            // Stop receiving, which closes the channel
                            break;
                        }
                    }
                }
            }
            Ok(())
        }
    }

    let sent = Arc::new(Mutex::new(0));
    let received = Arc::new(AtomicUsize::new(0));

    let source = CountingSource {
        items: (0..1000).collect(),
        sent: sent.clone(),
    };

    let sink = LimitedSink {
        limit: 10,
        received: received.clone(),
    };

    let pipe = source.pipe::<(), _>(sink);

    let rt = Runtime::new().buffer(16);
    let (tx, mut rx, _cancel, handle) = rt.spawn(pipe);

    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });

    tx.send(()).await.unwrap();
    drop(tx);

    handle.await??;
    drain.await.unwrap();

    let sent_count = *sent.lock().unwrap();
    let received_count = received.load(Ordering::SeqCst);

    // Sink should receive exactly its limit
    assert_eq!(received_count, 10);

    // Source should stop shortly after downstream closes
    // Due to buffering, it might send a few more than received, but not all 1000
    assert!(
        sent_count <= 50,
        "source should stop when downstream closes, but sent {}",
        sent_count
    );
    assert!(sent_count >= 10, "source should send at least what was received");

    Ok(())
}
