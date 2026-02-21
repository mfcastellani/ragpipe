use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use ragpipe::error::Result;
use ragpipe::pipeline::cancel::CancelToken;
use ragpipe::pipeline::chain::PipeExt;
use ragpipe::pipeline::pipe::Pipe;
use ragpipe::pipeline::runtime::Runtime;
use tokio::sync::mpsc::{Receiver, Sender};

mod common;
use common::VecSource;

/// A stage that tracks if it was cancelled
#[allow(dead_code)]
struct TrackingPipe {
    was_cancelled: Arc<AtomicBool>,
}

#[async_trait]
impl Pipe<u32, u32> for TrackingPipe {
    async fn process(
        &self,
        mut input: Receiver<u32>,
        output: Sender<u32>,
        _buffer: usize,
        cancel: CancelToken,
    ) -> Result<()> {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    self.was_cancelled.store(true, Ordering::SeqCst);
                    break
                },
                msg = input.recv() => {
                    let Some(v) = msg else { break; };
                    if output.send(v).await.is_err() {
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn cancellation_stops_pipeline_promptly() -> Result<()> {
    use std::time::Instant;

    // Use a very large dataset to ensure processing isn't done before cancel
    let pipe = VecSource::new((0..100000).collect::<Vec<u32>>())
        .strict_downstream(true)
        .map(|v| v * 2)
        .filter(|v| *v > 100);

    let rt = Runtime::new().buffer(16);
    let (tx, mut rx, cancel, handle) = rt.spawn(pipe);

    // Drain rx to prevent downstream close
    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });

    tx.send(()).await.unwrap();

    // Give it a moment to start
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Cancel and measure response time
    let start = Instant::now();
    cancel.cancel();
    drop(tx);

    let result = tokio::time::timeout(Duration::from_millis(500), handle).await;
    assert!(result.is_ok(), "pipeline should stop promptly after cancel");
    result.unwrap().unwrap()?;

    let elapsed = start.elapsed();
    drain.await.unwrap();

    // Cancellation should be fast
    assert!(
        elapsed < Duration::from_millis(300),
        "cancellation should be fast, took {:?}",
        elapsed
    );

    Ok(())
}

#[tokio::test]
async fn downstream_close_is_graceful() -> Result<()> {
    use std::sync::Mutex;

    struct EarlyCloseSink {
        close_after: usize,
        received: Arc<Mutex<Vec<u32>>>,
    }

    #[async_trait]
    impl Pipe<u32, ()> for EarlyCloseSink {
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
                        let Some(v) = msg else { break; };
                        self.received.lock().unwrap().push(v);
                        if self.received.lock().unwrap().len() >= self.close_after {
                            // Gracefully stop receiving
                            break;
                        }
                    }
                }
            }
            Ok(())
        }
    }

    let received = Arc::new(Mutex::new(Vec::new()));

    let pipe = VecSource::new((0..100).collect::<Vec<u32>>())
        .strict_downstream(false) // Allow graceful close
        .pipe::<(), _>(EarlyCloseSink {
            close_after: 5,
            received: received.clone(),
        });

    let rt = Runtime::new().buffer(8);
    let (tx, mut rx, _cancel, handle) = rt.spawn(pipe);

    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });

    tx.send(()).await.unwrap();
    drop(tx);

    // Should complete without error
    handle.await??;
    drain.await.unwrap();

    let items = received.lock().unwrap();
    assert_eq!(items.len(), 5, "sink should have received exactly 5 items");

    Ok(())
}

#[tokio::test]
async fn cancel_stops_retry_loop() -> Result<()> {
    use ragpipe::error::Error;
    use ragpipe::pipeline::retry::RetryPolicy;
    use tokio::sync::Notify;

    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_clone = attempts.clone();
    let started = Arc::new(Notify::new());
    let started_clone = started.clone();

    let retry = RetryPolicy::new(100)
        .base_delay(Duration::from_secs(1))
        .max_delay(Duration::from_secs(1))
        .retry_if(|_| true);

    let pipe = VecSource::new(vec![1u32])
        .strict_downstream(true)
        .try_map("always_fail", move |_| {
            let attempts = attempts_clone.clone();
            let started = started_clone.clone();
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                started.notify_waiters();
                Err::<u32, Error>(Error::pipeline("transient"))
            }
        })
        .with_retry(retry);

    let rt = Runtime::new().buffer(16);
    let (tx, _rx, cancel, handle) = rt.spawn(pipe);

    tx.send(()).await.unwrap();

    // Wait for first attempt
    started.notified().await;

    // Cancel during retry backoff
    tokio::time::sleep(Duration::from_millis(50)).await;
    cancel.cancel();
    drop(tx);

    let result = tokio::time::timeout(Duration::from_millis(500), handle).await;
    assert!(result.is_ok(), "pipeline should stop during retry backoff");

    // Should have attempted only once or twice, not 100 times
    let attempt_count = attempts.load(Ordering::SeqCst);
    assert!(
        attempt_count <= 3,
        "should stop retrying after cancel, got {} attempts",
        attempt_count
    );

    Ok(())
}

#[tokio::test]
async fn drop_input_sender_completes_pipeline() -> Result<()> {
    use std::sync::Mutex;

    let received = Arc::new(Mutex::new(Vec::<u32>::new()));

    struct SimpleSink {
        received: Arc<Mutex<Vec<u32>>>,
    }

    #[async_trait]
    impl Pipe<u32, ()> for SimpleSink {
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
                        let Some(v) = msg else { break; };
                        self.received.lock().unwrap().push(v);
                    }
                }
            }
            Ok(())
        }
    }

    let pipe = VecSource::new(vec![1, 2, 3])
        .strict_downstream(true)
        .pipe::<(), _>(SimpleSink {
            received: received.clone(),
        });

    let rt = Runtime::new().buffer(8);
    let (tx, mut rx, _cancel, handle) = rt.spawn(pipe);

    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });

    tx.send(()).await.unwrap();
    // Drop tx to signal completion
    drop(tx);

    // Pipeline should complete gracefully
    handle.await??;
    drain.await.unwrap();

    let items = received.lock().unwrap();
    assert_eq!(&*items, &[1, 2, 3]);

    Ok(())
}
