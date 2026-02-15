use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ragpipe::error::{Error, Result};
use ragpipe::pipeline::chain::PipeExt;
use ragpipe::pipeline::retry::RetryPolicy;
use ragpipe::pipeline::runtime::Runtime;
use tokio::sync::Notify;

mod common;
use common::{CollectSink, VecSource};

#[tokio::test(start_paused = true)]
async fn retry_waits_for_advance_before_next_attempt() -> Result<()> {
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_in_op = attempts.clone();
    let collected = Arc::new(Mutex::new(Vec::<u32>::new()));
    let sink = CollectSink::new(collected.clone());

    let retry = RetryPolicy::new(2)
        .base_delay(Duration::from_secs(1))
        .max_delay(Duration::from_secs(1))
        .retry_if(|_| true);

    let pipe = VecSource::new(vec![2u32])
        .strict_downstream(true)
        .try_map("retry_time", move |value| {
            let attempts = attempts_in_op.clone();
            async move {
                let n = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                if n == 1 {
                    Err(Error::pipeline("transient"))
                } else {
                    Ok(value * 2)
                }
            }
        })
        .with_retry(retry)
        .pipe::<(), _>(sink);

    let rt = Runtime::new().buffer(16);
    let (tx, mut rx, _cancel, handle) = rt.spawn(pipe);
    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });

    tx.send(()).await.expect("start send failed");
    drop(tx);

    for _ in 0..10 {
        if attempts.load(Ordering::SeqCst) >= 1 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert!(
        !handle.is_finished(),
        "pipeline should be waiting in retry sleep"
    );

    tokio::time::advance(Duration::from_millis(999)).await;
    tokio::task::yield_now().await;
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert!(!handle.is_finished(), "pipeline should still be sleeping");

    tokio::time::advance(Duration::from_millis(1)).await;
    handle.await??;
    drain.await.expect("drain join failed");

    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(&*collected.lock().expect("mutex poisoned"), &[4]);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn cancellation_during_paused_backoff_finishes_without_advance() -> Result<()> {
    let started = Arc::new(Notify::new());
    let started_in_op = started.clone();

    let retry = RetryPolicy::new(10)
        .base_delay(Duration::from_secs(3600))
        .max_delay(Duration::from_secs(3600))
        .retry_if(|_| true);

    let pipe = VecSource::new(vec![1u32])
        .strict_downstream(true)
        .try_map("cancel_paused", move |_| {
            let started = started_in_op.clone();
            async move {
                started.notify_waiters();
                Err::<u32, Error>(Error::pipeline("transient"))
            }
        })
        .with_retry(retry);

    let rt = Runtime::new().buffer(16);
    let (tx, _rx, cancel, handle) = rt.spawn(pipe);
    tx.send(()).await.expect("start send failed");
    started.notified().await;
    cancel.cancel();
    drop(tx);

    for _ in 0..10 {
        if handle.is_finished() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        handle.is_finished(),
        "pipeline should stop after cancellation without advancing time"
    );

    handle.await??;
    Ok(())
}
