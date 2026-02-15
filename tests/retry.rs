use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ragpipe::error::{Error, Result};
use ragpipe::pipeline::chain::PipeExt;
use ragpipe::pipeline::retry::{ErrorAction, RetryPolicy};
use ragpipe::pipeline::runtime::Runtime;
use tokio::sync::Notify;

mod common;
use common::{CollectSink, VecSource};

#[tokio::test]
async fn retries_succeed_after_n_failures() -> Result<()> {
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_in_op = attempts.clone();
    let collected = Arc::new(Mutex::new(Vec::<u32>::new()));
    let sink = CollectSink::new(collected.clone());

    let retry = RetryPolicy::new(4)
        .base_delay(Duration::from_millis(1))
        .max_delay(Duration::from_millis(2))
        .retry_if(|err| format!("{err}").contains("transient"));

    let pipe = VecSource::new(vec![5u32])
        .strict_downstream(true)
        .try_map("double", move |value| {
            let attempts = attempts_in_op.clone();
            async move {
                let current = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                if current <= 2 {
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

    tx.send(()).await.unwrap();
    drop(tx);

    handle.await??;
    drain.await.unwrap();

    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    assert_eq!(&*collected.lock().unwrap(), &[10]);
    Ok(())
}

#[tokio::test]
async fn retries_stop_after_max_attempts() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_in_op = attempts.clone();

    let retry = RetryPolicy::new(3)
        .base_delay(Duration::from_millis(1))
        .max_delay(Duration::from_millis(1))
        .retry_if(|_| true);

    let pipe = VecSource::new(vec![1u32])
        .strict_downstream(true)
        .try_map("always_fail", move |_| {
            let attempts = attempts_in_op.clone();
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err::<u32, Error>(Error::pipeline("transient"))
            }
        })
        .with_retry(retry);

    let rt = Runtime::new().buffer(16);
    let (tx, _rx, _cancel, handle) = rt.spawn(pipe);
    tx.send(()).await.unwrap();
    drop(tx);

    let err = handle.await.unwrap().unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("exhausted retries"));
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn retry_policy_new_does_not_retry_by_default() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_in_op = attempts.clone();

    let retry = RetryPolicy::new(5)
        .base_delay(Duration::from_millis(1))
        .max_delay(Duration::from_millis(1));

    let pipe = VecSource::new(vec![1u32])
        .strict_downstream(true)
        .try_map("no_default_retry", move |_| {
            let attempts = attempts_in_op.clone();
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err::<u32, Error>(Error::pipeline("transient"))
            }
        })
        .with_retry(retry);

    let rt = Runtime::new().buffer(16);
    let (tx, _rx, _cancel, handle) = rt.spawn(pipe);
    tx.send(()).await.unwrap();
    drop(tx);

    let err = handle.await.unwrap().unwrap_err();
    let msg = format!("{err}");
    assert!(!msg.contains("exhausted retries"));
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn on_error_skip_drops_item_and_continues() -> Result<()> {
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_in_op = attempts.clone();
    let collected = Arc::new(Mutex::new(Vec::<u32>::new()));
    let sink = CollectSink::new(collected.clone());

    let pipe = VecSource::new(vec![1u32, 2, 3])
        .strict_downstream(true)
        .try_map("skip_bad", move |value| {
            let attempts = attempts_in_op.clone();
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                if value == 2 {
                    Err(Error::pipeline("skip-me"))
                } else {
                    Ok(value)
                }
            }
        })
        .with_retry(RetryPolicy::new(3))
        .on_error(|ctx| {
            if matches!(ctx.error, Error::Pipeline { context: "skip-me" }) {
                ErrorAction::Skip
            } else {
                ErrorAction::Fail(Error::stage(
                    ctx.stage,
                    format!("unexpected error at attempt {}", ctx.attempt),
                ))
            }
        })
        .pipe::<(), _>(sink);

    let rt = Runtime::new().buffer(16);
    let (tx, mut rx, _cancel, handle) = rt.spawn(pipe);
    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });

    tx.send(()).await.unwrap();
    drop(tx);

    handle.await??;
    drain.await.unwrap();

    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    assert_eq!(&*collected.lock().unwrap(), &[1, 3]);
    Ok(())
}

#[tokio::test]
async fn on_error_fail_stops_immediately() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_in_op = attempts.clone();

    // Retry only if the error is a Stage error (example). Since we'll emit Pipeline("fatal"),
    // this should be NON-retryable and fail immediately.
    let retry = RetryPolicy::new(5)
        .base_delay(Duration::from_millis(1))
        .max_delay(Duration::from_millis(1))
        .retry_if(|err| matches!(err, Error::Stage { .. }));

    let pipe = VecSource::new(vec![1u32])
        .strict_downstream(true)
        .try_map("fatal_stage", move |_| {
            let attempts = attempts_in_op.clone();
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err::<u32, Error>(Error::pipeline("fatal"))
            }
        })
        .with_retry(retry)
        .on_error(|ctx| {
            // keep original error as source, but fail fast with a clear stage context
            ErrorAction::Fail(Error::stage_source(
                ctx.stage,
                Error::Message(format!("fail-fast on attempt {}", ctx.attempt)),
            ))
        });

    let rt = Runtime::new().buffer(16);
    let (tx, mut rx, _cancel, handle) = rt.spawn(pipe);

    // Drain output so sends don't fail due to dropped receiver.
    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });

    tx.send(()).await.unwrap();
    drop(tx);

    let err = handle.await.unwrap().unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("fail-fast"));
    assert_eq!(attempts.load(Ordering::SeqCst), 1);

    drain.await.unwrap();
}

#[tokio::test]
async fn on_error_retry_honors_max_attempts_and_exhausts() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_in_op = attempts.clone();

    let retry = RetryPolicy::new(3)
        .base_delay(Duration::from_millis(1))
        .max_delay(Duration::from_millis(1));

    let pipe = VecSource::new(vec![1u32])
        .strict_downstream(true)
        .try_map("handler_retry", move |_| {
            let attempts = attempts_in_op.clone();
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err::<u32, Error>(Error::pipeline("always"))
            }
        })
        .with_retry(retry)
        .on_error(|_ctx| ErrorAction::Retry);

    let rt = Runtime::new().buffer(16);
    let (tx, _rx, _cancel, handle) = rt.spawn(pipe);
    tx.send(()).await.unwrap();
    drop(tx);

    let err = handle.await.unwrap().unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("exhausted retries"));
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn cancellation_during_retry_stops_pipeline() -> Result<()> {
    let started = Arc::new(Notify::new());
    let started_in_op = started.clone();

    let retry = RetryPolicy::new(10)
        .base_delay(Duration::from_secs(10))
        .max_delay(Duration::from_secs(10))
        .retry_if(|_| true);

    let pipe = VecSource::new(vec![1u32])
        .strict_downstream(true)
        .try_map("cancel_backoff", move |_| {
            let started = started_in_op.clone();
            async move {
                started.notify_waiters();
                Err::<u32, Error>(Error::pipeline("transient"))
            }
        })
        .with_retry(retry);

    let rt = Runtime::new().buffer(16);
    let (tx, _rx, cancel, handle) = rt.spawn(pipe);
    tx.send(()).await.unwrap();
    started.notified().await;
    cancel.cancel();
    drop(tx);

    let finished = tokio::time::timeout(Duration::from_millis(300), handle).await;
    assert!(finished.is_ok(), "pipeline did not stop after cancellation");
    finished.unwrap().unwrap()?;
    Ok(())
}
