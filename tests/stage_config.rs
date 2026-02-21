use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ragpipe::error::{Error, Result};
use ragpipe::pipeline::chain::PipeExt;
use ragpipe::pipeline::runtime::Runtime;

mod common;
use common::{CollectSink, VecSource};

// ── buffer_stage ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn buffer_stage_override_applies() -> Result<()> {
    // Use a 1-slot override to maximise backpressure on the "transform" stage.
    // The pipeline should still complete correctly — we just verify it doesn't deadlock.
    let collected = Arc::new(Mutex::new(Vec::<u32>::new()));
    let sink = CollectSink::new(collected.clone());

    let pipe = VecSource::new((0u32..50).collect::<Vec<_>>())
        .try_map("transform", |x| async move { Ok::<u32, Error>(x * 2) })
        .pipe::<(), _>(sink);

    let rt = Runtime::new().buffer(128).buffer_stage("transform", 1); // tiny intermediate buffer

    let (tx, _cancel, handle) = rt.spawn_sink(pipe);
    tx.send(()).await.unwrap();
    drop(tx);
    handle.await??;

    let items = collected.lock().unwrap();
    assert_eq!(items.len(), 50);
    assert_eq!(items[0], 0);
    assert_eq!(items[49], 98);
    Ok(())
}

#[tokio::test]
async fn buffer_stage_unknown_name_falls_back_to_global() -> Result<()> {
    // An override for "nonexistent" shouldn't cause errors
    let collected = Arc::new(Mutex::new(Vec::<u32>::new()));
    let pipe = VecSource::new(vec![1u32, 2, 3]).pipe::<(), _>(CollectSink::new(collected.clone()));

    let rt = Runtime::new().buffer(32).buffer_stage("nonexistent", 4);

    let (tx, _cancel, handle) = rt.spawn_sink(pipe);
    tx.send(()).await.unwrap();
    drop(tx);
    handle.await??;

    assert_eq!(*collected.lock().unwrap(), vec![1, 2, 3]);
    Ok(())
}

// ── concurrency_stage ─────────────────────────────────────────────────────────

#[tokio::test]
async fn concurrency_stage_achieves_parallel_execution() -> Result<()> {
    let peak = Arc::new(AtomicUsize::new(0));
    let active = Arc::new(AtomicUsize::new(0));
    let collected = Arc::new(Mutex::new(Vec::<u32>::new()));

    let peak_c = peak.clone();
    let active_c = active.clone();

    let pipe = VecSource::new((0u32..20).collect::<Vec<_>>())
        .try_map("parallel", move |x| {
            let peak = peak_c.clone();
            let active = active_c.clone();
            async move {
                let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                // Track peak concurrency
                let mut current_peak = peak.load(Ordering::SeqCst);
                while now > current_peak {
                    match peak.compare_exchange(
                        current_peak,
                        now,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) {
                        Ok(_) => break,
                        Err(actual) => current_peak = actual,
                    }
                }
                // Simulate work
                tokio::time::sleep(Duration::from_millis(5)).await;
                active.fetch_sub(1, Ordering::SeqCst);
                Ok::<u32, Error>(x)
            }
        })
        .pipe::<(), _>(CollectSink::new(collected.clone()));

    let rt = Runtime::new().buffer(32).concurrency_stage("parallel", 4);

    let (tx, _cancel, handle) = rt.spawn_sink(pipe);
    tx.send(()).await.unwrap();
    drop(tx);
    handle.await??;

    let observed_peak = peak.load(Ordering::SeqCst);
    assert!(
        observed_peak >= 2,
        "expected parallel execution (peak >= 2), got peak = {}",
        observed_peak
    );
    assert_eq!(collected.lock().unwrap().len(), 20);
    Ok(())
}

#[tokio::test]
async fn concurrency_stage_error_fails_pipeline() -> Result<()> {
    // One item fails → pipeline should return Err
    let pipe = VecSource::new(vec![1u32, 2, 3, 4, 5])
        .try_map("fail_on_3", |x| async move {
            if x == 3 {
                Err(Error::pipeline("item 3 is bad"))
            } else {
                // Slight delay so workers overlap
                tokio::time::sleep(Duration::from_millis(5)).await;
                Ok(x)
            }
        })
        .pipe::<(), _>(CollectSink::new(Arc::new(Mutex::new(Vec::<u32>::new()))));

    let rt = Runtime::new().buffer(16).concurrency_stage("fail_on_3", 3);

    let (tx, _cancel, handle) = rt.spawn_sink(pipe);
    tx.send(()).await.unwrap();
    drop(tx);

    let result = handle.await.unwrap();
    assert!(result.is_err(), "pipeline should fail when a worker errors");
    Ok(())
}

#[tokio::test]
async fn concurrency_stage_cancellation_stops_workers() -> Result<()> {
    let pipe = VecSource::new((0u32..100_000).collect::<Vec<_>>())
        .try_map("slow", |x| async move {
            tokio::time::sleep(Duration::from_millis(1)).await;
            Ok::<u32, Error>(x)
        })
        .pipe::<(), _>(CollectSink::new(Arc::new(Mutex::new(Vec::<u32>::new()))));

    let rt = Runtime::new().buffer(16).concurrency_stage("slow", 4);

    let (tx, cancel, handle) = rt.spawn_sink(pipe);
    tx.send(()).await.unwrap();

    tokio::time::sleep(Duration::from_millis(20)).await;
    cancel.cancel();
    drop(tx);

    let result = tokio::time::timeout(Duration::from_millis(500), handle).await;
    assert!(result.is_ok(), "pipeline should stop promptly after cancel");
    result.unwrap().unwrap()?;
    Ok(())
}

#[tokio::test]
async fn buffer_and_concurrency_compose() -> Result<()> {
    // Both features active at the same time
    let collected = Arc::new(Mutex::new(Vec::<u32>::new()));
    let pipe = VecSource::new((1u32..=30).collect::<Vec<_>>())
        .try_map("compute", |x| async move { Ok::<u32, Error>(x * x) })
        .pipe::<(), _>(CollectSink::new(collected.clone()));

    let rt = Runtime::new()
        .buffer(64)
        .buffer_stage("compute", 4)
        .concurrency_stage("compute", 3);

    let (tx, _cancel, handle) = rt.spawn_sink(pipe);
    tx.send(()).await.unwrap();
    drop(tx);
    handle.await??;

    let mut items = collected.lock().unwrap().clone();
    items.sort();
    let expected: Vec<u32> = (1u32..=30).map(|x| x * x).collect();
    assert_eq!(items, expected);
    Ok(())
}

#[tokio::test]
async fn concurrency_one_is_same_as_default() -> Result<()> {
    // concurrency_stage("stage", 1) should behave identically to no override
    let collected = Arc::new(Mutex::new(Vec::<u32>::new()));
    let pipe = VecSource::new(vec![1u32, 2, 3])
        .try_map("stage", |x| async move { Ok::<u32, Error>(x) })
        .pipe::<(), _>(CollectSink::new(collected.clone()));

    let rt = Runtime::new().buffer(16).concurrency_stage("stage", 1);

    let (tx, _cancel, handle) = rt.spawn_sink(pipe);
    tx.send(()).await.unwrap();
    drop(tx);
    handle.await??;

    assert_eq!(*collected.lock().unwrap(), vec![1, 2, 3]);
    Ok(())
}

#[tokio::test]
async fn try_map_ref_concurrency_stage() -> Result<()> {
    // Verify concurrency also works for try_map_ref
    let collected = Arc::new(Mutex::new(Vec::<usize>::new()));
    let pipe = VecSource::new(vec!["hello", "world", "rust", "async"])
        .try_map_ref("len", |s| {
            let len = s.len();
            async move { Ok::<usize, Error>(len) }
        })
        .pipe::<(), _>(CollectSink::new(collected.clone()));

    let rt = Runtime::new().buffer(16).concurrency_stage("len", 2);

    let (tx, _cancel, handle) = rt.spawn_sink(pipe);
    tx.send(()).await.unwrap();
    drop(tx);
    handle.await??;

    let mut items = collected.lock().unwrap().clone();
    items.sort();
    assert_eq!(items, vec![4, 5, 5, 5]); // "rust"=4, "hello"=5, "world"=5, "async"=5
    Ok(())
}
