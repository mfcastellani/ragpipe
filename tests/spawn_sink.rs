use std::sync::{Arc, Mutex};
use std::time::Duration;

use ragpipe::error::{Error, Result};
use ragpipe::pipeline::chain::PipeExt;
use ragpipe::pipeline::runtime::Runtime;

mod common;
use common::{CollectSink, VecSource};

#[tokio::test]
async fn spawn_sink_completes_without_manual_drain() -> Result<()> {
    let collected = Arc::new(Mutex::new(Vec::<u32>::new()));
    let sink = CollectSink::new(collected.clone());

    let pipe = VecSource::new(vec![1u32, 2, 3, 4, 5])
        .map(|x| x * 10)
        .pipe::<(), _>(sink);

    let rt = Runtime::new().buffer(16);
    let (tx, _cancel, handle) = rt.spawn_sink(pipe);

    tx.send(()).await.unwrap();
    drop(tx);

    // No manual drain needed — handle.await is all that's required
    handle.await??;

    let items = collected.lock().unwrap();
    assert_eq!(*items, vec![10, 20, 30, 40, 50]);
    Ok(())
}

#[tokio::test]
async fn spawn_sink_with_cancellation() -> Result<()> {
    // Pipeline over a very large source; cancellation should stop it promptly.
    let pipe = VecSource::new((0u32..100_000).collect::<Vec<_>>())
        .pipe::<(), _>(CollectSink::new(Arc::new(Mutex::new(Vec::<u32>::new()))));

    let rt = Runtime::new().buffer(8);
    let (tx, cancel, handle) = rt.spawn_sink(pipe);

    tx.send(()).await.unwrap();

    // Let it start, then cancel
    tokio::time::sleep(Duration::from_millis(10)).await;
    cancel.cancel();
    drop(tx);

    let result = tokio::time::timeout(Duration::from_millis(500), handle).await;
    assert!(result.is_ok(), "pipeline should stop promptly after cancel");
    // Cancelled pipeline returns Ok(())
    result.unwrap().unwrap()?;
    Ok(())
}

#[tokio::test]
async fn spawn_sink_propagates_errors() -> Result<()> {
    let pipe = VecSource::new(vec![1u32])
        .try_map("fail", |_| async move {
            Err::<(), Error>(Error::pipeline("intentional error"))
        })
        .pipe::<(), _>(CollectSink::new(Arc::new(Mutex::new(Vec::<()>::new()))));

    let rt = Runtime::new().buffer(8);
    let (tx, _cancel, handle) = rt.spawn_sink(pipe);

    tx.send(()).await.unwrap();
    drop(tx);

    let join_result = handle.await.unwrap();
    assert!(
        join_result.is_err(),
        "pipeline error should propagate through handle"
    );
    Ok(())
}

#[tokio::test]
async fn spawn_sink_empty_source() -> Result<()> {
    let collected = Arc::new(Mutex::new(Vec::<u32>::new()));
    let pipe = VecSource::new(vec![]).pipe::<(), _>(CollectSink::new(collected.clone()));

    let rt = Runtime::new().buffer(8);
    let (tx, _cancel, handle) = rt.spawn_sink(pipe);

    tx.send(()).await.unwrap();
    drop(tx);

    handle.await??;

    assert!(collected.lock().unwrap().is_empty());
    Ok(())
}
