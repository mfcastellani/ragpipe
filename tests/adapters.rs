use std::sync::{Arc, Mutex};

use ragpipe::error::Result;
use ragpipe::pipeline::chain::PipeExt;
use ragpipe::pipeline::runtime::Runtime;
mod common;
use common::{CollectSink, VecSource};

#[tokio::test]
async fn map_transforms_items() -> Result<()> {
    let collected = Arc::new(Mutex::new(Vec::<u32>::new()));
    let sink = CollectSink::new(collected.clone());

    let pipe = VecSource::new(vec![1u32, 2, 3])
        .strict_downstream(true)
        .map(|v| v * 10)
        .pipe::<(), _>(sink);

    let rt = Runtime::new().buffer(16);
    let (tx, mut rx, _cancel, handle) = rt.spawn(pipe);

    // drena o rx em background pra manter o canal aberto
    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });

    tx.send(()).await.unwrap();
    drop(tx);

    handle.await??;
    drain.await.unwrap(); // <<< ESSENCIAL

    assert_eq!(&*collected.lock().unwrap(), &[10, 20, 30]);
    Ok(())
}

#[tokio::test]
async fn filter_keeps_only_matching() -> Result<()> {
    let collected = Arc::new(Mutex::new(Vec::<u32>::new()));
    let sink = CollectSink::new(collected.clone());

    let pipe = VecSource::new(vec![1u32, 2, 3, 4, 5])
        .strict_downstream(true)
        .filter(|v| *v % 2 == 0)
        .pipe::<(), _>(sink);

    let rt = Runtime::new().buffer(16);
    let (tx, mut rx, _cancel, handle) = rt.spawn(pipe);

    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });

    tx.send(()).await.unwrap();
    drop(tx);

    handle.await??;
    drain.await.unwrap(); // <<< ESSENCIAL

    assert_eq!(&*collected.lock().unwrap(), &[2, 4]);
    Ok(())
}

#[tokio::test]
async fn inspect_sees_all_items() -> Result<()> {
    let seen = Arc::new(Mutex::new(Vec::<u32>::new()));
    let seen2 = seen.clone();

    let collected = Arc::new(Mutex::new(Vec::<u32>::new()));
    let sink = CollectSink::new(collected.clone());

    let pipe = VecSource::new(vec![7u32, 8, 9])
        .strict_downstream(true)
        .inspect(move |v| seen2.lock().unwrap().push(*v))
        .pipe::<(), _>(sink);

    let rt = Runtime::new().buffer(16);
    let (tx, mut rx, _cancel, handle) = rt.spawn(pipe);

    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });

    tx.send(()).await.unwrap();
    drop(tx);

    handle.await??;
    drain.await.unwrap(); // <<< ESSENCIAL

    assert_eq!(&*seen.lock().unwrap(), &[7, 8, 9]);
    assert_eq!(&*collected.lock().unwrap(), &[7, 8, 9]);
    Ok(())
}
