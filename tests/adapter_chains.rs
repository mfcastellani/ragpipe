use std::sync::{Arc, Mutex};

use ragpipe::error::Result;
use ragpipe::pipeline::chain::PipeExt;
use ragpipe::pipeline::runtime::Runtime;

mod common;
use common::{CollectSink, VecSource};

#[tokio::test]
async fn map_filter_inspect_chain() -> Result<()> {
    let collected = Arc::new(Mutex::new(Vec::<u32>::new()));
    let inspected = Arc::new(Mutex::new(Vec::<u32>::new()));
    let sink = CollectSink::new(collected.clone());
    let inspected_clone = inspected.clone();

    let pipe = VecSource::new(vec![1u32, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        .strict_downstream(true)
        .map(|v| v * 2)
        .filter(|v| *v > 10)
        .inspect(move |v| inspected_clone.lock().unwrap().push(*v))
        .map(|v| v + 100)
        .pipe::<(), _>(sink);

    let rt = Runtime::new().buffer(16);
    let (tx, mut rx, _cancel, handle) = rt.spawn(pipe);

    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });

    tx.send(()).await.unwrap();
    drop(tx);

    handle.await??;
    drain.await.unwrap();

    let out = collected.lock().unwrap();
    let seen = inspected.lock().unwrap();

    // map(*2): [2,4,6,8,10,12,14,16,18,20]
    // filter(>10): [12,14,16,18,20]
    // inspect: should see [12,14,16,18,20]
    // map(+100): [112,114,116,118,120]
    assert_eq!(&*seen, &[12, 14, 16, 18, 20]);
    assert_eq!(&*out, &[112, 114, 116, 118, 120]);
    Ok(())
}

#[tokio::test]
async fn filter_filter_chain() -> Result<()> {
    let collected = Arc::new(Mutex::new(Vec::<u32>::new()));
    let sink = CollectSink::new(collected.clone());

    let pipe = VecSource::new(vec![1u32, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])
        .strict_downstream(true)
        .filter(|v| *v % 2 == 0) // even numbers
        .filter(|v| *v % 3 == 0) // divisible by 3
        .pipe::<(), _>(sink);

    let rt = Runtime::new().buffer(16);
    let (tx, mut rx, _cancel, handle) = rt.spawn(pipe);

    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });

    tx.send(()).await.unwrap();
    drop(tx);

    handle.await??;
    drain.await.unwrap();

    let out = collected.lock().unwrap();
    // Numbers divisible by both 2 and 3 (i.e., divisible by 6)
    assert_eq!(&*out, &[6, 12]);
    Ok(())
}

#[tokio::test]
async fn map_map_map_chain() -> Result<()> {
    let collected = Arc::new(Mutex::new(Vec::<u32>::new()));
    let sink = CollectSink::new(collected.clone());

    let pipe = VecSource::new(vec![1u32, 2, 3])
        .strict_downstream(true)
        .map(|v| v * 10)
        .map(|v| v + 5)
        .map(|v| v / 3)
        .pipe::<(), _>(sink);

    let rt = Runtime::new().buffer(16);
    let (tx, mut rx, _cancel, handle) = rt.spawn(pipe);

    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });

    tx.send(()).await.unwrap();
    drop(tx);

    handle.await??;
    drain.await.unwrap();

    let out = collected.lock().unwrap();
    // 1 -> 10 -> 15 -> 5
    // 2 -> 20 -> 25 -> 8
    // 3 -> 30 -> 35 -> 11
    assert_eq!(&*out, &[5, 8, 11]);
    Ok(())
}

#[tokio::test]
async fn filter_removes_all_items() -> Result<()> {
    let collected = Arc::new(Mutex::new(Vec::<u32>::new()));
    let sink = CollectSink::new(collected.clone());

    let pipe = VecSource::new(vec![1u32, 3, 5, 7, 9])
        .strict_downstream(true)
        .filter(|v| *v % 2 == 0)
        .pipe::<(), _>(sink);

    let rt = Runtime::new().buffer(16);
    let (tx, mut rx, _cancel, handle) = rt.spawn(pipe);

    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });

    tx.send(()).await.unwrap();
    drop(tx);

    handle.await??;
    drain.await.unwrap();

    let out = collected.lock().unwrap();
    assert!(out.is_empty(), "filter should remove all odd numbers");
    Ok(())
}

#[tokio::test]
async fn inspect_multiple_times() -> Result<()> {
    let collected = Arc::new(Mutex::new(Vec::<u32>::new()));
    let inspect1 = Arc::new(Mutex::new(Vec::<u32>::new()));
    let inspect2 = Arc::new(Mutex::new(Vec::<u32>::new()));
    let sink = CollectSink::new(collected.clone());
    let inspect1_clone = inspect1.clone();
    let inspect2_clone = inspect2.clone();

    let pipe = VecSource::new(vec![1u32, 2, 3])
        .strict_downstream(true)
        .inspect(move |v| inspect1_clone.lock().unwrap().push(*v))
        .map(|v| v * 2)
        .inspect(move |v| inspect2_clone.lock().unwrap().push(*v))
        .pipe::<(), _>(sink);

    let rt = Runtime::new().buffer(16);
    let (tx, mut rx, _cancel, handle) = rt.spawn(pipe);

    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });

    tx.send(()).await.unwrap();
    drop(tx);

    handle.await??;
    drain.await.unwrap();

    let out = collected.lock().unwrap();
    let seen1 = inspect1.lock().unwrap();
    let seen2 = inspect2.lock().unwrap();

    assert_eq!(&*seen1, &[1, 2, 3]);
    assert_eq!(&*seen2, &[2, 4, 6]);
    assert_eq!(&*out, &[2, 4, 6]);
    Ok(())
}
