use std::sync::{Arc, Mutex};

use bytes::Bytes;
use ragpipe::chunk::tokens::TokenChunker;
use ragpipe::error::Result;
use ragpipe::pipeline::chain::PipeExt;
use ragpipe::pipeline::runtime::Runtime;

mod common;
use common::{CollectSink, VecSource};

#[tokio::test]
async fn chunker_empty_input() -> Result<()> {
    let collected = Arc::new(Mutex::new(Vec::<Bytes>::new()));
    let sink = CollectSink::new(collected.clone());

    let input = Bytes::new();
    let pipe = VecSource::new(vec![input])
        .strict_downstream(true)
        .pipe::<Bytes, _>(TokenChunker::new(4))
        .pipe::<(), _>(sink);

    let rt = Runtime::new().buffer(8);
    let (tx, mut rx, _cancel, handle) = rt.spawn(pipe);

    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });

    tx.send(()).await.unwrap();
    drop(tx);

    handle.await??;
    drain.await.unwrap();

    let out = collected.lock().unwrap();
    assert!(out.is_empty(), "empty input should produce no chunks");
    Ok(())
}

#[tokio::test]
async fn chunker_exact_multiple() -> Result<()> {
    let collected = Arc::new(Mutex::new(Vec::<Bytes>::new()));
    let sink = CollectSink::new(collected.clone());

    let input = Bytes::from_static(b"abcdefgh");
    let pipe = VecSource::new(vec![input])
        .strict_downstream(true)
        .pipe::<Bytes, _>(TokenChunker::new(4))
        .pipe::<(), _>(sink);

    let rt = Runtime::new().buffer(8);
    let (tx, mut rx, _cancel, handle) = rt.spawn(pipe);

    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });

    tx.send(()).await.unwrap();
    drop(tx);

    handle.await??;
    drain.await.unwrap();

    let out = collected.lock().unwrap();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0], Bytes::from_static(b"abcd"));
    assert_eq!(out[1], Bytes::from_static(b"efgh"));
    Ok(())
}

#[tokio::test]
async fn chunker_single_byte_size() -> Result<()> {
    let collected = Arc::new(Mutex::new(Vec::<Bytes>::new()));
    let sink = CollectSink::new(collected.clone());

    let input = Bytes::from_static(b"abc");
    let pipe = VecSource::new(vec![input])
        .strict_downstream(true)
        .pipe::<Bytes, _>(TokenChunker::new(1))
        .pipe::<(), _>(sink);

    let rt = Runtime::new().buffer(8);
    let (tx, mut rx, _cancel, handle) = rt.spawn(pipe);

    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });

    tx.send(()).await.unwrap();
    drop(tx);

    handle.await??;
    drain.await.unwrap();

    let out = collected.lock().unwrap();
    assert_eq!(out.len(), 3);
    assert_eq!(out[0], Bytes::from_static(b"a"));
    assert_eq!(out[1], Bytes::from_static(b"b"));
    assert_eq!(out[2], Bytes::from_static(b"c"));
    Ok(())
}

#[tokio::test]
async fn chunker_larger_than_input() -> Result<()> {
    let collected = Arc::new(Mutex::new(Vec::<Bytes>::new()));
    let sink = CollectSink::new(collected.clone());

    let input = Bytes::from_static(b"small");
    let pipe = VecSource::new(vec![input])
        .strict_downstream(true)
        .pipe::<Bytes, _>(TokenChunker::new(1024))
        .pipe::<(), _>(sink);

    let rt = Runtime::new().buffer(8);
    let (tx, mut rx, _cancel, handle) = rt.spawn(pipe);

    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });

    tx.send(()).await.unwrap();
    drop(tx);

    handle.await??;
    drain.await.unwrap();

    let out = collected.lock().unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0], Bytes::from_static(b"small"));
    Ok(())
}

#[tokio::test]
async fn chunker_multiple_inputs() -> Result<()> {
    let collected = Arc::new(Mutex::new(Vec::<Bytes>::new()));
    let sink = CollectSink::new(collected.clone());

    let inputs = vec![
        Bytes::from_static(b"abc"),
        Bytes::from_static(b"def"),
        Bytes::from_static(b"ghi"),
    ];
    let pipe = VecSource::new(inputs)
        .strict_downstream(true)
        .pipe::<Bytes, _>(TokenChunker::new(2))
        .pipe::<(), _>(sink);

    let rt = Runtime::new().buffer(8);
    let (tx, mut rx, _cancel, handle) = rt.spawn(pipe);

    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });

    tx.send(()).await.unwrap();
    drop(tx);

    handle.await??;
    drain.await.unwrap();

    let out = collected.lock().unwrap();
    // Each input is chunked separately: "abc" -> "ab", "c"; "def" -> "de", "f"; "ghi" -> "gh", "i"
    assert_eq!(out.len(), 6);
    assert_eq!(out[0], Bytes::from_static(b"ab"));
    assert_eq!(out[1], Bytes::from_static(b"c"));
    Ok(())
}
