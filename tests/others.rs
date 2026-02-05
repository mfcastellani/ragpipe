use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use ragpipe::chunk::tokens::TokenChunker;
use ragpipe::error::{Error, Result};
use ragpipe::pipeline::cancel::CancelToken;
use ragpipe::pipeline::chain::PipeExt;
use ragpipe::pipeline::pipe::Pipe;
use ragpipe::pipeline::runtime::Runtime;
use ragpipe::source::fs::FsSource;
use ragpipe::store::debug::DebugSink;
use tokio::sync::mpsc::{Receiver, Sender};

#[derive(Clone)]
struct VecSource<T> {
    items: Vec<T>,
}

impl<T> VecSource<T> {
    fn new(items: Vec<T>) -> Self {
        Self { items }
    }
}

#[async_trait]
impl<T> Pipe<(), T> for VecSource<T>
where
    T: Send + Sync + Clone + 'static,
{
    async fn process(
        &self,
        mut input: Receiver<()>,
        output: Sender<T>,
        _buffer: usize,
        cancel: CancelToken,
    ) -> Result<()> {
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            _ = input.recv() => {}
        }

        let items = self.items.clone();
        for v in items {
            if cancel.is_cancelled() {
                break;
            }
            if output.send(v).await.is_err() {
                return Err(Error::pipeline("output channel closed"));
            }
        }
        Ok(())
    }
}

struct CollectSink<T> {
    out: Arc<Mutex<Vec<T>>>,
}

impl<T> CollectSink<T> {
    fn new(out: Arc<Mutex<Vec<T>>>) -> Self {
        Self { out }
    }
}

#[async_trait]
impl<T> Pipe<T, ()> for CollectSink<T>
where
    T: Send + Sync + 'static,
{
    async fn process(
        &self,
        mut input: Receiver<T>,
        _output: Sender<()>,
        _buffer: usize,
        cancel: CancelToken,
    ) -> Result<()> {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                msg = input.recv() => {
                    let Some(v) = msg else { break; };
                    self.out.lock().unwrap().push(v);
                }
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn token_chunker_splits_bytes() -> Result<()> {
    let collected = Arc::new(Mutex::new(Vec::<Bytes>::new()));
    let sink = CollectSink::new(collected.clone());

    let input = Bytes::from_static(b"abcdefghij");
    let pipe = VecSource::new(vec![input])
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
    let chunks: Vec<Bytes> = out.iter().cloned().collect();
    assert_eq!(
        chunks,
        vec![
            Bytes::from_static(b"abcd"),
            Bytes::from_static(b"efgh"),
            Bytes::from_static(b"ij"),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn fs_source_reads_file() -> Result<()> {
    let dir = std::env::temp_dir();
    let path = dir.join("ragpipe_fs_source_test.txt");
    let data = b"ragpipe test data";
    std::fs::write(&path, data).unwrap();

    let collected = Arc::new(Mutex::new(Vec::<Bytes>::new()));
    let sink = CollectSink::new(collected.clone());

    let pipe = FsSource::new(path.to_string_lossy().to_string()).pipe::<(), _>(sink);

    let rt = Runtime::new().buffer(8);
    let (tx, mut rx, _cancel, handle) = rt.spawn(pipe);

    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });

    tx.send(()).await.unwrap();
    drop(tx);

    handle.await??;
    drain.await.unwrap();

    let out = collected.lock().unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].as_ref(), data);

    let _ = std::fs::remove_file(&path);
    Ok(())
}

#[tokio::test]
async fn debug_sink_consumes_bytes() -> Result<()> {
    let input = Bytes::from_static(b"hello");
    let pipe = VecSource::new(vec![input]).pipe::<(), _>(DebugSink);

    let rt = Runtime::new().buffer(8);
    let (tx, mut rx, _cancel, handle) = rt.spawn(pipe);

    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });

    tx.send(()).await.unwrap();
    drop(tx);

    handle.await??;
    drain.await.unwrap();
    Ok(())
}
