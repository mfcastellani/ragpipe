use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use ragpipe::error::{Error, Result};
use ragpipe::pipeline::cancel::CancelToken;
use ragpipe::pipeline::chain::PipeExt;
use ragpipe::pipeline::pipe::Pipe;
use ragpipe::pipeline::runtime::Runtime;
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
async fn map_transforms_items() -> Result<()> {
    let collected = Arc::new(Mutex::new(Vec::<u32>::new()));
    let sink = CollectSink::new(collected.clone());

    let pipe = VecSource::new(vec![1u32, 2, 3])
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
