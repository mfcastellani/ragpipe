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
        // aguarda start ou cancel
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            _ = input.recv() => {}
        }

        // evita borrow atravessando await
        let items = self.items.clone();
        for item in items {
            if cancel.is_cancelled() {
                break;
            }
            if output.send(item).await.is_err() {
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

struct FailPipe;

#[async_trait]
impl Pipe<u32, u32> for FailPipe {
    async fn process(
        &self,
        mut input: Receiver<u32>,
        _output: Sender<u32>,
        _buffer: usize,
        cancel: CancelToken,
    ) -> Result<()> {
        tokio::select! {
            _ = cancel.cancelled() => Ok(()),
            msg = input.recv() => {
                if msg.is_some() {
                    Err(Error::Pipeline { context: "boom" })
                } else {
                    Ok(())
                }
            }
        }
    }
}

#[tokio::test]
async fn chain_passes_items_through() -> Result<()> {
    let collected = Arc::new(Mutex::new(Vec::<u32>::new()));
    let sink = CollectSink::new(collected.clone());

    let pipe = VecSource::new(vec![1, 2, 3, 4]).pipe::<(), _>(sink);

    let rt = Runtime::new().buffer(16);
    let (tx, _rx, _cancel, handle) = rt.spawn(pipe);

    tx.send(()).await.unwrap();
    drop(tx);

    handle.await??;

    assert_eq!(&*collected.lock().unwrap(), &[1, 2, 3, 4]);
    Ok(())
}

#[tokio::test]
async fn chain_propagates_error() {
    // source emite u32 e o FailPipe falha no primeiro item
    let pipe = VecSource::new(vec![10u32]).pipe::<u32, _>(FailPipe);

    let rt = Runtime::new().buffer(16);
    let (tx, _rx, _cancel, handle) = rt.spawn(pipe);

    tx.send(()).await.unwrap();
    drop(tx);

    let err = handle.await.unwrap().unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("boom"));
}

#[tokio::test]
async fn multi_stage_chain_works() -> Result<()> {
    struct LenPipe;

    #[async_trait]
    impl Pipe<Vec<u8>, u32> for LenPipe {
        async fn process(
            &self,
            mut input: Receiver<Vec<u8>>,
            output: Sender<u32>,
            _buffer: usize,
            cancel: CancelToken,
        ) -> Result<()> {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    msg = input.recv() => {
                        let Some(v) = msg else { break; };
                        if output.send(v.len() as u32).await.is_err() {
                            return Err(Error::pipeline("output channel closed"));
                        }
                    }
                }
            }
            Ok(())
        }
    }

    let collected = Arc::new(Mutex::new(Vec::<u32>::new()));
    let sink = CollectSink::new(collected.clone());

    let pipe = VecSource::new(vec![vec![1, 2], vec![3, 4, 5], vec![]])
        .pipe::<u32, _>(LenPipe)
        .pipe::<(), _>(sink);

    let rt = Runtime::new().buffer(8);
    let (tx, _rx, _cancel, handle) = rt.spawn(pipe);

    tx.send(()).await.unwrap();
    drop(tx);

    handle.await??;

    assert_eq!(&*collected.lock().unwrap(), &[2, 3, 0]);
    Ok(())
}
