use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use ragpipe::error::{Error, Result};
use ragpipe::pipeline::cancel::CancelToken;
use ragpipe::pipeline::chain::PipeExt;
use ragpipe::pipeline::pipe::Pipe;
use ragpipe::pipeline::runtime::Runtime;
use tokio::sync::mpsc::{Receiver, Sender};

mod common;
use common::{CollectSink, VecSource};

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

    let pipe = VecSource::new(vec![1, 2, 3, 4])
        .strict_downstream(true)
        .pipe::<(), _>(sink);

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
    let pipe = VecSource::new(vec![10u32])
        .strict_downstream(true)
        .pipe::<u32, _>(FailPipe);

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
        .strict_downstream(true)
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
