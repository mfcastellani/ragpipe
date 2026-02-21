#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use ragpipe::error::{Error, Result};
use ragpipe::pipeline::cancel::CancelToken;
use ragpipe::pipeline::pipe::Pipe;
use tokio::sync::mpsc::{Receiver, Sender};

#[derive(Clone)]
pub struct VecSource<T> {
    items: Vec<T>,
    strict_downstream: bool,
}

impl<T> VecSource<T> {
    pub fn new(items: Vec<T>) -> Self {
        Self {
            items,
            strict_downstream: false,
        }
    }

    #[allow(dead_code)]
    pub fn strict_downstream(mut self, strict: bool) -> Self {
        self.strict_downstream = strict;
        self
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
        for item in items {
            if cancel.is_cancelled() {
                break;
            }
            if output.send(item).await.is_err() {
                // Downstream closing due to cancellation is expected and graceful.
                if self.strict_downstream && !cancel.is_cancelled() {
                    return Err(Error::pipeline("downstream receiver closed unexpectedly"));
                }
                break;
            }
        }
        Ok(())
    }
}

pub struct CollectSink<T> {
    out: Arc<Mutex<Vec<T>>>,
}

impl<T> CollectSink<T> {
    pub fn new(out: Arc<Mutex<Vec<T>>>) -> Self {
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
                    self.out.lock().expect("mutex poisoned").push(v);
                }
            }
        }
        Ok(())
    }
}
