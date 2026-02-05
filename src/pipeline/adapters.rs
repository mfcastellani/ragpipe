use async_trait::async_trait;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::error::{Error, Result};
use crate::pipeline::cancel::CancelToken;
use crate::pipeline::pipe::Pipe;

/// map: O -> N
pub struct MapPipe<F>(pub F);

#[async_trait]
impl<I, N, F> Pipe<I, N> for MapPipe<F>
where
    I: Send + 'static,
    N: Send + 'static,
    F: Fn(I) -> N + Send + Sync + 'static,
{
    async fn process(
        &self,
        mut input: Receiver<I>,
        output: Sender<N>,
        _buffer: usize,
        cancel: CancelToken,
    ) -> Result<()> {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                msg = input.recv() => {
                    let Some(v) = msg else { break; };
                    if output.send((self.0)(v)).await.is_err() {
                        return Err(Error::pipeline("output channel closed"));
                    }
                }
            }
        }
        Ok(())
    }
}

pub struct FilterPipe<P>(pub P);

#[async_trait]
impl<T, P> Pipe<T, T> for FilterPipe<P>
where
    T: Send + 'static,
    P: Fn(&T) -> bool + Send + Sync + 'static,
{
    async fn process(
        &self,
        mut input: Receiver<T>,
        output: Sender<T>,
        _buffer: usize,
        cancel: CancelToken,
    ) -> Result<()> {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                msg = input.recv() => {
                    let Some(v) = msg else { break; };
                    if (self.0)(&v) && output.send(v).await.is_err() {
                        return Err(Error::pipeline("output channel closed"));
                    }
                }
            }
        }
        Ok(())
    }
}

pub struct InspectPipe<F>(pub F);

#[async_trait]
impl<T, F> Pipe<T, T> for InspectPipe<F>
where
    T: Send + 'static,
    F: Fn(&T) + Send + Sync + 'static,
{
    async fn process(
        &self,
        mut input: Receiver<T>,
        output: Sender<T>,
        _buffer: usize,
        cancel: CancelToken,
    ) -> Result<()> {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                msg = input.recv() => {
                    let Some(v) = msg else { break; };
                    (self.0)(&v);
                    if output.send(v).await.is_err() {
                        return Err(Error::pipeline("output channel closed"));
                    }
                }
            }
        }
        Ok(())
    }
}
