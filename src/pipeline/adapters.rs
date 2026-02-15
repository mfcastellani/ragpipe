use async_trait::async_trait;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::error::Result;
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
    fn stage_name(&self) -> &'static str {
        "map"
    }

    async fn process(
        &self,
        mut input: Receiver<I>,
        output: Sender<N>,
        _buffer: usize,
        cancel: CancelToken,
    ) -> Result<()> {
        #[cfg(feature = "tracing")]
        let stage = self.stage_name();

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    #[cfg(feature = "tracing")]
                    tracing::event!(tracing::Level::DEBUG, event = "ragpipe.cancelled", stage = stage, where_ = "recv", "ragpipe.cancelled");
                    break
                },
                msg = input.recv() => {
                    let Some(v) = msg else { break; };
                    if output.send((self.0)(v)).await.is_err() {
                        #[cfg(feature = "tracing")]
                        tracing::event!(tracing::Level::INFO, event = "ragpipe.downstream.closed", stage = stage, "ragpipe.downstream.closed");
                        break;
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
    fn stage_name(&self) -> &'static str {
        "filter"
    }

    async fn process(
        &self,
        mut input: Receiver<T>,
        output: Sender<T>,
        _buffer: usize,
        cancel: CancelToken,
    ) -> Result<()> {
        #[cfg(feature = "tracing")]
        let stage = self.stage_name();

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    #[cfg(feature = "tracing")]
                    tracing::event!(tracing::Level::DEBUG, event = "ragpipe.cancelled", stage = stage, where_ = "recv", "ragpipe.cancelled");
                    break
                },
                msg = input.recv() => {
                    let Some(v) = msg else { break; };
                    if (self.0)(&v) && output.send(v).await.is_err() {
                        #[cfg(feature = "tracing")]
                        tracing::event!(tracing::Level::INFO, event = "ragpipe.downstream.closed", stage = stage, "ragpipe.downstream.closed");
                        break;
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
    fn stage_name(&self) -> &'static str {
        "inspect"
    }

    async fn process(
        &self,
        mut input: Receiver<T>,
        output: Sender<T>,
        _buffer: usize,
        cancel: CancelToken,
    ) -> Result<()> {
        #[cfg(feature = "tracing")]
        let stage = self.stage_name();

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    #[cfg(feature = "tracing")]
                    tracing::event!(tracing::Level::DEBUG, event = "ragpipe.cancelled", stage = stage, where_ = "recv", "ragpipe.cancelled");
                    break
                },
                msg = input.recv() => {
                    let Some(v) = msg else { break; };
                    (self.0)(&v);
                    if output.send(v).await.is_err() {
                        #[cfg(feature = "tracing")]
                        tracing::event!(tracing::Level::INFO, event = "ragpipe.downstream.closed", stage = stage, "ragpipe.downstream.closed");
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}
