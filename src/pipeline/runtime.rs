use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::error::Result;
use crate::pipeline::cancel::CancelToken;
use crate::pipeline::config::{StageConfig, STAGE_CONFIG};
use crate::pipeline::pipe::Pipe;

pub struct Runtime {
    buffer: usize,
    stage_buffers: HashMap<&'static str, usize>,
    stage_concurrencies: HashMap<&'static str, usize>,
}

impl Runtime {
    pub fn new() -> Self {
        Self {
            buffer: 128,
            stage_buffers: HashMap::new(),
            stage_concurrencies: HashMap::new(),
        }
    }

    pub fn buffer(mut self, buffer: usize) -> Self {
        self.buffer = buffer;
        self
    }

    /// Override the intermediate channel buffer size for a specific named stage.
    ///
    /// This applies to the channel feeding *into* the named stage. Useful for
    /// tuning backpressure on slow or fast stages without affecting the global
    /// default.
    ///
    /// Has no effect on unnamed stages (`"map"`, `"filter"`, `"inspect"`).
    pub fn buffer_stage(mut self, stage: &'static str, buffer: usize) -> Self {
        self.stage_buffers.insert(stage, buffer);
        self
    }

    /// Set the number of concurrent workers for a specific `try_map` or
    /// `try_map_ref` stage.
    ///
    /// When `workers > 1`, items are dispatched to a pool of `workers` async
    /// tasks that all read from the same input channel concurrently. Output
    /// ordering is **not** preserved.
    ///
    /// This setting has no effect on `map`, `filter`, `inspect`, or custom
    /// [`Pipe`] implementations — only `try_map` and `try_map_ref` stages read
    /// the concurrency configuration.
    pub fn concurrency_stage(mut self, stage: &'static str, workers: usize) -> Self {
        self.stage_concurrencies.insert(stage, workers.max(1));
        self
    }

    fn make_config(&self) -> StageConfig {
        StageConfig {
            buffers: Arc::new(self.stage_buffers.clone()),
            concurrencies: Arc::new(self.stage_concurrencies.clone()),
        }
    }

    pub fn spawn<I, O, P>(
        &self,
        pipe: P,
    ) -> (
        mpsc::Sender<I>,
        mpsc::Receiver<O>,
        CancelToken,
        JoinHandle<Result<()>>,
    )
    where
        I: Send + 'static,
        O: Send + 'static,
        P: Pipe<I, O> + Send + Sync + 'static,
    {
        let (tx_in, rx_in) = mpsc::channel::<I>(self.buffer);
        let (tx_out, rx_out) = mpsc::channel::<O>(self.buffer);

        let buffer = self.buffer;
        let cancel = CancelToken::default();
        let cancel_task = cancel.clone();
        let config = self.make_config();

        #[cfg(feature = "tracing")]
        let handle = {
            use tracing::Instrument;
            let stage = pipe.stage_name();
            let span = tracing::info_span!("ragpipe.stage", stage = stage, buffer = buffer);
            tokio::spawn(
                STAGE_CONFIG
                    .scope(config, async move {
                        pipe.process(rx_in, tx_out, buffer, cancel_task).await
                    })
                    .instrument(span),
            )
        };

        #[cfg(not(feature = "tracing"))]
        let handle = tokio::spawn(STAGE_CONFIG.scope(config, async move {
            pipe.process(rx_in, tx_out, buffer, cancel_task).await
        }));

        (tx_in, rx_out, cancel, handle)
    }

    /// Spawn a pipeline that ends in a sink (output type `()`).
    ///
    /// Unlike [`Runtime::spawn`], there is no output `Receiver` to drain.
    /// The runtime drains the final channel internally so the caller only needs
    /// to send into `tx` and `await` the returned `JoinHandle`.
    ///
    /// # Returns
    ///
    /// `(Sender<I>, CancelToken, JoinHandle<Result<()>>)`
    ///
    /// # Example
    ///
    /// ```no_run
    /// use ragpipe::pipeline::chain::PipeExt;
    /// use ragpipe::pipeline::runtime::Runtime;
    /// use ragpipe::source::fs::FsSource;
    /// use ragpipe::store::debug::DebugSink;
    ///
    /// #[tokio::main]
    /// async fn main() -> ragpipe::error::Result<()> {
    ///     let pipe = FsSource::new("big.txt").pipe(DebugSink);
    ///
    ///     let rt = Runtime::new().buffer(128);
    ///     let (tx, _cancel, handle) = rt.spawn_sink(pipe);
    ///
    ///     tx.send(()).await.unwrap();
    ///     drop(tx);
    ///
    ///     handle.await??;
    ///     Ok(())
    /// }
    /// ```
    pub fn spawn_sink<I, P>(
        &self,
        pipe: P,
    ) -> (mpsc::Sender<I>, CancelToken, JoinHandle<Result<()>>)
    where
        I: Send + 'static,
        P: Pipe<I, ()> + Send + Sync + 'static,
    {
        let (tx_in, rx_in) = mpsc::channel::<I>(self.buffer);
        let (tx_out, mut rx_out) = mpsc::channel::<()>(self.buffer);
        let config = self.make_config();
        let cancel = CancelToken::default();
        let cancel_task = cancel.clone();
        let buffer = self.buffer;

        #[cfg(feature = "tracing")]
        let handle = {
            use tracing::Instrument;
            let stage = pipe.stage_name();
            let span = tracing::info_span!("ragpipe.stage", stage = stage, buffer = buffer);
            tokio::spawn(
                STAGE_CONFIG
                    .scope(config, async move {
                        let pipe_fut = pipe.process(rx_in, tx_out, buffer, cancel_task);
                        let drain_fut = async move { while rx_out.recv().await.is_some() {} };
                        let (pipe_res, _) = tokio::join!(pipe_fut, drain_fut);
                        pipe_res
                    })
                    .instrument(span),
            )
        };

        #[cfg(not(feature = "tracing"))]
        let handle = tokio::spawn(STAGE_CONFIG.scope(config, async move {
            let pipe_fut = pipe.process(rx_in, tx_out, buffer, cancel_task);
            let drain_fut = async move { while rx_out.recv().await.is_some() {} };
            let (pipe_res, _) = tokio::join!(pipe_fut, drain_fut);
            pipe_res
        }));

        (tx_in, cancel, handle)
    }
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new()
    }
}
