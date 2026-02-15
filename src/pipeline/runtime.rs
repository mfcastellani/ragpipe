use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::error::Result;
use crate::pipeline::cancel::CancelToken;
use crate::pipeline::pipe::Pipe;

pub struct Runtime {
    buffer: usize,
}

impl Runtime {
    pub fn new() -> Self {
        Self { buffer: 128 }
    }

    pub fn buffer(mut self, buffer: usize) -> Self {
        self.buffer = buffer;
        self
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

        #[cfg(feature = "tracing")]
        let handle = {
            use tracing::Instrument;
            let stage = pipe.stage_name();
            let span = tracing::info_span!("ragpipe.stage", stage = stage, buffer = buffer);
            tokio::spawn(
                async move { pipe.process(rx_in, tx_out, buffer, cancel_task).await }
                    .instrument(span),
            )
        };

        #[cfg(not(feature = "tracing"))]
        let handle =
            tokio::spawn(async move { pipe.process(rx_in, tx_out, buffer, cancel_task).await });

        (tx_in, rx_out, cancel, handle)
    }
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new()
    }
}
