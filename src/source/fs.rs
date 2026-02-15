use async_trait::async_trait;
use bytes::Bytes;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::error::Result;
use crate::pipeline::cancel::CancelToken;
use crate::pipeline::pipe::Pipe;

pub struct FsSource {
    path: String,
}

impl FsSource {
    pub fn new(path: impl Into<String>) -> Self {
        Self { path: path.into() }
    }
}

#[async_trait]
impl Pipe<(), Bytes> for FsSource {
    fn stage_name(&self) -> &'static str {
        "fs_source"
    }

    async fn process(
        &self,
        mut input: Receiver<()>,
        output: Sender<Bytes>,
        _buffer: usize,
        cancel: CancelToken,
    ) -> Result<()> {
        #[cfg(feature = "tracing")]
        let stage = self.stage_name();

        tokio::select! {
            _ = cancel.cancelled() => {
                #[cfg(feature = "tracing")]
                tracing::event!(tracing::Level::DEBUG, event = "ragpipe.cancelled", stage = stage, where_ = "recv", "ragpipe.cancelled");
                return Ok(());
            },
            _ = input.recv() => {}
        }

        let mut file = File::open(&self.path).await?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).await?;

        if cancel.is_cancelled() {
            #[cfg(feature = "tracing")]
            tracing::event!(
                tracing::Level::DEBUG,
                event = "ragpipe.cancelled",
                stage = stage,
                where_ = "send",
                "ragpipe.cancelled"
            );
            return Ok(());
        }

        if output.send(Bytes::from(buf)).await.is_err() {
            #[cfg(feature = "tracing")]
            tracing::event!(
                tracing::Level::INFO,
                event = "ragpipe.downstream.closed",
                stage = stage,
                "ragpipe.downstream.closed"
            );
            return Ok(());
        }
        Ok(())
    }
}
