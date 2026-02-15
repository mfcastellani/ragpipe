use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::error::Result;
use crate::pipeline::cancel::CancelToken;
use crate::pipeline::pipe::Pipe;

pub struct TokenChunker {
    size: usize,
}

impl TokenChunker {
    pub fn new(size: usize) -> Self {
        Self { size }
    }
}

#[async_trait]
impl Pipe<Bytes, Bytes> for TokenChunker {
    fn stage_name(&self) -> &'static str {
        "token_chunker"
    }

    async fn process(
        &self,
        mut input: Receiver<Bytes>,
        output: Sender<Bytes>,
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
                    let Some(data) = msg else { break; };
                    for chunk in data.chunks(self.size) {
                        if cancel.is_cancelled() {
                            #[cfg(feature = "tracing")]
                            tracing::event!(tracing::Level::DEBUG, event = "ragpipe.cancelled", stage = stage, where_ = "send", "ragpipe.cancelled");
                            break;
                        }
                        if output.send(Bytes::copy_from_slice(chunk)).await.is_err() {
                            #[cfg(feature = "tracing")]
                            tracing::event!(tracing::Level::INFO, event = "ragpipe.downstream.closed", stage = stage, "ragpipe.downstream.closed");
                            return Ok(());
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

impl Default for TokenChunker {
    fn default() -> Self {
        Self::new(1024)
    }
}
