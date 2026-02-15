use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::error::Result;
use crate::pipeline::cancel::CancelToken;
use crate::pipeline::pipe::Pipe;

pub struct DebugSink;

#[async_trait]
impl Pipe<Bytes, ()> for DebugSink {
    fn stage_name(&self) -> &'static str {
        "debug_sink"
    }

    async fn process(
        &self,
        mut input: Receiver<Bytes>,
        _output: Sender<()>,
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
                    let Some(chunk) = msg else { break; };
                    println!("chunk: {} bytes", chunk.len());
                }
            }
        }
        Ok(())
    }
}

impl Default for DebugSink {
    fn default() -> Self {
        Self
    }
}
