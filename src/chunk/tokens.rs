use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::error::{Error, Result};
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
    async fn process(
        &self,
        mut input: Receiver<Bytes>,
        output: Sender<Bytes>,
        _buffer: usize,
        cancel: CancelToken,
    ) -> Result<()> {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                msg = input.recv() => {
                    let Some(data) = msg else { break; };
                    for chunk in data.chunks(self.size) {
                        if cancel.is_cancelled() { break; }
                        if output.send(Bytes::copy_from_slice(chunk)).await.is_err() {
                            return Err(Error::pipeline("output channel closed"));
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
