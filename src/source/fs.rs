use async_trait::async_trait;
use bytes::Bytes;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::error::{Error, Result};
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
    async fn process(
        &self,
        mut input: Receiver<()>,
        output: Sender<Bytes>,
        _buffer: usize,
        cancel: CancelToken,
    ) -> Result<()> {
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            _ = input.recv() => {}
        }

        let mut file = File::open(&self.path).await?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).await?;

        if cancel.is_cancelled() {
            return Ok(());
        }

        if output.send(Bytes::from(buf)).await.is_err() {
            return Err(Error::pipeline("output channel closed"));
        }
        Ok(())
    }
}
