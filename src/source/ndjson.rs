use std::marker::PhantomData;

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::error::{Error, Result};
use crate::pipeline::cancel::CancelToken;
use crate::pipeline::pipe::Pipe;

const DEFAULT_MAX_LINE_BYTES: usize = 1024 * 1024;
const DEFAULT_READ_CHUNK_BYTES: usize = 8 * 1024;

/// Stream NDJSON records directly from a file path.
///
/// This source reads the file in chunks, handles line boundaries across reads,
/// and deserializes each line as one JSON value.
pub struct NdjsonSource<T> {
    path: String,
    read_chunk_bytes: usize,
    max_line_bytes: usize,
    allow_empty_lines: bool,
    _marker: PhantomData<fn() -> T>,
}

impl<T> NdjsonSource<T> {
    /// Create a source from a file path.
    pub fn from_file(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            read_chunk_bytes: DEFAULT_READ_CHUNK_BYTES,
            max_line_bytes: DEFAULT_MAX_LINE_BYTES,
            allow_empty_lines: false,
            _marker: PhantomData,
        }
    }

    /// Number of bytes read per filesystem call.
    pub fn read_chunk_bytes(mut self, n: usize) -> Self {
        self.read_chunk_bytes = n.max(1);
        self
    }

    /// Maximum number of bytes allowed for one NDJSON line.
    pub fn max_line_bytes(mut self, n: usize) -> Self {
        self.max_line_bytes = n;
        self
    }

    /// Whether blank lines should be ignored.
    pub fn allow_empty_lines(mut self, yes: bool) -> Self {
        self.allow_empty_lines = yes;
        self
    }
}

enum EmitOutcome {
    Continue,
    Stop,
}

impl<T> NdjsonSource<T>
where
    T: DeserializeOwned + Send + 'static,
{
    async fn emit_line(
        &self,
        line: &[u8],
        output: &mut Sender<T>,
        cancel: &CancelToken,
    ) -> Result<EmitOutcome> {
        let line = strip_cr(line);

        if line.is_empty() && self.allow_empty_lines {
            return Ok(EmitOutcome::Continue);
        }

        let value = serde_json::from_slice::<T>(line).map_err(|err| {
            Error::stage(
                "ndjson",
                format!(
                    "failed to parse line ({} bytes, preview: {:?}): {}",
                    line.len(),
                    preview(line),
                    err
                ),
            )
        })?;

        tokio::select! {
            _ = cancel.cancelled() => {
                #[cfg(feature = "tracing")]
                tracing::event!(tracing::Level::DEBUG, event = "ragpipe.cancelled", stage = "ndjson_source", where_ = "send", "ragpipe.cancelled");
                Ok(EmitOutcome::Stop)
            },
            sent = output.send(value) => {
                if sent.is_err() {
                    #[cfg(feature = "tracing")]
                    tracing::event!(tracing::Level::INFO, event = "ragpipe.downstream.closed", stage = "ndjson_source", "ragpipe.downstream.closed");
                    Ok(EmitOutcome::Stop)
                } else {
                    Ok(EmitOutcome::Continue)
                }
            }
        }
    }
}

#[async_trait]
impl<T> Pipe<(), T> for NdjsonSource<T>
where
    T: DeserializeOwned + Send + 'static,
{
    fn stage_name(&self) -> &'static str {
        "ndjson_source"
    }

    async fn process(
        &self,
        mut input: Receiver<()>,
        mut output: Sender<T>,
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

        let mut file = File::open(&self.path)
            .await
            .map_err(|e| Error::stage_source("ndjson", e.into()))?;
        let mut read_buf = vec![0_u8; self.read_chunk_bytes];
        let mut line_buf = Vec::<u8>::new();

        loop {
            let n = tokio::select! {
                _ = cancel.cancelled() => {
                    #[cfg(feature = "tracing")]
                    tracing::event!(tracing::Level::DEBUG, event = "ragpipe.cancelled", stage = stage, where_ = "read", "ragpipe.cancelled");
                    return Ok(());
                },
                read = file.read(&mut read_buf) => {
                    read.map_err(|e| Error::stage_source("ndjson", e.into()))?
                }
            };

            if n == 0 {
                break;
            }

            for &byte in &read_buf[..n] {
                if byte == b'\n' {
                    match self.emit_line(&line_buf, &mut output, &cancel).await? {
                        EmitOutcome::Continue => {
                            line_buf.clear();
                        }
                        EmitOutcome::Stop => return Ok(()),
                    }
                    continue;
                }

                line_buf.push(byte);
                if line_buf.len() > self.max_line_bytes {
                    return Err(Error::stage(
                        "ndjson",
                        format!(
                            "line exceeded max_line_bytes ({} > {})",
                            line_buf.len(),
                            self.max_line_bytes
                        ),
                    ));
                }
            }
        }

        if !line_buf.is_empty() {
            let _ = self.emit_line(&line_buf, &mut output, &cancel).await?;
        }

        Ok(())
    }
}

fn strip_cr(line: &[u8]) -> &[u8] {
    if let Some(stripped) = line.strip_suffix(b"\r") {
        stripped
    } else {
        line
    }
}

fn preview(line: &[u8]) -> String {
    const PREVIEW_LEN: usize = 80;
    let text = String::from_utf8_lossy(line);
    let escaped = text.replace('\n', "\\n").replace('\r', "\\r");
    let mut short = escaped.chars().take(PREVIEW_LEN).collect::<String>();
    if escaped.chars().count() > PREVIEW_LEN {
        short.push_str("...");
    }
    short
}
