use std::marker::PhantomData;

use async_trait::async_trait;
use bytes::Bytes;
use serde::de::DeserializeOwned;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::error::{Error, Result};
use crate::pipeline::cancel::CancelToken;
use crate::pipeline::pipe::Pipe;

const DEFAULT_MAX_LINE_BYTES: usize = 1024 * 1024;

/// Streaming NDJSON decoder.
///
/// Accepts `Bytes` chunks and emits one deserialized item per line.
/// Lines can cross chunk boundaries.
pub struct NdjsonDecoder<T> {
    max_line_bytes: usize,
    allow_empty_lines: bool,
    _marker: PhantomData<fn() -> T>,
}

impl<T> NdjsonDecoder<T> {
    /// Create a decoder with default limits.
    pub fn new() -> Self {
        Self {
            max_line_bytes: DEFAULT_MAX_LINE_BYTES,
            allow_empty_lines: false,
            _marker: PhantomData,
        }
    }

    /// Maximum number of bytes allowed for a single NDJSON line.
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

impl<T> Default for NdjsonDecoder<T> {
    fn default() -> Self {
        Self::new()
    }
}

enum EmitOutcome {
    Continue,
    Stop,
}

impl<T> NdjsonDecoder<T>
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
                tracing::event!(tracing::Level::DEBUG, event = "ragpipe.cancelled", stage = "ndjson_decoder", where_ = "send", "ragpipe.cancelled");
                Ok(EmitOutcome::Stop)
            },
            sent = output.send(value) => {
                if sent.is_err() {
                    #[cfg(feature = "tracing")]
                    tracing::event!(tracing::Level::INFO, event = "ragpipe.downstream.closed", stage = "ndjson_decoder", "ragpipe.downstream.closed");
                    Ok(EmitOutcome::Stop)
                } else {
                    Ok(EmitOutcome::Continue)
                }
            }
        }
    }
}

#[async_trait]
impl<T> Pipe<Bytes, T> for NdjsonDecoder<T>
where
    T: DeserializeOwned + Send + 'static,
{
    fn stage_name(&self) -> &'static str {
        "ndjson_decoder"
    }

    async fn process(
        &self,
        mut input: Receiver<Bytes>,
        mut output: Sender<T>,
        _buffer: usize,
        cancel: CancelToken,
    ) -> Result<()> {
        #[cfg(feature = "tracing")]
        let stage = self.stage_name();

        let mut line_buf = Vec::new();

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    #[cfg(feature = "tracing")]
                    tracing::event!(tracing::Level::DEBUG, event = "ragpipe.cancelled", stage = stage, where_ = "recv", "ragpipe.cancelled");
                    return Ok(())
                },
                msg = input.recv() => {
                    let Some(chunk) = msg else { break; };

                    for &byte in chunk.as_ref() {
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
