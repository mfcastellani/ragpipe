//! examples/ndjson.rs
//!
//! Run with:
//!   cargo run --example ndjson --features ndjson
//!
//! This is a "fake" end-to-end pipeline:
//! VecSource<Bytes> -> NdjsonDecoder<Value> -> CollectSink<Value>

use async_trait::async_trait;
use bytes::Bytes;
use serde_json::Value;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::{Receiver, Sender};

use ragpipe::error::{Error, Result};
use ragpipe::ndjson::NdjsonDecoder;
use ragpipe::pipeline::cancel::CancelToken;
use ragpipe::pipeline::chain::PipeExt;
use ragpipe::pipeline::pipe::Pipe;
use ragpipe::pipeline::runtime::Runtime;

#[derive(Clone)]
struct VecSource<T> {
    items: Vec<T>,
}
impl<T> VecSource<T> {
    fn new(items: Vec<T>) -> Self {
        Self { items }
    }
}

#[async_trait]
impl<T> Pipe<(), T> for VecSource<T>
where
    T: Send + Sync + Clone + 'static,
{
    async fn process(
        &self,
        mut input: Receiver<()>,
        output: Sender<T>,
        _buffer: usize,
        cancel: CancelToken,
    ) -> Result<()> {
        // Wait for the pipeline "start" signal.
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            _ = input.recv() => {}
        }

        for item in self.items.clone() {
            if cancel.is_cancelled() {
                break;
            }

            // If downstream is closed, treat as graceful shutdown.
            if output.send(item).await.is_err() {
                break;
            }
        }

        Ok(())
    }
}

struct CollectSink<T> {
    out: Arc<Mutex<Vec<T>>>,
}
impl<T> CollectSink<T> {
    fn new(out: Arc<Mutex<Vec<T>>>) -> Self {
        Self { out }
    }
}

#[async_trait]
impl<T> Pipe<T, ()> for CollectSink<T>
where
    T: Send + Sync + 'static,
{
    async fn process(
        &self,
        mut input: Receiver<T>,
        _output: Sender<()>,
        _buffer: usize,
        cancel: CancelToken,
    ) -> Result<()> {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                msg = input.recv() => {
                    let Some(v) = msg else { break; };
                    self.out.lock().unwrap().push(v);
                }
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // NDJSON stream split across chunks (boundary test).
    let chunks = vec![
        Bytes::from_static(b"{\"id\":1,\"name\":\"a\"}\n{\"id\":2"),
        Bytes::from_static(b",\"name\":\"b\"}\n\n{\"id\":3,\"name\":\"c\"}"),
        Bytes::from_static(b"\n"),
    ];

    let collected: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = CollectSink::new(collected.clone());

    let pipeline = VecSource::new(chunks)
        .pipe(
            NdjsonDecoder::<Value>::new()
                .allow_empty_lines(true)
                .max_line_bytes(1024 * 1024),
        )
        .pipe(sink);

    let rt = Runtime::new().buffer(16);
    let (tx, mut rx, _cancel, handle) = rt.spawn(pipeline);

    // Drain the "final output receiver" (since our sink outputs `()`),
    // to prevent upstream send failures caused by a dropped receiver.
    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });

    // Start
    tx.send(())
        .await
        .map_err(|_| Error::pipeline("failed to start pipeline"))?;
    drop(tx);

    // Wait
    handle.await??;
    drain.await.map_err(|e| Error::Join(e))?;

    // Print collected values
    let out = collected.lock().unwrap();
    println!("Collected {} items:", out.len());
    for (i, v) in out.iter().enumerate() {
        println!("  {}: {}", i + 1, v);
    }

    Ok(())
}
