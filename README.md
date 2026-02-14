# ragpipe

Streaming-first RAG pipelines in Rust.

[![CI](https://github.com/mfcastellani/ragpipe/actions/workflows/ci.yml/badge.svg)](https://github.com/mfcastellani/ragpipe/actions/workflows/ci.yml)


`ragpipe` is a Rust crate for building memory-efficient, backpressure-aware, and cancellable pipelines for real-world Retrieval-Augmented Generation (RAG) ingestion.

It is designed for production constraints:

- large datasets
- bounded memory
- slow networks
- async execution
- graceful shutdown
- composable stages

> No “load everything into a Vec and pray”.
>
> ragpipe streams everything.

## Why ragpipe?

Most RAG tooling assumes:

- small inputs
- unlimited RAM
- synchronous workflows
- Python-style batch processing

But real ingestion pipelines look like this:

- JSON feeds with millions of records
- PDFs stored in S3
- embeddings computed slowly
- vector stores with rate limits
- memory budgets that actually matter

ragpipe exists to make streaming RAG ingestion feel natural in Rust.

## Features

- Streaming by default
- Bounded channels with backpressure
- Composable pipeline stages
- Async end-to-end (tokio)
- Cancellation support
- Adapters like map, filter, inspect, try_map, try_map_ref
- Per-item retry policies (backoff, max delay, optional jitter, retry predicate)
- Production-oriented design (no hidden buffering)

Quick Example

```rust
use ragpipe::pipeline::chain::PipeExt;
use ragpipe::pipeline::runtime::Runtime;

use ragpipe::source::fs::FsSource;
use ragpipe::chunk::tokens::TokenChunker;
use ragpipe::store::debug::DebugSink;

#[tokio::main]
async fn main() -> ragpipe::error::Result<()> {
    let pipeline = FsSource::new("big.txt")
        .pipe(TokenChunker::new(512))
        .pipe(DebugSink);

    let rt = Runtime::new().buffer(128);

    let (tx, _rx, cancel, handle) = rt.spawn(pipeline);

    // Start the source
    tx.send(()).await.unwrap();
    drop(tx);

    // Wait for completion
    handle.await??;

    Ok(())
}
```

## Pipeline Model

A pipeline is a chain of stages:

```
Source → Decode → Chunk → Embed → Store
```

Each stage is a Pipe:

```
#[async_trait]
pub trait Pipe<I, O>: Send + Sync {
    async fn process(
        &self,
        input: Receiver<I>,
        output: Sender<O>,
        buffer: usize,
        cancel: CancelToken,
    ) -> Result<()>;
}
```

Stages communicate through bounded Tokio channels, providing:

- streaming semantics
- natural backpressure
- predictable memory usage

## Built-in Adapters

ragpipe provides functional-style adapters:

### map

```rust
source.map(|x| x * 10)
```

### filter

```rust
source.filter(|x| x.is_valid())
```

### inspect

```rust
source.inspect(|chunk| println!("chunk size = {}", chunk.len()))
```

### try_map + retry

```rust
use std::time::Duration;
use ragpipe::pipeline::retry::RetryPolicy;

let retry = RetryPolicy::new(4)
    .base_delay(Duration::from_millis(25))
    .max_delay(Duration::from_secs(1))
    .with_jitter(Duration::from_millis(20))
    // Prefer structured error predicates over string matching:
    .retry_if(|err| matches!(err, ragpipe::error::Error::Pipeline { .. }));

let pipeline = source
    .try_map("embed", |chunk| async move { embed(chunk).await })
    .with_retry(retry)
    .on_error(|ctx| {
        eprintln!(
            "stage={} attempt={} err={}",
            ctx.stage, ctx.attempt, ctx.error
        );

    ragpipe::pipeline::retry::ErrorAction::Retry
});
```


### try_map_ref + retry (zero-copy)

Use this for large items when retries are enabled, to avoid `Clone` on every retry:

```rust
let pipeline = source
    .try_map_ref("embed", |chunk| std::future::ready(embed_ref(chunk)))
    .with_retry(retry);
```

These adapters are cancellable and streaming-safe.

## Cancellation

Every pipeline receives a CancelToken:

```rust
let (tx, rx, cancel, handle) = rt.spawn(pipeline);
cancel.cancel(); // stops the entire pipeline
```

Stages are expected to exit gracefully when cancellation occurs.

## Roadmap

### v0.1 (current)

- core streaming pipeline engine
- chaining
- cancellation
- adapters (map/filter/inspect)
- basic sources and sinks

### v0.2

- S3 streaming source
- NDJSON ingestion
- embedding providers (OpenAI / local)

### v0.3

- Vector store backends (Qdrant, pgvector)
- retry + rate limiting
- metrics + tracing spans

### v1.0

- stable public API
- production hardening
- benchmarks + real ingestion examples

## Philosophy

ragpipe is built around a few principles:

- Streaming is not optional
- Memory is a budget
- Backpressure is a feature
- RAG ingestion is systems programming
- Rust deserves production-grade AI infrastructure

## Contributing

Contributions are very welcome. If you're interested in:

- streaming ingestion
- async pipelines
- RAG infrastructure
- embedding/vector store integration

…this project is for you.

Open an issue or PR and let's build something useful.

## License

Licensed under either of `Apache License, Version 2.0` and/or `MIT license` at your option.

## Author

Created by Marcelo Castellani (Rust community contributor, systems engineer, and writer).
