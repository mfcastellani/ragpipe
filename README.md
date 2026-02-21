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
- Optional NDJSON streaming decoder (`ndjson` feature)
- Adapters like map, filter, inspect, try_map, try_map_ref
- Per-item retry policies (backoff, max delay, optional jitter, retry predicate)
- Production-oriented design (no hidden buffering)

### NDJSON Decoder

Enable feature `ndjson` to decode line-delimited JSON from `Bytes` chunks without loading full files in memory:

```toml
ragpipe = { version = "0.2", features = ["ndjson"] }
```

Then use the `NdjsonDecoder` to decode chunks of JSON into `serde_json::Value`s:

```rust
use ragpipe::ndjson::NdjsonDecoder;

let decode = NdjsonDecoder::<serde_json::Value>::new()
    .max_line_bytes(1024 * 1024)
    .allow_empty_lines(true);
```

For file ingestion, use `NdjsonSource` directly:

```rust
use ragpipe::source::ndjson::NdjsonSource;

let source = NdjsonSource::<serde_json::Value>::from_file("mocks/data.ndjson")
    .allow_empty_lines(true);
```

#### Quick Example

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
use ragpipe::pipeline::retry::{RetryPolicy, ErrorAction};

let retry = RetryPolicy::new(4)
    .base_delay(Duration::from_millis(25))
    .max_delay(Duration::from_secs(1))
    .with_jitter(Duration::from_millis(20))
    // Retries are opt-in; configure retryability explicitly:
    .retry_if(|err| matches!(err, ragpipe::error::Error::Pipeline { .. }));

let pipeline = source
    .try_map("embed", |chunk| async move { embed(chunk).await })
    .with_retry(retry)
    .on_error(|ctx| {
        eprintln!(
        "stage={} attempt={} err={}",
        ctx.stage, ctx.attempt, ctx.error
    );

    ErrorAction::Retry
});
```

`RetryPolicy::new(max_attempts)` retries nothing by default until `.retry_if(...)` is set.

### try_map_ref + retry (zero-copy)

Use `try_map_ref` for large items when retries are enabled, to avoid `Clone` on every retry.
`try_map` consumes items and may require `Clone` to retry the same value:

```rust
let pipeline = source
    .try_map_ref("embed", |chunk| async move { embed_ref(chunk).await })
    .with_retry(retry);
```

These adapters are cancellable and streaming-safe.

### on_error semantics

`on_error` can decide per-item behavior:

- `ErrorAction::Retry`: retry the same item (still bounded by `max_attempts`)
- `ErrorAction::Skip`: drop current item and continue stream processing
- `ErrorAction::Fail(err)`: fail stage immediately with `err`

If an error handler is registered, its action overrides the policy's default retry/fail decision.
`max_attempts` remains enforced.

## Runtime Ergonomics

### spawn_sink

When the last stage of a pipeline outputs `()`, use `spawn_sink` to avoid
manually draining a `Receiver<()>`:

```rust
let (tx, _cancel, handle) = rt.spawn_sink(pipeline);

tx.send(()).await.unwrap();
drop(tx);
handle.await??;
```

### buffer_stage

Override the intermediate channel buffer for a specific named stage:

```rust
let rt = Runtime::new()
    .buffer(128)              // global default
    .buffer_stage("embed", 8); // narrow buffer into a slow stage
```

### concurrency_stage

Run multiple async workers in parallel for `try_map` or `try_map_ref` stages:

```rust
let rt = Runtime::new()
    .concurrency_stage("embed", 4); // 4 parallel embed workers
```

Output ordering is **not** preserved when `workers > 1`. This setting only
applies to `try_map` / `try_map_ref` stages — `map`, `filter`, `inspect`,
and custom `Pipe` implementations are unaffected.

All three options compose:

```rust
let rt = Runtime::new()
    .buffer(64)
    .buffer_stage("embed", 8)
    .concurrency_stage("embed", 4);

let (tx, _cancel, handle) = rt.spawn_sink(pipeline);
```

## Cancellation

Every pipeline receives a CancelToken:

```rust
let (tx, rx, cancel, handle) = rt.spawn(pipeline);
cancel.cancel(); // stops the entire pipeline
```

Stages are expected to exit gracefully when cancellation occurs.

## API Contracts

- Bounded memory and backpressure: stages communicate via bounded Tokio channels.
- Cancellation semantics: cancellation stops stages promptly and no further retries are attempted.
- Retry semantics: `RetryPolicy::new(max_attempts)` retries nothing by default unless `.retry_if(...)` is configured.
- Downstream-closed semantics: output channel close is treated as graceful shutdown (no error).

## Observability

Enable tracing instrumentation:

```toml
ragpipe = { version = "0.2", features = ["tracing"] }
```

Minimal setup with `tracing-subscriber`:

```rust
use tracing_subscriber::fmt;

fn main() {
    fmt()
        .with_target(false)
        .with_env_filter("ragpipe=info")
        .init();
}
```

With this enabled, ragpipe emits structured spans/events like:
- `ragpipe.stage`
- `ragpipe.retry.attempt_failed`
- `ragpipe.retry.sleep`
- `ragpipe.retry.exhausted`
- `ragpipe.error_handler.action`
- `ragpipe.downstream.closed`
- `ragpipe.cancelled`

## Roadmap

### v0.1

- ✅ core streaming pipeline engine 
- ✅ chaining
- ✅ cancellation
- ✅ adapters (map/filter/inspect)
- ✅ basic sources and sinks

### v0.2 (current)

- ✅ NDJSON ingestion
- ✅ `spawn_sink` (no manual drain)
- ✅ `buffer_stage` (per-stage channel tuning)
- ✅ `concurrency_stage` (parallel workers for try_map/try_map_ref)
- embedding providers (OpenAI / local)
- S3 streaming source

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
