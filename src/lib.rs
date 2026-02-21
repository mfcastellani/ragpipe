//! # ragpipe
//!
//! **Streaming-first RAG pipelines in Rust.**
//!
//! `ragpipe` is a Rust crate for building **memory-efficient**, **backpressure-aware**
//! and **cancellable** pipelines for Retrieval-Augmented Generation (RAG) ingestion.
//!
//! It is designed for production constraints:
//!
//! - large datasets (GBs, not MBs)
//! - bounded memory
//! - async execution
//! - graceful shutdown
//! - composable pipeline stages
//!
//! > No “load everything into a Vec and pray”.
//! > `ragpipe` streams everything.
//!
//! ---
//!
//! ## Core Model
//!
//! A pipeline is a chain of stages:
//!
//! ```text
//! Source → Decode → Chunk → Embed → Store
//! ```
//!
//! Each stage implements the [`Pipe`] trait and communicates through bounded Tokio channels.
//!
//! ---
//!
//! ## Getting Started
//!
//! A minimal streaming pipeline:
//!
//! ```no_run
//! use ragpipe::pipeline::chain::PipeExt;
//! use ragpipe::pipeline::runtime::Runtime;
//!
//! use ragpipe::source::fs::FsSource;
//! use ragpipe::chunk::tokens::TokenChunker;
//! use ragpipe::store::debug::DebugSink;
//!
//! #[tokio::main]
//! async fn main() -> ragpipe::error::Result<()> {
//!     let pipe = FsSource::new("big.txt")
//!         .pipe(TokenChunker::new(512))
//!         .pipe(DebugSink);
//!
//!     let rt = Runtime::new().buffer(128);
//!
//!     let (tx, _rx, _cancel, handle) = rt.spawn(pipe);
//!
//!     // Start the source
//!     tx.send(()).await.unwrap();
//!     drop(tx);
//!
//!     // Wait for completion
//!     handle.await??;
//!
//!     Ok(())
//! }
//! ```
//!
//! ---
//!
//! ## Common Patterns
//!
//! ### Retry Transient Errors
//!
//! ```no_run
//! use ragpipe::pipeline::chain::PipeExt;
//! use ragpipe::pipeline::retry::RetryPolicy;
//!
//! # fn example<S>(source: S)
//! # where
//! #   S: ragpipe::pipeline::pipe::Pipe<(), u32> + Send + Sync + 'static,
//! # {
//! let retry = RetryPolicy::new(4)
//!     .base_delay(std::time::Duration::from_millis(25))
//!     .max_delay(std::time::Duration::from_secs(1))
//!     .retry_if(|err| matches!(err, ragpipe::error::Error::Pipeline { .. }));
//!
//! let pipeline = source
//!     .try_map("enrich", |x| async move {
//!         if x % 2 == 0 {
//!             Err(ragpipe::error::Error::pipeline("transient"))
//!         } else {
//!             Ok::<u32, ragpipe::error::Error>(x + 1)
//!         }
//!     })
//!     .with_retry(retry);
//! # let _ = pipeline;
//! # }
//! ```
//!
//! ### Skip Bad Items And Continue
//!
//! ```no_run
//! use ragpipe::pipeline::chain::PipeExt;
//! use ragpipe::pipeline::retry::{ErrorAction, RetryPolicy};
//!
//! # fn example<S>(source: S)
//! # where
//! #   S: ragpipe::pipeline::pipe::Pipe<(), u32> + Send + Sync + 'static,
//! # {
//! let pipeline = source
//!     .try_map("parse", |x| async move {
//!         if x == 0 {
//!             Err(ragpipe::error::Error::pipeline("bad-item"))
//!         } else {
//!             Ok::<u32, ragpipe::error::Error>(x)
//!         }
//!     })
//!     .with_retry(RetryPolicy::new(3))
//!     .on_error(|ctx| {
//!         if matches!(ctx.error, ragpipe::error::Error::Pipeline { context: "bad-item" }) {
//!             ErrorAction::Skip
//!         } else {
//!             ErrorAction::Fail(ragpipe::error::Error::stage(ctx.stage, "unexpected"))
//!         }
//!     });
//! # let _ = pipeline;
//! # }
//! ```
//!
//! ### Fail Fast On Specific Errors
//!
//! ```no_run
//! use ragpipe::pipeline::chain::PipeExt;
//! use ragpipe::pipeline::retry::{ErrorAction, RetryPolicy};
//!
//! # fn example<S>(source: S)
//! # where
//! #   S: ragpipe::pipeline::pipe::Pipe<(), u32> + Send + Sync + 'static,
//! # {
//! let pipeline = source
//!     .try_map("critical", |x| async move {
//!         Err::<u32, ragpipe::error::Error>(ragpipe::error::Error::pipeline("fatal"))
//!     })
//!     .with_retry(RetryPolicy::new(5).retry_if(|_| true))
//!     .on_error(|ctx| {
//!         if matches!(ctx.error, ragpipe::error::Error::Pipeline { context: "fatal" }) {
//!             ErrorAction::Fail(ragpipe::error::Error::stage(ctx.stage, "fail-fast"))
//!         } else {
//!             ErrorAction::Retry
//!         }
//!     });
//! # let _ = pipeline;
//! # }
//! ```
//!
//! ---
//!
//! ## Built-in Adapters
//!
//! `ragpipe` includes functional-style adapters:
//!
//! ```no_run
//! use ragpipe::pipeline::chain::PipeExt;
//! use ragpipe::pipeline::retry::RetryPolicy;
//!
//! # fn example<S>(source: S)
//! # where
//! #   S: ragpipe::pipeline::pipe::Pipe<(), u32> + Send + Sync + 'static,
//! # {
//! let retry = RetryPolicy::new(3)
//!     .base_delay(std::time::Duration::from_millis(10))
//!     .max_delay(std::time::Duration::from_millis(200))
//!     .retry_if(|err| matches!(err, ragpipe::error::Error::Pipeline { .. }));
//!
//! let pipeline = source
//!     .map(|x| x * 10)
//!     .filter(|x| *x > 20)
//!     .try_map("enrich", |x| async move { Ok::<u32, ragpipe::error::Error>(x + 1) })
//!     .with_retry(retry)
//!     .inspect(|x| println!("value = {x}"));
//! # }
//! ```
//!
//! `RetryPolicy::new(max_attempts)` retries nothing by default.
//! Configure retryability explicitly via `.retry_if(...)`.
//!
//! `try_map` consumes each item and may require `Clone` when retries are enabled.
//! For large items, prefer `try_map_ref` to avoid `Clone` during retries:
//!
//! ```no_run
//! use ragpipe::pipeline::chain::PipeExt;
//! # fn example<S>(source: S)
//! # where
//! #   S: ragpipe::pipeline::pipe::Pipe<(), Vec<u8>> + Send + Sync + 'static,
//! # {
//! let pipeline = source
//!     .try_map_ref("validate", |bytes| {
//!         std::future::ready(Ok::<usize, ragpipe::error::Error>(bytes.len()))
//!     });
//! # let _ = pipeline;
//! # }
//! ```
//!
//! ---
//!
//! ## Error Handling Contract
//!
//! `try_map` and `try_map_ref` support `.on_error(...)` for per-item behavior:
//!
//! - `ErrorAction::Retry`: retry the current item (bounded by `max_attempts`)
//! - `ErrorAction::Skip`: drop the current item and continue
//! - `ErrorAction::Fail(err)`: fail the stage immediately with `err`
//!
//! If a handler is set, its action overrides the policy's default retry/fail
//! decision. `max_attempts` is still enforced.
//!
//! ---
//!
//! ## Backpressure Explained
//!
//! `ragpipe` uses bounded Tokio channels between stages.
//!
//! - If downstream is slower, upstream `send()` naturally waits.
//! - This keeps memory bounded to channel capacity plus in-flight work.
//! - You can tune pressure with [`Runtime::buffer`](pipeline::runtime::Runtime::buffer).
//! - If downstream is closed, stages exit gracefully instead of failing.
//!
//! ---
//!
//! ## Sink Pipelines
//!
//! When the last stage of a pipeline outputs `()`, use [`Runtime::spawn_sink`]
//! instead of [`Runtime::spawn`] to avoid manually draining the terminal channel:
//!
//! ```no_run
//! use ragpipe::pipeline::chain::PipeExt;
//! use ragpipe::pipeline::runtime::Runtime;
//! use ragpipe::source::fs::FsSource;
//! use ragpipe::store::debug::DebugSink;
//!
//! #[tokio::main]
//! async fn main() -> ragpipe::error::Result<()> {
//!     let pipe = FsSource::new("big.txt").pipe(DebugSink);
//!
//!     let rt = Runtime::new();
//!     let (tx, _cancel, handle) = rt.spawn_sink(pipe);
//!
//!     tx.send(()).await.unwrap();
//!     drop(tx);
//!     handle.await??;
//!     Ok(())
//! }
//! ```
//!
//! ---
//!
//! ## Per-Stage Tuning
//!
//! ### Buffer overrides
//!
//! Control the intermediate channel buffer for a single named stage without
//! changing the global default:
//!
//! ```no_run
//! use ragpipe::pipeline::runtime::Runtime;
//! # fn example<P>(pipe: P)
//! # where P: ragpipe::pipeline::pipe::Pipe<(), ()> + Send + Sync + 'static
//! # {
//! let rt = Runtime::new()
//!     .buffer(128)              // global default
//!     .buffer_stage("embed", 8); // narrow buffer into the slow stage
//! # let _ = rt.spawn_sink(pipe);
//! # }
//! ```
//!
//! ### Concurrent workers
//!
//! Run multiple async workers in parallel for `try_map` or `try_map_ref` stages:
//!
//! ```no_run
//! use ragpipe::pipeline::runtime::Runtime;
//! # fn example<P>(pipe: P)
//! # where P: ragpipe::pipeline::pipe::Pipe<(), ()> + Send + Sync + 'static
//! # {
//! let rt = Runtime::new()
//!     .concurrency_stage("embed", 4); // 4 parallel embed workers
//! # let _ = rt.spawn_sink(pipe);
//! # }
//! ```
//!
//! **Note**: output ordering is **not** preserved when `workers > 1`.
//! Concurrency has no effect on `map`, `filter`, `inspect`, or custom
//! [`Pipe`] implementations.
//!
//! ---
//!
//! ## Cancellation Semantics
//!
//! Pipelines support graceful cancellation via [`CancelToken`].
//!
//! ```no_run
//! use ragpipe::pipeline::runtime::Runtime;
//!
//! # async fn demo<P>(pipe: P) -> ragpipe::error::Result<()>
//! # where
//! #   P: ragpipe::pipeline::pipe::Pipe<(), ()> + Send + Sync + 'static,
//! # {
//! let rt = Runtime::new();
//! let (tx, _rx, cancel, handle) = rt.spawn(pipe);
//!
//! tx.send(()).await.unwrap();
//!
//! // Stop everything
//! cancel.cancel();
//!
//! handle.await??;
//! # Ok(())
//! # }
//! ```
//!
//! Cancellation rules:
//!
//! - Stages should check/await cancellation and exit quickly.
//! - Retries stop when cancelled, including during backoff sleep.
//! - Cancellation is cooperative; no task is force-killed.
//!
//! ---
//!
//! ## Writing Your Own Pipe
//!
//! Implement [`Pipe`] when you need a custom stage:
//!
//! ```no_run
//! use async_trait::async_trait;
//! use ragpipe::error::Result;
//! use ragpipe::pipeline::cancel::CancelToken;
//! use ragpipe::pipeline::pipe::Pipe;
//! use tokio::sync::mpsc::{Receiver, Sender};
//!
//! struct Uppercase;
//!
//! #[async_trait]
//! impl Pipe<String, String> for Uppercase {
//!     fn stage_name(&self) -> &'static str {
//!         "uppercase"
//!     }
//!
//!     async fn process(
//!         &self,
//!         mut input: Receiver<String>,
//!         output: Sender<String>,
//!         _buffer: usize,
//!         cancel: CancelToken,
//!     ) -> Result<()> {
//!         loop {
//!             tokio::select! {
//!                 _ = cancel.cancelled() => break,
//!                 msg = input.recv() => {
//!                     let Some(v) = msg else { break; };
//!                     if output.send(v.to_uppercase()).await.is_err() {
//!                         break;
//!                     }
//!                 }
//!             }
//!         }
//!         Ok(())
//!     }
//! }
//! ```
//!
//! Guidelines:
//!
//! - Treat `output.send(...).await.is_err()` as graceful downstream shutdown.
//! - Respect cancellation in receive loops and long operations.
//! - Avoid unbounded buffering inside stages.
//!
//! ---
//!
//! ## API Contracts
//!
//! - Bounded memory/backpressure: stages communicate over bounded Tokio channels.
//! - Cancellation: stages stop promptly when cancelled; no further retries run.
//! - Retry defaults: `RetryPolicy::new(max_attempts)` retries nothing unless
//!   `retry_if(...)` is configured.
//! - Downstream closed: if a stage cannot send because downstream is closed, it
//!   exits gracefully without error.
//!
//! ---
//!
//! ## Observability
//!
//! Enable tracing instrumentation with:
//!
//! ```toml
//! ragpipe = { version = "0.2", features = ["tracing"] }
//! ```
//!
//! Minimal subscriber setup:
//!
//! ```ignore
//! use tracing_subscriber::fmt;
//!
//! fn main() {
//!     fmt()
//!         .with_target(false)
//!         .with_env_filter("ragpipe=info")
//!         .init();
//! }
//! ```
//!
//! `ragpipe` emits structured spans/events such as `ragpipe.stage`,
//! `ragpipe.retry.attempt_failed`, `ragpipe.retry.sleep`,
//! `ragpipe.retry.exhausted`, `ragpipe.error_handler.action`,
//! `ragpipe.downstream.closed`, and `ragpipe.cancelled`.
//!
//! ---
//!
//! ## Feature Flags
//!
//! - `tracing` *(default)*: enables optional tracing spans/logging.
//! - `ndjson`: enables NDJSON streaming decoder [`ndjson::NdjsonDecoder`].
//!
//! ---
//!
//! ## Roadmap
//!
//! Upcoming pipeline integrations:
//!
//! - NDJSON streaming ingestion
//! - S3 sources
//! - Embedding providers (OpenAI, local models)
//! - Vector stores (Qdrant, pgvector)
//!
//! ---
//!
//! ## Philosophy
//!
//! `ragpipe` is built around a few principles:
//!
//! - Streaming is not optional
//! - Memory is a budget
//! - Backpressure is a feature
//! - RAG ingestion is systems programming
//!
//! Enjoy building production-grade AI pipelines in Rust 🦀🌊
//!
//! ---
//!
//! [`Pipe`]: pipeline::pipe::Pipe
//! [`CancelToken`]: pipeline::cancel::CancelToken

// Public modules
pub mod chunk;
pub mod error;
#[cfg(feature = "ndjson")]
pub mod ndjson;
pub mod pipeline;
pub mod source;
pub mod store;

pub mod prelude {
    //! Convenient imports for most `ragpipe` users.

    pub use crate::pipeline::cancel::CancelToken;
    pub use crate::pipeline::chain::PipeExt;
    pub use crate::pipeline::retry::RetryPolicy;
    pub use crate::pipeline::runtime::Runtime;
}
