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
//! ## Example
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
//!     .max_delay(std::time::Duration::from_millis(200));
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
//! ## Cancellation
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
