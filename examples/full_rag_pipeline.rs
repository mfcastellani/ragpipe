//! Full RAG Pipeline Example
//!
//! Run with:
//!   cargo run --example full_rag_pipeline
//!
//! This example demonstrates a complete RAG ingestion pipeline:
//! FsSource → TokenChunker → MockEmbedder → MockVectorStore
//!
//! It showcases:
//! - Reading a file from disk
//! - Chunking text into manageable pieces
//! - Simulating embedding generation
//! - Storing embeddings in a mock vector database
//! - Error handling and retry logic
//! - Graceful cancellation

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use ragpipe::chunk::tokens::TokenChunker;
use ragpipe::error::{Error, Result};
use ragpipe::pipeline::cancel::CancelToken;
use ragpipe::pipeline::chain::PipeExt;
use ragpipe::pipeline::pipe::Pipe;
use ragpipe::pipeline::retry::RetryPolicy;
use ragpipe::pipeline::runtime::Runtime;
use ragpipe::source::fs::FsSource;
use tokio::sync::mpsc::{Receiver, Sender};

/// Represents a document chunk with metadata
#[derive(Debug, Clone)]
struct Chunk {
    id: usize,
    content: String,
    byte_size: usize,
}

/// Represents an embedded chunk ready for storage
#[derive(Debug, Clone)]
struct EmbeddedChunk {
    chunk: Chunk,
    embedding: Vec<f32>,
}

/// Mock embedder that simulates generating embeddings
struct MockEmbedder {
    dimension: usize,
}

impl MockEmbedder {
    fn new(dimension: usize) -> Self {
        Self { dimension }
    }

    fn embed(&self, text: &str) -> Vec<f32> {
        // Simple mock: use hash of text to generate deterministic embedding
        let hash = text
            .bytes()
            .fold(0u64, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u64));

        (0..self.dimension)
            .map(|i| {
                let seed = hash.wrapping_add(i as u64);
                ((seed % 1000) as f32) / 1000.0
            })
            .collect()
    }
}

#[async_trait]
impl Pipe<Chunk, EmbeddedChunk> for MockEmbedder {
    fn stage_name(&self) -> &'static str {
        "mock_embedder"
    }

    async fn process(
        &self,
        mut input: Receiver<Chunk>,
        output: Sender<EmbeddedChunk>,
        _buffer: usize,
        cancel: CancelToken,
    ) -> Result<()> {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                msg = input.recv() => {
                    let Some(chunk) = msg else { break; };

                    // Simulate embedding generation
                    let embedding = self.embed(&chunk.content);

                    let embedded = EmbeddedChunk { chunk, embedding };

                    if output.send(embedded).await.is_err() {
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}

/// Mock vector store that stores embeddings in memory
struct MockVectorStore {
    store: Arc<Mutex<HashMap<usize, EmbeddedChunk>>>,
}

impl MockVectorStore {
    fn new() -> Self {
        Self {
            store: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn get_store(&self) -> Arc<Mutex<HashMap<usize, EmbeddedChunk>>> {
        self.store.clone()
    }
}

#[async_trait]
impl Pipe<EmbeddedChunk, ()> for MockVectorStore {
    fn stage_name(&self) -> &'static str {
        "vector_store"
    }

    async fn process(
        &self,
        mut input: Receiver<EmbeddedChunk>,
        _output: Sender<()>,
        _buffer: usize,
        cancel: CancelToken,
    ) -> Result<()> {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                msg = input.recv() => {
                    let Some(embedded) = msg else { break; };

                    let chunk_id = embedded.chunk.id;
                    let byte_size = embedded.chunk.byte_size;
                    let embedding_len = embedded.embedding.len();

                    // Store in our mock database
                    self.store
                        .lock()
                        .unwrap()
                        .insert(chunk_id, embedded);

                    println!(
                        "✓ Stored chunk {} ({} bytes, embedding dim {})",
                        chunk_id,
                        byte_size,
                        embedding_len
                    );
                }
            }
        }
        Ok(())
    }
}

/// Converts raw bytes to structured chunks
struct BytesToChunks {
    chunk_id: Arc<Mutex<usize>>,
}

impl BytesToChunks {
    fn new() -> Self {
        Self {
            chunk_id: Arc::new(Mutex::new(0)),
        }
    }
}

#[async_trait]
impl Pipe<Bytes, Chunk> for BytesToChunks {
    fn stage_name(&self) -> &'static str {
        "bytes_to_chunks"
    }

    async fn process(
        &self,
        mut input: Receiver<Bytes>,
        output: Sender<Chunk>,
        _buffer: usize,
        cancel: CancelToken,
    ) -> Result<()> {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                msg = input.recv() => {
                    let Some(bytes) = msg else { break; };

                    let content = String::from_utf8_lossy(&bytes).to_string();
                    let byte_size = bytes.len();

                    let id = {
                        let mut id = self.chunk_id.lock().unwrap();
                        *id += 1;
                        *id
                    };

                    let chunk = Chunk {
                        id,
                        content,
                        byte_size,
                    };

                    if output.send(chunk).await.is_err() {
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("-> Starting RAG ingestion pipeline...\n");

    // Create a sample file
    let tmp = std::env::temp_dir();
    let file_path = tmp.join("ragpipe_rag_example.txt");
    let sample_text = "Rust is a systems programming language that runs blazingly fast, \
                       prevents segfaults, and guarantees thread safety. \
                       It accomplishes these goals without garbage collection. \
                       Rust has great documentation, a friendly compiler with useful error messages, \
                       and top-notch tooling.";
    std::fs::write(&file_path, sample_text)?;
    println!("-> Created sample file: {}", file_path.display());

    // Build the pipeline
    let vector_store = MockVectorStore::new();
    let store_handle = vector_store.get_store();

    let retry_policy = RetryPolicy::new(3)
        .base_delay(std::time::Duration::from_millis(100))
        .max_delay(std::time::Duration::from_secs(1))
        .retry_if(|err| matches!(err, Error::Pipeline { .. }));

    let pipeline = FsSource::new(file_path.to_string_lossy().to_string())
        .pipe(TokenChunker::new(64)) // 64-byte chunks
        .pipe(BytesToChunks::new())
        .pipe(MockEmbedder::new(384)) // 384-dimensional embeddings
        .try_map("validate", |embedded: EmbeddedChunk| async move {
            // Simulate validation that might fail
            if embedded.embedding.is_empty() {
                Err(Error::pipeline("empty embedding"))
            } else {
                Ok(embedded)
            }
        })
        .with_retry(retry_policy)
        .pipe(vector_store);

    println!("\n-> Pipeline configuration:");
    println!("   - Chunk size: 64 bytes");
    println!("   - Embedding dimension: 384");
    println!("   - Buffer size: 32");
    println!("   - Retry attempts: 3\n");

    let rt = Runtime::new().buffer(32);
    let (tx, mut rx, _cancel, handle) = rt.spawn(pipeline);

    // Drain the final output channel
    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });

    // Start the pipeline
    tx.send(())
        .await
        .map_err(|_| Error::pipeline("failed to start"))?;
    drop(tx);

    println!("-> Processing...\n");

    // Wait for completion
    handle.await??;
    drain.await.map_err(Error::Join)?;

    // Show results
    let stored = store_handle.lock().unwrap();
    println!("\n-> Pipeline completed successfully!");
    println!("-> Results:");
    println!("   - Total chunks stored: {}", stored.len());
    println!("   - Embedding dimension: 384");

    if let Some(first_chunk) = stored.values().next() {
        println!("\n-> Sample chunk:");
        println!("   ID: {}", first_chunk.chunk.id);
        println!("   Size: {} bytes", first_chunk.chunk.byte_size);
        println!(
            "   Content: {}...",
            first_chunk
                .chunk
                .content
                .chars()
                .take(50)
                .collect::<String>()
        );
        println!(
            "   Embedding preview: [{:.3}, {:.3}, {:.3}, ...]",
            first_chunk.embedding[0], first_chunk.embedding[1], first_chunk.embedding[2]
        );
    }

    // Cleanup
    let _ = std::fs::remove_file(file_path);

    Ok(())
}
