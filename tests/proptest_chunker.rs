use std::sync::{Arc, Mutex};

use bytes::Bytes;
use proptest::prelude::*;
use ragpipe::chunk::tokens::TokenChunker;
use ragpipe::pipeline::chain::PipeExt;
use ragpipe::pipeline::runtime::Runtime;

mod common;
use common::{CollectSink, VecSource};

fn run_chunker(input: Vec<u8>, chunk_size: usize) -> Vec<Bytes> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    rt.block_on(async move {
        let collected = Arc::new(Mutex::new(Vec::<Bytes>::new()));
        let sink = CollectSink::new(collected.clone());

        let pipe = VecSource::new(vec![Bytes::from(input)])
            .strict_downstream(true)
            .pipe::<Bytes, _>(TokenChunker::new(chunk_size))
            .pipe::<(), _>(sink);

        let runtime = Runtime::new().buffer(8);
        let (tx, mut rx, _cancel, handle) = runtime.spawn(pipe);
        let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });

        tx.send(()).await.expect("start send failed");
        drop(tx);

        handle.await.expect("join failed").expect("pipeline failed");
        drain.await.expect("drain join failed");
        let out = collected.lock().expect("mutex poisoned").clone();
        out
    })
}

proptest! {
    #[test]
    fn chunker_is_lossless_and_bounded(
        input in proptest::collection::vec(any::<u8>(), 0..4096),
        chunk_size in 1usize..128
    ) {
        let chunks = run_chunker(input.clone(), chunk_size);

        let mut roundtrip = Vec::new();
        for chunk in &chunks {
            prop_assert!(chunk.len() <= chunk_size);
            roundtrip.extend_from_slice(chunk);
        }

        if input.is_empty() {
            prop_assert!(chunks.is_empty());
        }
        prop_assert_eq!(roundtrip, input);
    }

    #[test]
    fn chunker_handles_unicode_text_roundtrip(
        text in proptest::collection::vec(any::<char>(), 0..1024).prop_map(|chars| chars.into_iter().collect::<String>()),
        chunk_size in 1usize..64
    ) {
        let bytes = text.into_bytes();
        let chunks = run_chunker(bytes.clone(), chunk_size);

        let mut roundtrip = Vec::new();
        for chunk in &chunks {
            prop_assert!(chunk.len() <= chunk_size);
            roundtrip.extend_from_slice(chunk);
        }
        prop_assert_eq!(roundtrip, bytes);
    }
}

#[test]
fn chunker_edge_case_size_one() {
    let input = b"abcdef".to_vec();
    let chunks = run_chunker(input, 1);
    assert_eq!(chunks.len(), 6);
    assert!(chunks.iter().all(|c| c.len() == 1));
}

#[test]
fn chunker_edge_case_large_input() {
    let input = vec![42_u8; 32 * 1024];
    let chunks = run_chunker(input.clone(), 257);
    let total: usize = chunks.iter().map(|c| c.len()).sum();
    assert_eq!(total, input.len());
    assert!(chunks.iter().all(|c| c.len() <= 257));
}
