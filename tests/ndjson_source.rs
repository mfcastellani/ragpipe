#![cfg(feature = "ndjson")]

use ragpipe::error::Result;
use ragpipe::pipeline::runtime::Runtime;
use ragpipe::source::ndjson::NdjsonSource;
use serde::Deserialize;

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct Row {
    id: u32,
    name: String,
}

fn mock_path() -> String {
    format!("{}/mocks/data.ndjson", env!("CARGO_MANIFEST_DIR"))
}

async fn run_source(source: NdjsonSource<Row>) -> Result<Vec<Row>> {
    let rt = Runtime::new().buffer(8);
    let (tx, mut rx, _cancel, handle) = rt.spawn(source);

    tx.send(()).await.unwrap();
    drop(tx);

    let mut out = Vec::new();
    while let Some(item) = rx.recv().await {
        out.push(item);
    }

    handle.await??;
    Ok(out)
}

#[tokio::test]
async fn reads_mock_file() -> Result<()> {
    let out =
        run_source(NdjsonSource::<Row>::from_file(mock_path()).allow_empty_lines(true)).await?;
    assert_eq!(
        out,
        vec![
            Row {
                id: 1,
                name: "a".to_string(),
            },
            Row {
                id: 2,
                name: "b".to_string(),
            },
            Row {
                id: 3,
                name: "c".to_string(),
            },
        ]
    );
    Ok(())
}

#[tokio::test]
async fn reads_with_small_chunks() -> Result<()> {
    let out = run_source(
        NdjsonSource::<Row>::from_file(mock_path())
            .allow_empty_lines(true)
            .read_chunk_bytes(5),
    )
    .await?;
    assert_eq!(out.len(), 3);
    Ok(())
}

#[tokio::test]
async fn max_line_bytes_triggers_error() {
    let rt = Runtime::new().buffer(8);
    let (tx, _rx, _cancel, handle) = rt.spawn(
        NdjsonSource::<Row>::from_file(mock_path())
            .allow_empty_lines(true)
            .max_line_bytes(4),
    );

    tx.send(()).await.unwrap();
    drop(tx);

    let err = handle.await.unwrap().unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("stage `ndjson` error"));
    assert!(msg.contains("max_line_bytes"));
}
