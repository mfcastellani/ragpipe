#![cfg(feature = "ndjson")]

use bytes::Bytes;
use ragpipe::error::Result;
use ragpipe::ndjson::NdjsonDecoder;
use ragpipe::pipeline::runtime::Runtime;
use serde::Deserialize;
use serde_json::Value;
use tokio::time::{timeout, Duration};

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct Row {
    id: u32,
}

async fn run_decoder<T>(decoder: NdjsonDecoder<T>, chunks: Vec<Bytes>) -> Result<Vec<T>>
where
    T: serde::de::DeserializeOwned + Send + 'static,
{
    let rt = Runtime::new().buffer(8);
    let (tx, mut rx, _cancel, handle) = rt.spawn(decoder);

    for chunk in chunks {
        tx.send(chunk).await.unwrap();
    }
    drop(tx);

    handle.await??;

    let mut out = Vec::new();
    while let Some(item) = rx.recv().await {
        out.push(item);
    }
    Ok(out)
}

#[tokio::test]
async fn decodes_3_lines() -> Result<()> {
    let out = run_decoder(
        NdjsonDecoder::<Row>::new(),
        vec![Bytes::from_static(
            br#"{"id":1}
{"id":2}
{"id":3}
"#,
        )],
    )
    .await?;

    assert_eq!(out, vec![Row { id: 1 }, Row { id: 2 }, Row { id: 3 }]);
    Ok(())
}

#[tokio::test]
async fn handles_boundary_across_chunks() -> Result<()> {
    let out = run_decoder(
        NdjsonDecoder::<Row>::new(),
        vec![
            Bytes::from_static(br#"{"id":"#),
            Bytes::from_static(b"1}\n"),
            Bytes::from_static(br#"{"id":2}"#),
            Bytes::from_static(b"\n"),
            Bytes::from_static(br#"{"id":3}"#),
        ],
    )
    .await?;

    assert_eq!(out, vec![Row { id: 1 }, Row { id: 2 }, Row { id: 3 }]);
    Ok(())
}

#[tokio::test]
async fn last_line_without_newline() -> Result<()> {
    let out = run_decoder(
        NdjsonDecoder::<Row>::new(),
        vec![Bytes::from_static(
            br#"{"id":1}
{"id":2}"#,
        )],
    )
    .await?;

    assert_eq!(out, vec![Row { id: 1 }, Row { id: 2 }]);
    Ok(())
}

#[tokio::test]
async fn empty_lines_ignored() -> Result<()> {
    let out = run_decoder(
        NdjsonDecoder::<Row>::new().allow_empty_lines(true),
        vec![Bytes::from_static(
            br#"{"id":1}

{"id":2}

"#,
        )],
    )
    .await?;

    assert_eq!(out, vec![Row { id: 1 }, Row { id: 2 }]);
    Ok(())
}

#[tokio::test]
async fn max_line_bytes_triggers_error() {
    let rt = Runtime::new().buffer(8);
    let (tx, _rx, _cancel, handle) = rt.spawn(NdjsonDecoder::<Row>::new().max_line_bytes(4));

    tx.send(Bytes::from_static(
        br#"{"id":1}
"#,
    ))
    .await
    .unwrap();
    drop(tx);

    let err = handle.await.unwrap().unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("stage `ndjson` error"));
    assert!(msg.contains("max_line_bytes"));
}

#[tokio::test]
async fn cancellation_stops_pipeline() -> Result<()> {
    let rt = Runtime::new().buffer(1);
    let (tx, _rx, cancel, handle) = rt.spawn(NdjsonDecoder::<Value>::new());

    tx.send(Bytes::from_static(
        br#"{"id":1}
{"id":2}
{"id":3}
"#,
    ))
    .await
    .unwrap();

    tokio::task::yield_now().await;
    cancel.cancel();
    drop(tx);

    timeout(Duration::from_millis(500), handle)
        .await
        .expect("decoder should stop promptly after cancellation")??;
    Ok(())
}
