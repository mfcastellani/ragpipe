use std::sync::{Arc, Mutex};

use bytes::Bytes;
use ragpipe::error::Result;
use ragpipe::pipeline::chain::PipeExt;
use ragpipe::pipeline::runtime::Runtime;
use ragpipe::source::fs::FsSource;

mod common;
use common::CollectSink;

#[tokio::test]
async fn fs_source_nonexistent_file() {
    let pipe = FsSource::new("/nonexistent/path/that/does/not/exist.txt");

    let rt = Runtime::new().buffer(8);
    let (tx, _rx, _cancel, handle) = rt.spawn(pipe);

    tx.send(()).await.unwrap();
    drop(tx);

    let result = handle.await.unwrap();
    assert!(result.is_err(), "should fail on nonexistent file");

    let err = result.unwrap_err();
    let msg = format!("{err}");
    // Should contain IO error information
    assert!(
        msg.contains("io error") || msg.contains("No such file"),
        "error message: {msg}"
    );
}

#[tokio::test]
async fn fs_source_directory_instead_of_file() {
    let dir = std::env::temp_dir();

    let pipe = FsSource::new(dir.to_string_lossy().to_string());

    let rt = Runtime::new().buffer(8);
    let (tx, _rx, _cancel, handle) = rt.spawn(pipe);

    tx.send(()).await.unwrap();
    drop(tx);

    let result = handle.await.unwrap();
    assert!(
        result.is_err(),
        "should fail when trying to read a directory"
    );

    let err = result.unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("io error") || msg.contains("Is a directory"),
        "error message: {msg}"
    );
}

#[tokio::test]
async fn fs_source_empty_file() -> Result<()> {
    let dir = std::env::temp_dir();
    let path = dir.join("ragpipe_empty_file_test.txt");
    std::fs::write(&path, b"").unwrap();

    let collected = Arc::new(Mutex::new(Vec::<Bytes>::new()));
    let sink = CollectSink::new(collected.clone());

    let pipe = FsSource::new(path.to_string_lossy().to_string()).pipe::<(), _>(sink);

    let rt = Runtime::new().buffer(8);
    let (tx, mut rx, _cancel, handle) = rt.spawn(pipe);

    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });

    tx.send(()).await.unwrap();
    drop(tx);

    handle.await??;
    drain.await.unwrap();

    let out = collected.lock().unwrap();
    assert_eq!(out.len(), 1);
    assert!(out[0].is_empty(), "empty file should produce empty Bytes");

    let _ = std::fs::remove_file(&path);
    Ok(())
}

#[tokio::test]
async fn fs_source_large_file() -> Result<()> {
    let dir = std::env::temp_dir();
    let path = dir.join("ragpipe_large_file_test.txt");
    let data = vec![42u8; 1024 * 1024]; // 1MB file
    std::fs::write(&path, &data).unwrap();

    let collected = Arc::new(Mutex::new(Vec::<Bytes>::new()));
    let sink = CollectSink::new(collected.clone());

    let pipe = FsSource::new(path.to_string_lossy().to_string()).pipe::<(), _>(sink);

    let rt = Runtime::new().buffer(8);
    let (tx, mut rx, _cancel, handle) = rt.spawn(pipe);

    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });

    tx.send(()).await.unwrap();
    drop(tx);

    handle.await??;
    drain.await.unwrap();

    let out = collected.lock().unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].len(), 1024 * 1024);

    let _ = std::fs::remove_file(&path);
    Ok(())
}
