use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use ragpipe::error::{Error, Result};
use ragpipe::pipeline::cancel::CancelToken;
use ragpipe::pipeline::chain::PipeExt;
use ragpipe::pipeline::pipe::Pipe;
use ragpipe::pipeline::retry::RetryPolicy;
use ragpipe::pipeline::runtime::Runtime;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Notify;
mod common;
use common::CollectSink;

struct NonCloneItem {
    id: u32,
    payload: Vec<u8>,
}

struct QueueSource<T> {
    items: Mutex<VecDeque<T>>,
}

impl<T> QueueSource<T> {
    fn new(items: Vec<T>) -> Self {
        Self {
            items: Mutex::new(items.into()),
        }
    }
}

#[async_trait]
impl<T> Pipe<(), T> for QueueSource<T>
where
    T: Send + 'static,
{
    async fn process(
        &self,
        mut input: Receiver<()>,
        output: Sender<T>,
        _buffer: usize,
        cancel: CancelToken,
    ) -> Result<()> {
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            _ = input.recv() => {}
        }

        loop {
            if cancel.is_cancelled() {
                break;
            }

            let next = self.items.lock().expect("mutex poisoned").pop_front();
            let Some(item) = next else { break };
            if output.send(item).await.is_err() {
                break;
            }
        }

        Ok(())
    }
}

#[tokio::test]
async fn try_map_ref_retries_non_clone_item() -> Result<()> {
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_in_op = attempts.clone();
    let out = Arc::new(Mutex::new(Vec::<usize>::new()));
    let sink = CollectSink::new(out.clone());

    let retry = RetryPolicy::new(4)
        .base_delay(Duration::from_millis(1))
        .max_delay(Duration::from_millis(2));

    let source = QueueSource::new(vec![NonCloneItem {
        id: 7,
        payload: vec![1; 1024],
    }]);

    let pipe = source
        .try_map_ref("measure", move |item| {
            let attempts = attempts_in_op.clone();
            let size = item.payload.len();
            let id = item.id;
            async move {
                let current = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                if current <= 2 {
                    Err(Error::pipeline("transient"))
                } else {
                    Ok(size + id as usize)
                }
            }
        })
        .with_retry(retry)
        .pipe::<(), _>(sink);

    let rt = Runtime::new().buffer(16);
    let (tx, mut rx, _cancel, handle) = rt.spawn(pipe);
    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });

    tx.send(()).await.expect("start send failed");
    drop(tx);

    handle.await??;
    drain.await.expect("drain join failed");

    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    assert_eq!(&*out.lock().expect("mutex poisoned"), &[1031]);
    Ok(())
}

#[tokio::test]
async fn try_map_ref_cancellation_during_backoff_stops() -> Result<()> {
    let started = Arc::new(Notify::new());
    let started_in_op = started.clone();

    let retry = RetryPolicy::new(10)
        .base_delay(Duration::from_secs(10))
        .max_delay(Duration::from_secs(10));

    let source = QueueSource::new(vec![NonCloneItem {
        id: 1,
        payload: vec![9; 64],
    }]);

    let pipe = source
        .try_map_ref("always_fail", move |_item| {
            let started = started_in_op.clone();
            async move {
                started.notify_waiters();
                Err::<usize, Error>(Error::pipeline("transient"))
            }
        })
        .with_retry(retry);

    let rt = Runtime::new().buffer(16);
    let (tx, _rx, cancel, handle) = rt.spawn(pipe);
    tx.send(()).await.expect("start send failed");
    started.notified().await;
    cancel.cancel();
    drop(tx);

    let finished = tokio::time::timeout(Duration::from_millis(300), handle).await;
    assert!(finished.is_ok(), "pipeline did not stop after cancellation");
    finished.expect("timeout").expect("join failed")?;
    Ok(())
}
