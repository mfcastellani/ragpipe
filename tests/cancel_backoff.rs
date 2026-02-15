use std::time::Duration;

use ragpipe::error::{Error, Result};
use ragpipe::pipeline::chain::PipeExt;
use ragpipe::pipeline::retry::RetryPolicy;
use ragpipe::pipeline::runtime::Runtime;
use tokio::sync::Notify;

mod common;
use common::VecSource;

#[tokio::test(start_paused = true)]
async fn cancel_during_retry_backoff_stops_without_advancing_time() -> Result<()> {
    let entered_backoff = std::sync::Arc::new(Notify::new());
    let entered_backoff_in_op = entered_backoff.clone();

    let retry = RetryPolicy::new(10)
        .base_delay(Duration::from_secs(60))
        .max_delay(Duration::from_secs(60))
        .retry_if(|_| true);

    let pipe = VecSource::new(vec![1u32])
        .strict_downstream(true)
        .try_map("cancel_backoff", move |_| {
            let entered_backoff = entered_backoff_in_op.clone();
            async move {
                entered_backoff.notify_waiters();
                Err::<u32, Error>(Error::pipeline("transient"))
            }
        })
        .with_retry(retry);

    let rt = Runtime::new().buffer(16);
    let (tx, _rx, cancel, handle) = rt.spawn(pipe);
    tx.send(()).await.expect("start send failed");
    entered_backoff.notified().await;

    cancel.cancel();
    drop(tx);

    for _ in 0..10 {
        if handle.is_finished() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        handle.is_finished(),
        "pipeline did not stop after cancellation"
    );

    handle.await??;
    Ok(())
}
