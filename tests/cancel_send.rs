use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use ragpipe::error::Result;
use ragpipe::pipeline::cancel::CancelToken;
use ragpipe::pipeline::chain::PipeExt;
use ragpipe::pipeline::pipe::Pipe;
use ragpipe::pipeline::runtime::Runtime;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Notify;

struct BurstSource {
    about_to_block: Arc<Notify>,
}

#[async_trait]
impl Pipe<(), u32> for BurstSource {
    async fn process(
        &self,
        mut input: Receiver<()>,
        output: Sender<u32>,
        _buffer: usize,
        cancel: CancelToken,
    ) -> Result<()> {
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            _ = input.recv() => {}
        }

        for i in 0..1024_u32 {
            if i == 1 {
                self.about_to_block.notify_waiters();
            }
            if output.send(i).await.is_err() {
                break;
            }
            if cancel.is_cancelled() {
                break;
            }
        }
        Ok(())
    }
}

struct BlockedSink;

#[async_trait]
impl Pipe<u32, ()> for BlockedSink {
    async fn process(
        &self,
        _input: Receiver<u32>,
        _output: Sender<()>,
        _buffer: usize,
        cancel: CancelToken,
    ) -> Result<()> {
        cancel.cancelled().await;
        Ok(())
    }
}

#[tokio::test]
async fn cancel_while_upstream_blocked_on_send_finishes_promptly() -> Result<()> {
    let about_to_block = Arc::new(Notify::new());

    let pipe = BurstSource {
        about_to_block: about_to_block.clone(),
    }
    .pipe::<(), _>(BlockedSink);

    let rt = Runtime::new().buffer(1);
    let (tx, _rx, cancel, handle) = rt.spawn(pipe);

    tx.send(()).await.expect("start send failed");
    about_to_block.notified().await;
    cancel.cancel();
    drop(tx);

    tokio::time::timeout(Duration::from_millis(300), handle)
        .await
        .expect("pipeline did not stop after cancellation")??;
    Ok(())
}
