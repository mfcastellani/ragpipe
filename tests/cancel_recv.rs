use async_trait::async_trait;
use ragpipe::error::Result;
use ragpipe::pipeline::cancel::CancelToken;
use ragpipe::pipeline::pipe::Pipe;
use ragpipe::pipeline::runtime::Runtime;
use tokio::sync::mpsc::{Receiver, Sender};

struct WaitForStart;

#[async_trait]
impl Pipe<(), ()> for WaitForStart {
    async fn process(
        &self,
        mut input: Receiver<()>,
        _output: Sender<()>,
        _buffer: usize,
        cancel: CancelToken,
    ) -> Result<()> {
        tokio::select! {
            _ = cancel.cancelled() => Ok(()),
            _ = input.recv() => Ok(()),
        }
    }
}

#[tokio::test]
async fn cancel_while_waiting_on_recv_finishes_promptly() -> Result<()> {
    let rt = Runtime::new().buffer(1);
    let (_tx, _rx, cancel, handle) = rt.spawn(WaitForStart);

    cancel.cancel();

    tokio::time::timeout(std::time::Duration::from_millis(300), handle)
        .await
        .expect("pipeline did not stop after cancellation")??;
    Ok(())
}
