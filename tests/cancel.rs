use std::time::Duration;

use async_trait::async_trait;
use ragpipe::error::Result;
use ragpipe::pipeline::cancel::CancelToken;
use ragpipe::pipeline::pipe::Pipe;
use ragpipe::pipeline::runtime::Runtime;
use tokio::sync::mpsc::{Receiver, Sender};

struct NeverEnding;

#[async_trait]
impl Pipe<(), ()> for NeverEnding {
    async fn process(
        &self,
        mut input: Receiver<()>,
        _output: Sender<()>,
        _buffer: usize,
        cancel: CancelToken,
    ) -> Result<()> {
        // start
        let _ = input.recv().await;

        // loop infinito, mas obedecendo cancel
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(Duration::from_millis(50)) => {}
            }
        }

        Ok(())
    }
}

#[tokio::test]
async fn cancel_stops_pipeline() -> Result<()> {
    let rt = Runtime::new().buffer(8);
    let (tx, _rx, cancel, handle) = rt.spawn(NeverEnding);

    tx.send(()).await.ok();
    // cancela logo em seguida
    cancel.cancel();

    // deve terminar rapidamente (se travar aqui, falha)
    handle.await??;
    Ok(())
}
