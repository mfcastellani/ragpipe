use async_trait::async_trait;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::error::Result;
use crate::pipeline::cancel::CancelToken;

#[async_trait]
pub trait Pipe<I: Send + 'static, O: Send + 'static>: Send + Sync {
    fn stage_name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }

    async fn process(
        &self,
        input: Receiver<I>,
        output: Sender<O>,
        buffer: usize,
        cancel: CancelToken,
    ) -> Result<()>;
}
