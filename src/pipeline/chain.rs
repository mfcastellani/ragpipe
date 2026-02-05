use std::marker::PhantomData;

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::error::Result;
use crate::pipeline::adapters::{FilterPipe, InspectPipe, MapPipe};
use crate::pipeline::cancel::CancelToken;
use crate::pipeline::pipe::Pipe;

pub struct Chain<A, B, M> {
    a: A,
    b: B,
    _m: PhantomData<fn() -> M>,
}

impl<A, B, M> Chain<A, B, M> {
    pub fn new(a: A, b: B) -> Self {
        Self {
            a,
            b,
            _m: PhantomData,
        }
    }
}

#[async_trait]
impl<I, M, O, A, B> Pipe<I, O> for Chain<A, B, M>
where
    I: Send + 'static,
    M: Send + 'static,
    O: Send + 'static,
    A: Pipe<I, M> + Send + Sync,
    B: Pipe<M, O> + Send + Sync,
{
    async fn process(
        &self,
        input: mpsc::Receiver<I>,
        output: mpsc::Sender<O>,
        buffer: usize,
        cancel: CancelToken,
    ) -> Result<()> {
        let (tx_mid, rx_mid) = mpsc::channel::<M>(buffer);

        let left = self.a.process(input, tx_mid, buffer, cancel.clone());
        let right = self.b.process(rx_mid, output, buffer, cancel.clone());

        tokio::pin!(left);
        tokio::pin!(right);

        let mut left_done = false;
        let mut right_done = false;
        let mut left_res: Option<Result<()>> = None;
        let mut right_res: Option<Result<()>> = None;

        loop {
            tokio::select! {
                res = &mut left, if !left_done => {
                    left_done = true;
                    if res.is_err() {
                        cancel.cancel();
                    }
                    left_res = Some(res);
                }
                res = &mut right, if !right_done => {
                    right_done = true;
                    if res.is_err() {
                        cancel.cancel();
                    }
                    right_res = Some(res);
                }
            }

            if left_done && right_done {
                break;
            }
        }

        left_res.unwrap()?;
        right_res.unwrap()?;
        Ok(())
    }
}

pub trait PipeExt<I, O>: Pipe<I, O> + Sized
where
    I: Send + 'static,
    O: Send + 'static,
{
    fn pipe<N, P2>(self, next: P2) -> Chain<Self, P2, O>
    where
        N: Send + 'static,
        P2: Pipe<O, N> + Send + Sync,
        Self: Send + Sync,
    {
        Chain::new(self, next)
    }

    fn map<N, F>(self, f: F) -> Chain<Self, MapPipe<F>, O>
    where
        N: Send + 'static,
        F: Fn(O) -> N + Send + Sync + 'static,
        Self: Send + Sync,
    {
        Chain::new(self, MapPipe(f))
    }

    fn filter<F>(self, pred: F) -> Chain<Self, FilterPipe<F>, O>
    where
        F: Fn(&O) -> bool + Send + Sync + 'static,
        Self: Send + Sync,
    {
        Chain::new(self, FilterPipe(pred))
    }

    fn inspect<F>(self, f: F) -> Chain<Self, InspectPipe<F>, O>
    where
        F: Fn(&O) + Send + Sync + 'static,
        Self: Send + Sync,
    {
        Chain::new(self, InspectPipe(f))
    }
}

impl<I, O, P> PipeExt<I, O> for P
where
    I: Send + 'static,
    O: Send + 'static,
    P: Pipe<I, O> + Sized + Send + Sync,
{
}
