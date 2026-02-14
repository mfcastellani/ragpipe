use std::future::Future;

use async_trait::async_trait;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::error::{Error, Result};
use crate::pipeline::cancel::CancelToken;
use crate::pipeline::pipe::Pipe;
use crate::pipeline::retry::{ErrorAction, ErrorContext, ErrorHandler, RetryPolicy};

pub struct TryMapPipe<F> {
    stage: &'static str,
    f: F,
    retry_policy: RetryPolicy,
    error_handler: Option<ErrorHandler>,
}

pub struct TryMapRefPipe<F> {
    stage: &'static str,
    f: F,
    retry_policy: RetryPolicy,
    error_handler: Option<ErrorHandler>,
}

impl<F> TryMapPipe<F> {
    pub fn new(stage: &'static str, f: F) -> Self {
        Self {
            stage,
            f,
            retry_policy: RetryPolicy::new(1),
            error_handler: None,
        }
    }

    pub fn with_retry(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }

    pub fn retry(self, policy: RetryPolicy) -> Self {
        self.with_retry(policy)
    }

    pub fn on_error<H>(mut self, handler: H) -> Self
    where
        H: for<'a> Fn(ErrorContext<'a>) -> ErrorAction + Send + Sync + 'static,
    {
        self.error_handler = Some(ErrorHandler::new(handler));
        self
    }
}

impl<F> TryMapRefPipe<F> {
    pub fn new(stage: &'static str, f: F) -> Self {
        Self {
            stage,
            f,
            retry_policy: RetryPolicy::new(1),
            error_handler: None,
        }
    }

    pub fn with_retry(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }

    pub fn retry(self, policy: RetryPolicy) -> Self {
        self.with_retry(policy)
    }

    pub fn on_error<H>(mut self, handler: H) -> Self
    where
        H: for<'a> Fn(ErrorContext<'a>) -> ErrorAction + Send + Sync + 'static,
    {
        self.error_handler = Some(ErrorHandler::new(handler));
        self
    }
}

#[async_trait]
impl<I, O, F, Fut> Pipe<I, O> for TryMapPipe<F>
where
    I: Send + Clone + 'static,
    O: Send + 'static,
    F: Fn(I) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<O>> + Send,
{
    async fn process(
        &self,
        mut input: Receiver<I>,
        output: Sender<O>,
        _buffer: usize,
        cancel: CancelToken,
    ) -> Result<()> {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                msg = input.recv() => {
                    let Some(item) = msg else { break; };
                    let value = match self.run_with_retry(item, cancel.clone()).await? {
                        Some(v) => v,
                        None => break,
                    };

                    if output.send(value).await.is_err() {
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl<I, O, F, Fut> Pipe<I, O> for TryMapRefPipe<F>
where
    I: Send + 'static,
    O: Send + 'static,
    F: Fn(&I) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<O>> + Send + 'static,
{
    async fn process(
        &self,
        mut input: Receiver<I>,
        output: Sender<O>,
        _buffer: usize,
        cancel: CancelToken,
    ) -> Result<()> {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                msg = input.recv() => {
                    let Some(item) = msg else { break; };
                    let value = match self.run_with_retry(item, cancel.clone()).await? {
                        Some(v) => v,
                        None => break,
                    };

                    if output.send(value).await.is_err() {
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}

impl<F> TryMapPipe<F> {
    async fn run_with_retry<I, O, Fut>(&self, item: I, cancel: CancelToken) -> Result<Option<O>>
    where
        I: Clone,
        F: Fn(I) -> Fut,
        Fut: Future<Output = Result<O>>,
    {
        let max_attempts = self.retry_policy.max_attempts();
        let mut attempt = 1u32;

        loop {
            let current_item = item.clone();

            match (self.f)(current_item).await {
                Ok(value) => return Ok(Some(value)),

                Err(error) => {
                    let mut error_slot = Some(error);

                    // Build context before moving error.
                    let ctx = ErrorContext {
                        stage: self.stage,
                        attempt,
                        max_attempts,
                        error: error_slot
                            .as_ref()
                            .expect("error must be available for context"),
                    };

                    // Default action: retry when retryable, otherwise fail with original error.
                    let action = if let Some(handler) = &self.error_handler {
                        handler.call(ctx)
                    } else if self.retry_policy.is_retryable(ctx.error) {
                        ErrorAction::Retry
                    } else {
                        ErrorAction::Fail(
                            error_slot
                                .take()
                                .expect("error must be available for fail action"),
                        )
                    };

                    match action {
                        ErrorAction::Skip => return Ok(None),
                        ErrorAction::Fail(mapped) => {
                            return Err(Error::stage_source(self.stage, mapped));
                        }
                        ErrorAction::Retry => {
                            if attempt >= max_attempts {
                                return Err(Error::retry_exhausted(
                                    self.stage,
                                    max_attempts,
                                    error_slot
                                        .take()
                                        .expect("error must be available for retry exhaustion"),
                                ));
                            }

                            let delay = self.retry_policy.backoff_delay(attempt);
                            if !delay.is_zero() {
                                tokio::select! {
                                    _ = cancel.cancelled() => return Ok(None),
                                    _ = tokio::time::sleep(delay) => {}
                                }
                            }

                            attempt += 1;
                        }
                    }
                }
            }
        }
    }
}

impl<F> TryMapRefPipe<F> {
    async fn run_with_retry<I, O, Fut>(&self, item: I, cancel: CancelToken) -> Result<Option<O>>
    where
        F: Fn(&I) -> Fut,
        Fut: Future<Output = Result<O>>,
    {
        let max_attempts = self.retry_policy.max_attempts();
        let mut attempt = 1u32;

        loop {
            match (self.f)(&item).await {
                Ok(value) => return Ok(Some(value)),

                Err(error) => {
                    let mut error_slot = Some(error);

                    let ctx = ErrorContext {
                        stage: self.stage,
                        attempt,
                        max_attempts,
                        error: error_slot
                            .as_ref()
                            .expect("error must be available for context"),
                    };

                    let action = if let Some(handler) = &self.error_handler {
                        handler.call(ctx)
                    } else if self.retry_policy.is_retryable(ctx.error) {
                        ErrorAction::Retry
                    } else {
                        ErrorAction::Fail(
                            error_slot
                                .take()
                                .expect("error must be available for fail action"),
                        )
                    };

                    match action {
                        ErrorAction::Skip => return Ok(None),
                        ErrorAction::Fail(mapped) => {
                            return Err(Error::stage_source(self.stage, mapped));
                        }
                        ErrorAction::Retry => {
                            if attempt >= max_attempts {
                                return Err(Error::retry_exhausted(
                                    self.stage,
                                    max_attempts,
                                    error_slot
                                        .take()
                                        .expect("error must be available for retry exhaustion"),
                                ));
                            }

                            let delay = self.retry_policy.backoff_delay(attempt);
                            if !delay.is_zero() {
                                tokio::select! {
                                    _ = cancel.cancelled() => return Ok(None),
                                    _ = tokio::time::sleep(delay) => {}
                                }
                            }

                            attempt += 1;
                        }
                    }
                }
            }
        }
    }
}
