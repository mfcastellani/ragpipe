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

enum RunResult<O> {
    Emit(O),
    Skip,
    Stop,
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

    /// Configure retry policy for this stage.
    ///
    /// `RetryPolicy::new(max_attempts)` does not retry by default until
    /// `retry_if(...)` is configured.
    pub fn with_retry(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }

    /// Deprecated alias for [`Self::with_retry`].
    #[deprecated(note = "use with_retry()")]
    pub fn retry(self, policy: RetryPolicy) -> Self {
        self.with_retry(policy)
    }

    /// Install a per-item error handler.
    ///
    /// If set, this handler overrides the policy's default retry/fail decision.
    /// `ErrorAction::Retry` still obeys `max_attempts`.
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

    /// Configure retry policy for this stage.
    ///
    /// `RetryPolicy::new(max_attempts)` does not retry by default until
    /// `retry_if(...)` is configured.
    pub fn with_retry(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }

    /// Deprecated alias for [`Self::with_retry`].
    #[deprecated(note = "use with_retry()")]
    pub fn retry(self, policy: RetryPolicy) -> Self {
        self.with_retry(policy)
    }

    /// Install a per-item error handler.
    ///
    /// If set, this handler overrides the policy's default retry/fail decision.
    /// `ErrorAction::Retry` still obeys `max_attempts`.
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
    fn stage_name(&self) -> &'static str {
        self.stage
    }

    async fn process(
        &self,
        mut input: Receiver<I>,
        output: Sender<O>,
        _buffer: usize,
        cancel: CancelToken,
    ) -> Result<()> {
        #[cfg(feature = "tracing")]
        let stage = self.stage_name();

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    #[cfg(feature = "tracing")]
                    tracing::event!(tracing::Level::DEBUG, event = "ragpipe.cancelled", stage = stage, where_ = "recv", "ragpipe.cancelled");
                    break
                },
                msg = input.recv() => {
                    let Some(item) = msg else { break; };
                    let value = match self.run_with_retry(item, cancel.clone()).await? {
                        RunResult::Emit(v) => v,
                        RunResult::Skip => continue,
                        RunResult::Stop => break,
                    };

                    if output.send(value).await.is_err() {
                        #[cfg(feature = "tracing")]
                        tracing::event!(tracing::Level::INFO, event = "ragpipe.downstream.closed", stage = stage, "ragpipe.downstream.closed");
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
    fn stage_name(&self) -> &'static str {
        self.stage
    }

    async fn process(
        &self,
        mut input: Receiver<I>,
        output: Sender<O>,
        _buffer: usize,
        cancel: CancelToken,
    ) -> Result<()> {
        #[cfg(feature = "tracing")]
        let stage = self.stage_name();

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    #[cfg(feature = "tracing")]
                    tracing::event!(tracing::Level::DEBUG, event = "ragpipe.cancelled", stage = stage, where_ = "recv", "ragpipe.cancelled");
                    break
                },
                msg = input.recv() => {
                    let Some(item) = msg else { break; };
                    let value = match self.run_with_retry(item, cancel.clone()).await? {
                        RunResult::Emit(v) => v,
                        RunResult::Skip => continue,
                        RunResult::Stop => break,
                    };

                    if output.send(value).await.is_err() {
                        #[cfg(feature = "tracing")]
                        tracing::event!(tracing::Level::INFO, event = "ragpipe.downstream.closed", stage = stage, "ragpipe.downstream.closed");
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}

impl<F> TryMapPipe<F> {
    async fn run_with_retry<I, O, Fut>(&self, item: I, cancel: CancelToken) -> Result<RunResult<O>>
    where
        I: Clone,
        F: Fn(I) -> Fut,
        Fut: Future<Output = Result<O>>,
    {
        let max_attempts = self.retry_policy.max_attempts();
        let mut attempt = 1u32;

        loop {
            if cancel.is_cancelled() {
                #[cfg(feature = "tracing")]
                tracing::event!(
                    tracing::Level::DEBUG,
                    event = "ragpipe.cancelled",
                    stage = self.stage,
                    where_ = "attempt",
                    "ragpipe.cancelled"
                );
                return Ok(RunResult::Stop);
            }
            let current_item = item.clone();

            match (self.f)(current_item).await {
                Ok(value) => return Ok(RunResult::Emit(value)),

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
                    let retryable = self.retry_policy.is_retryable(ctx.error);

                    #[cfg(feature = "tracing")]
                    tracing::event!(
                        tracing::Level::WARN,
                        event = "ragpipe.retry.attempt_failed",
                        stage = self.stage,
                        attempt = attempt,
                        max_attempts = max_attempts,
                        retryable = retryable,
                        error = %ctx.error,
                        "ragpipe.retry.attempt_failed"
                    );

                    // Default action: retry when retryable, otherwise fail with original error.
                    let action = if let Some(handler) = &self.error_handler {
                        handler.call(ctx)
                    } else if retryable {
                        ErrorAction::Retry
                    } else {
                        ErrorAction::Fail(
                            error_slot
                                .take()
                                .expect("error must be available for fail action"),
                        )
                    };
                    let action_name = match &action {
                        ErrorAction::Retry => "retry",
                        ErrorAction::Skip => "skip",
                        ErrorAction::Fail(_) => "fail",
                    };
                    if self.error_handler.is_some()
                        && ((retryable && action_name != "retry")
                            || (!retryable && action_name != "fail"))
                    {
                        #[cfg(feature = "tracing")]
                        tracing::event!(
                            tracing::Level::DEBUG,
                            event = "ragpipe.error_handler.action",
                            stage = self.stage,
                            attempt = attempt,
                            action = action_name,
                            "ragpipe.error_handler.action"
                        );
                    }

                    match action {
                        ErrorAction::Retry => {
                            if attempt >= max_attempts {
                                #[cfg(feature = "tracing")]
                                tracing::event!(
                                    tracing::Level::ERROR,
                                    event = "ragpipe.retry.exhausted",
                                    stage = self.stage,
                                    attempts = max_attempts,
                                    error = %error_slot
                                        .as_ref()
                                        .expect("error must be available for retry exhaustion"),
                                    "ragpipe.retry.exhausted"
                                );
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
                                #[cfg(feature = "tracing")]
                                tracing::event!(
                                    tracing::Level::WARN,
                                    event = "ragpipe.retry.sleep",
                                    stage = self.stage,
                                    attempt = attempt,
                                    delay_ms = delay.as_millis() as u64,
                                    "ragpipe.retry.sleep"
                                );
                                tokio::select! {
                                    _ = cancel.cancelled() => {
                                        #[cfg(feature = "tracing")]
                                        tracing::event!(tracing::Level::DEBUG, event = "ragpipe.cancelled", stage = self.stage, where_ = "backoff", "ragpipe.cancelled");
                                        return Ok(RunResult::Stop)
                                    },
                                    _ = tokio::time::sleep(delay) => {}
                                }
                            }

                            attempt += 1;
                        }
                        ErrorAction::Skip => return Ok(RunResult::Skip),
                        ErrorAction::Fail(mapped) => {
                            return Err(Error::stage_source(self.stage, mapped))
                        }
                    }
                }
            }
        }
    }
}

impl<F> TryMapRefPipe<F> {
    async fn run_with_retry<I, O, Fut>(&self, item: I, cancel: CancelToken) -> Result<RunResult<O>>
    where
        F: Fn(&I) -> Fut,
        Fut: Future<Output = Result<O>>,
    {
        let max_attempts = self.retry_policy.max_attempts();
        let mut attempt = 1u32;

        loop {
            if cancel.is_cancelled() {
                #[cfg(feature = "tracing")]
                tracing::event!(
                    tracing::Level::DEBUG,
                    event = "ragpipe.cancelled",
                    stage = self.stage,
                    where_ = "attempt",
                    "ragpipe.cancelled"
                );
                return Ok(RunResult::Stop);
            }
            match (self.f)(&item).await {
                Ok(value) => return Ok(RunResult::Emit(value)),

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
                    let retryable = self.retry_policy.is_retryable(ctx.error);

                    #[cfg(feature = "tracing")]
                    tracing::event!(
                        tracing::Level::WARN,
                        event = "ragpipe.retry.attempt_failed",
                        stage = self.stage,
                        attempt = attempt,
                        max_attempts = max_attempts,
                        retryable = retryable,
                        error = %ctx.error,
                        "ragpipe.retry.attempt_failed"
                    );

                    let action = if let Some(handler) = &self.error_handler {
                        handler.call(ctx)
                    } else if retryable {
                        ErrorAction::Retry
                    } else {
                        ErrorAction::Fail(
                            error_slot
                                .take()
                                .expect("error must be available for fail action"),
                        )
                    };
                    let action_name = match &action {
                        ErrorAction::Retry => "retry",
                        ErrorAction::Skip => "skip",
                        ErrorAction::Fail(_) => "fail",
                    };
                    if self.error_handler.is_some()
                        && ((retryable && action_name != "retry")
                            || (!retryable && action_name != "fail"))
                    {
                        #[cfg(feature = "tracing")]
                        tracing::event!(
                            tracing::Level::DEBUG,
                            event = "ragpipe.error_handler.action",
                            stage = self.stage,
                            attempt = attempt,
                            action = action_name,
                            "ragpipe.error_handler.action"
                        );
                    }

                    match action {
                        ErrorAction::Retry => {
                            if attempt >= max_attempts {
                                #[cfg(feature = "tracing")]
                                tracing::event!(
                                    tracing::Level::ERROR,
                                    event = "ragpipe.retry.exhausted",
                                    stage = self.stage,
                                    attempts = max_attempts,
                                    error = %error_slot
                                        .as_ref()
                                        .expect("error must be available for retry exhaustion"),
                                    "ragpipe.retry.exhausted"
                                );
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
                                #[cfg(feature = "tracing")]
                                tracing::event!(
                                    tracing::Level::WARN,
                                    event = "ragpipe.retry.sleep",
                                    stage = self.stage,
                                    attempt = attempt,
                                    delay_ms = delay.as_millis() as u64,
                                    "ragpipe.retry.sleep"
                                );
                                tokio::select! {
                                    _ = cancel.cancelled() => {
                                        #[cfg(feature = "tracing")]
                                        tracing::event!(tracing::Level::DEBUG, event = "ragpipe.cancelled", stage = self.stage, where_ = "backoff", "ragpipe.cancelled");
                                        return Ok(RunResult::Stop)
                                    },
                                    _ = tokio::time::sleep(delay) => {}
                                }
                            }

                            attempt += 1;
                        }
                        ErrorAction::Skip => return Ok(RunResult::Skip),
                        ErrorAction::Fail(mapped) => {
                            return Err(Error::stage_source(self.stage, mapped))
                        }
                    }
                }
            }
        }
    }
}
