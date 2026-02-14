use std::sync::Arc;
use std::time::Duration;

use crate::error::Error;

type RetryPredicate = Arc<dyn Fn(&Error) -> bool + Send + Sync>;
type BoxErrorHandler = Arc<dyn for<'a> Fn(ErrorContext<'a>) -> ErrorAction + Send + Sync>;

#[derive(Clone)]
pub struct RetryPolicy {
    max_attempts: u32,
    base_delay: Duration,
    max_delay: Duration,
    jitter: Option<Duration>,
    retry_if: RetryPredicate,
}

impl RetryPolicy {
    pub fn new(max_attempts: u32) -> Self {
        Self {
            max_attempts: max_attempts.max(1),
            base_delay: Duration::from_millis(25),
            max_delay: Duration::from_secs(5),
            jitter: None,
            retry_if: Arc::new(|_| true),
        }
    }

    pub fn base_delay(mut self, base_delay: Duration) -> Self {
        self.base_delay = base_delay;
        self
    }

    pub fn max_delay(mut self, max_delay: Duration) -> Self {
        self.max_delay = max_delay;
        self
    }

    pub fn with_jitter(mut self, max_jitter: Duration) -> Self {
        self.jitter = Some(max_jitter);
        self
    }

    pub fn retry_if<F>(mut self, predicate: F) -> Self
    where
        F: Fn(&Error) -> bool + Send + Sync + 'static,
    {
        self.retry_if = Arc::new(predicate);
        self
    }

    pub fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    pub(crate) fn is_retryable(&self, error: &Error) -> bool {
        (self.retry_if)(error)
    }

    pub(crate) fn backoff_delay(&self, attempt: u32) -> Duration {
        let exp = attempt.saturating_sub(1);
        let mut delay = self.base_delay.saturating_mul(2u32.saturating_pow(exp));
        delay = delay.min(self.max_delay);

        if let Some(max_jitter) = self.jitter {
            let jitter = deterministic_jitter(max_jitter, attempt);
            delay = delay.saturating_add(jitter).min(self.max_delay);
        }

        delay
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::new(3)
    }
}

fn deterministic_jitter(max_jitter: Duration, attempt: u32) -> Duration {
    let nanos = max_jitter.as_nanos().min(u128::from(u64::MAX)) as u64;
    if nanos == 0 {
        return Duration::ZERO;
    }

    let seed = (attempt as u64)
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    Duration::from_nanos(seed % nanos.saturating_add(1))
}

#[derive(Debug)]
pub struct ErrorContext<'a> {
    pub stage: &'static str,
    pub attempt: u32,
    pub max_attempts: u32,
    pub error: &'a Error,
}

#[derive(Debug)]
pub enum ErrorAction {
    Retry,
    Skip,
    Fail(Error),
}

pub struct ErrorHandler(BoxErrorHandler);

impl ErrorHandler {
    pub fn new<F>(f: F) -> Self
    where
        F: for<'a> Fn(ErrorContext<'a>) -> ErrorAction + Send + Sync + 'static,
    {
        Self(Arc::new(f))
    }

    pub(crate) fn call(&self, ctx: ErrorContext<'_>) -> ErrorAction {
        (self.0)(ctx)
    }
}

impl Clone for ErrorHandler {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
