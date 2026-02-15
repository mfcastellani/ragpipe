# Changelog

## 0.2.0
- Retry policy contract clarified: `RetryPolicy::new(max_attempts)` now retries nothing by default unless `retry_if(...)` is configured.
- Added `RetryPolicy::none()` and `RetryPolicy::retry_none()` helpers for explicit no-retry configuration.
- `retry(policy)` alias kept but deprecated in favor of `with_retry(policy)`.
- `on_error(ErrorAction::Skip)` now drops only the current item and continues the stream.
- Documentation expanded with API contracts for backpressure, cancellation, retries, and downstream-close behavior.
- Added feature-gated `tracing` spans/events for stage execution, retries, cancellation, and downstream-close observability.

## 0.1.0-alpha2
- Added support for custom error handling and retry mechanisms.

## 0.1.0-alpha1
- Initial release: streaming pipeline core, chaining, cancellation, adapters (map/filter/inspect), basic sources/sinks and tests.
