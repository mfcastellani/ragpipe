# Changelog

## 0.2.1

- Added `Runtime::spawn_sink()`: launches a pipeline whose final stage outputs `()`, draining the terminal channel internally so callers don't need to manually drain a `Receiver<()>`.
- Added `Runtime::buffer_stage(stage, size)`: overrides the intermediate channel buffer for a specific named stage without changing the global default.
- Added `Runtime::concurrency_stage(stage, workers)`: enables a pool of concurrent async workers for `try_map` and `try_map_ref` stages. Output ordering is not preserved when `workers > 1`. Has no effect on `map`, `filter`, `inspect`, or custom `Pipe` implementations.
- `TryMapPipe<F>` and `TryMapRefPipe<F>` now store the user function behind `Arc<F>`, enabling cheap cloning for concurrent worker pools (no `F: Clone` bound required).
- Per-stage configuration is propagated via a `tokio::task_local!` set by `Runtime::spawn`/`spawn_sink`, preserving the existing `Pipe` trait signature.

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
