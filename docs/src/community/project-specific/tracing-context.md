# Tracing Context Propagation

Rust query execution in Lance crosses many async boundaries: boxed futures, boxed streams, and detached Tokio tasks.
If those boundaries do not preserve the current span, Tempo will show `lance-rust` work as orphan root traces instead of attaching it to the original request trace.

## Rule of Thumb

Do not add new direct `.in_current_span()` calls in tracing-sensitive execution paths.
If a helper does not already exist for the boundary you are working on, add or extend a helper in `lance-core` instead of repeating manual span capture at each call site.

## Preferred Helpers

Use the shared helpers from `lance_core`:

- `spawn_in_current_span(...)` / `spawn_in_span(...)` for `tokio::spawn(...)`
- `future_in_current_span()` / `future_in_span(...)` for non-boxed futures
- `boxed_in_current_span()` / `boxed_in_span(...)` for boxed futures
- `stream_in_current_span()` / `stream_in_span(...)` for non-boxed streams
- `boxed_stream_in_current_span()` / `boxed_stream_in_span(...)` for boxed streams

For decoder-style batch task wrappers, prefer tracing-aware constructors such as `ReadBatchTask::from_future(...)` instead of manually boxing and instrumenting the future at each call site.

## Common Rewrites

### Spawning work

Instead of:

```rust
 tokio::spawn(async move {
     do_work().await
 }.in_current_span())
```

Use:

```rust
spawn_in_current_span(async move {
    do_work().await
})
```

### Boxing a future

Instead of:

```rust
let fut = async move {
    load_page().await
}
.in_current_span()
.boxed();
```

Use:

```rust
let fut = async move {
    load_page().await
}
.boxed_in_current_span();
```

### Boxing a stream

Instead of:

```rust
stream.stream_in_current_span().boxed()
```

Use:

```rust
stream.boxed_stream_in_current_span()
```

## CI Policy

`ci/check_tracing_context.py` enforces these rules for the current tracing-sensitive hotspots.
When a new execution path becomes trace-sensitive, extend the hotspot file list in that script and in `.github/workflows/tracing-context-check.yml` as part of the same change.

## How to Validate

For tracing regressions, validate with a real request and inspect the full Tempo time window around that request.
Do not only search for a single span name.
Success means the window contains one request root trace and `lance-rust` spans remain underneath it instead of appearing as a separate root trace.
