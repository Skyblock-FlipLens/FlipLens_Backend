# Adaptive Polling (Hypixel A/B)

This backend now runs two adaptive pollers instead of one fixed `@Scheduled(5s)` loop.

- Endpoint A (`auctions`) targets ~`20s` update rhythm.
- Endpoint B (`bazaar`) targets ~`60s` update rhythm.
- Each endpoint keeps exactly one in-flight request.
- Processing is decoupled with bounded/coalescing pipeline semantics.

## State Machine

`WARMUP -> STEADY -> BURST -> BACKOFF`

1. `WARMUP`
- Moderate polling (`warmup-interval`, default `PT2S`) until first real change events are observed.
- Ends automatically after `warmup-max-seconds`.

2. `STEADY`
- Predict next update: `lastChange + estimatedPeriod`.
- Schedules a probe at `expected - guardWindowMs`.

3. `BURST`
- If probe misses, temporarily polls faster (`burst-interval-ms`) around the expected update window.
- Exits when a change is detected.

4. `BACKOFF`
- If burst window expires with no change or when transient failures occur, waits `backoff-interval`.
- Also used when 429/rate-limit windows are active.

## Change Detection

Priority:

1. `ETag` / `If-None-Match`
2. `Last-Modified` / `If-Modified-Since`
3. Stable hash fallback on relevant payload fields

`304 Not Modified` is treated as `NO_CHANGE`.

`Cache-Control`/`Age` are used to avoid wasteful burst behavior while response data is still cache-fresh.

## Rate Limit Handling

- `429` is handled without retry spam.
- `Retry-After` and `RateLimit-Reset` / `X-RateLimit-Reset` are parsed.
- Poller stays blocked until the computed unblock timestamp.
- Global and endpoint rate limiters cap request frequency.

## Processing / Overlap Protection

Default behavior uses coalescing (`queue-capacity: 1`, `coalesce-enabled: true`):

- If processing is busy, only the latest pending payload is retained.
- No parallel processing for the same endpoint.
- Prevents pile-up under slow processing.

## Metrics

Emitted Micrometer metrics include:

- `skyblock.adaptive.update_detected`
- `skyblock.adaptive.poll_interval_ms`
- `skyblock.adaptive.misses`
- `skyblock.adaptive.phase_error_ms`
- `skyblock.adaptive.processing_lag_ms`
- `skyblock.adaptive.http_429`
- `skyblock.adaptive.mode_transitions`
- `skyblock.adaptive.state`

## Example Config (A=20s / B=60s)

```yaml
config:
  hypixel:
    adaptive:
      enabled: true
      global-max-requests-per-second: 3.0
      auctions:
        period-hint: PT20S
        warmup-interval: PT2S
        guard-window-ms: 400
        burst-interval-ms: 500
        burst-window-ms: 4000
        backoff-interval: PT2S
      bazaar:
        period-hint: PT60S
        warmup-interval: PT2S
        guard-window-ms: 400
        burst-interval-ms: 500
        burst-window-ms: 4000
        backoff-interval: PT2S
      pipeline:
        queue-capacity: 1
        coalesce-enabled: true
```
