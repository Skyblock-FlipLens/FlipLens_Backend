# Runtime Blocking-Time Instrumentation

## Overview
This implementation adds low-overhead runtime instrumentation for the 5-second polling cycle. It combines:

- Per-cycle context propagation (`cycleId` in MDC + ThreadLocal).
- Per-phase Micrometer timers.
- Explicit app-level blocked-time attribution wrappers for HTTP/DB blocking points.
- Continuous JFR recordings for lock/park/IO/GC events.
- Secured admin endpoints for JFR snapshot dumps, latest JFR report summary, and optional async-profiler trigger.
- A standalone CLI to print blocking summaries from a `.jfr` file.

## Per-cycle context
- New cycle context holder and model:
  - `CycleContext`
  - `CycleContextHolder`
  - `CycleInstrumentationService`
- `SourceJobs.pollApi()` creates cycle IDs in the format `<counter>-<epochMillis>` for every scheduled loop and finalizes cycle summaries.
- `cycleId` is attached to MDC key `cycleId` for log correlation.

## Phase timers (Micrometer)
Recorded phases:
- `pull_http`
- `deserialize`
- `normalize`
- `compute_flips`
- `persist/cache_update`
- `total_cycle`

Metric name:
- `skyblock.polling.phase`

Tags:
- `phase`
- `cycleId` (bucketed by 100-cycle windows)
- `outcome` (`success`/`failure`)
- `payload_size_bucket` (`lt_100kb`, `100kb_1mb`, `1mb_10mb`, `gte_10mb`)

## JFR runtime profiling
### Startup behavior
`JfrRecordingManager` starts two recordings from JFR `profile` configuration:

1. **Continuous** recording
   - destination: `var/profiling/jfr/continuous.jfr` (configurable)
   - retention via `maxAge` (default 2h)
   - capped by `maxSize` (default 512MB)

2. **Snapshot ring** recording
   - ring buffer with `maxAge` = snapshot window (default 2m)
   - used for instant snapshot dumps

### Enabled events for blocking analysis
- `jdk.JavaMonitorBlocked`
- `jdk.JavaMonitorWait`
- `jdk.ThreadPark`
- `jdk.SocketRead`
- `jdk.SocketWrite`
- `jdk.FileRead`
- `jdk.FileWrite`
- `jdk.GarbageCollection`
- `jdk.CPULoad`
- `jdk.ExecutionSample`

Stack depth is set at runtime using `jdk.jfr.stackdepth` (default `256`).

### Retention cleanup
A scheduled cleanup removes stale `.jfr` files older than configured retention.

## App-level blocked-time attribution
`BlockingTimeTracker` wraps known blocking points and records blocked milliseconds per label/category into current cycle context.

Current wrappers:
- Hypixel HTTP calls in `HypixelClient.request(...)` as `http.hypixel<uri>` category `http`.
- Market snapshot repository calls in `MarketSnapshotPersistenceService` category `db`.

Slow event logging:
- threshold default: `100ms`
- stack capture sampled (default `1%`)
- rate-limited (default every `30s` max)

## Per-cycle structured summary
At cycle completion, logs include structured summary fields:
- `cycleId`
- `payloadBytes`
- `totalCycleMillis`
- `perPhaseMillis`
- `slowBlockingPoints` (top 5)
- `outcome`

## Admin endpoints (secured)
Base path: `/internal/admin/instrumentation`

Security:
- local-only by default (`instrumentation.admin.local-only=true`)
- optional token via header `X-Admin-Token`

Endpoints:
- `POST /jfr/snapshot` → dumps snapshot ring to disk immediately.
- `GET /jfr/report/latest` → parses latest JFR and returns:
  - top blocking stacks (monitor/park/wait)
  - top IO wait stacks (socket/file)
  - blocked-vs-CPU summary
- `POST /async-profiler/run` → runs optional async-profiler script (when enabled).

## Optional async-profiler integration
Script added:
- `scripts/run_async_profiler.sh`

Expected env:
- `ASYNC_PROFILER_HOME=<path>`

Produces 30s profiles:
- CPU flamegraph
- lock contention flamegraph
- allocation flamegraph

## CLI summary tool
`JfrReportCli`:

```bash
java -cp target/classes com.skyblockflipper.backend.instrumentation.JfrReportCli var/profiling/jfr/snapshot-<ts>.jfr
```

Prints:
- top blocking stacks
- top IO wait stacks
- blocked time vs CPU time ratio

## Configuration
Added `instrumentation.*` properties in `application.yml`:

- `instrumentation.jfr.*`
- `instrumentation.blocking.*`
- `instrumentation.admin.*`
- `instrumentation.async-profiler.*`

All settings are environment-overridable.
