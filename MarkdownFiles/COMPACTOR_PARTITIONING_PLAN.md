# Compactor Partitioning Plan (Planned, Not Implemented)

## Status

This document describes **planned** retention/compaction changes.

- Partitioning is **not implemented** yet.
- The goal is to replace high-churn row deletes with partition lifecycle operations.

## Why this is planned

Current diagnostics indicate:

- Long-running delete workloads on `flip`-related tables.
- High WAL pressure (`WALWrite` / `WalSync` waits).
- Dead tuple growth that autovacuum struggles to recover quickly.
- API instability during peak compaction windows.

Soft delete (`deleted` flag / `deleted_at`) is not sufficient for this issue:

- It is still an `UPDATE` under MVCC.
- It still creates tuple churn and WAL.
- It still requires vacuum cleanup.

For time-based retention, partition drop is the intended long-term solution.

## Planned architecture change

### 1) Partition by snapshot time

Primary target tables:

- `market_snapshot`
- `flip`
- High-churn dependent tables that are retention-coupled (expected at least `flip_step`, `flip_constraints`; final scope validated during migration design).

Partition key:

- `snapshot_timestamp_epoch_millis` (RANGE partitions).

Partition granularity (initial proposal):

- Daily partitions for `market_snapshot` and `flip`.
- Re-evaluate to hourly if per-day partitions become too large.

### 2) Retention by partition lifecycle

Instead of large `DELETE ... WHERE ...`:

- Create upcoming partitions proactively.
- Mark old partitions as out-of-retention.
- Use either `ALTER TABLE ... DETACH PARTITION` followed by `DROP TABLE` on the detached partition, or directly `DROP TABLE` on the partition table, during low-load windows.

Expected outcome:

- Near-constant delete cost for large expirations.
- Lower lock duration and lower WAL bursts compared to row-by-row purge.

### 3) Keep short-term batch delete safeguards

Until partitioning is live:

- Keep chunked delete behavior.
- Keep per-batch commits.
- Keep small inter-batch pause to smooth write pressure.

These remain a fallback path after partition rollout for edge cases.

## Planned schema/migration strategy

### Phase A: Design and compatibility

1. Confirm table relationships and FK behavior for partitioned parents.
2. Confirm query patterns and required indexes per partition.
3. Decide daily vs hourly partition cadence from observed write volume.

### Phase B: Introduce partitioned tables

1. Create partitioned parent tables (new names or in-place migration strategy).
2. Create partition templates/indexes for required read paths.
3. Add automated partition creation job (forward window, e.g., next 7-14 days).

### Phase C: Backfill and cutover

1. Backfill historical data into partitions.
2. Validate row counts and critical API parity.
3. Switch writes to partitioned tables.
4. Switch compactor retention from row deletes to partition retention job.

### Phase D: Cleanup

1. Disable old heavy delete paths.
2. Remove obsolete indexes/queries if no longer needed.
3. Keep emergency fallback path behind a feature flag.

## Planned compactor behavior after partitioning

Compactor no longer performs monolithic orphan delete sweeps as primary retention.

Primary retention flow:

1. Determine expired time windows.
2. Resolve matching partitions.
3. Detach/drop partitions according to policy.
4. Emit metrics and audit logs.

Fallback flow (feature-flagged):

- Current chunked ID-based orphan cleanup if partition operations are unavailable.

## Observability and safety gates (planned)

Track per compaction run:

- Partition operations count and duration.
- Rows expired (estimated from partition stats).
- WAL bytes (if available from DB metrics pipeline).
- Lock wait duration and blocked sessions.
- API readiness around compaction windows.

Safety controls:

- Circuit breaker: skip destructive retention if DB/API load thresholds exceed limits.
- Max runtime budget per compaction run.
- Dry-run mode for retention planning validation.

## Rollout and rollback plan (planned)

Rollout:

1. Deploy partition creation only (no retention drop).
2. Enable dual-write/validated-write path as needed by migration strategy.
3. Enable partition retention in dry-run mode.
4. Enable actual detach/drop in controlled windows.
5. Decommission legacy row-delete compaction path.

Rollback:

1. Disable partition retention job via config.
2. Re-enable legacy chunked compactor path.
3. Keep partitioned data intact until root cause analysis is complete.

## Open decisions before implementation

1. Final partition granularity: daily vs hourly.
2. Exact dependent tables to co-partition.
3. Retention windows per table class (raw vs derived).
4. Operational window and SLO limits for partition drop operations.
5. Whether to enforce strict feature-flag gating for each phase.

## Acceptance criteria (implementation-ready)

Partitioning work can start when:

1. DDL design is reviewed for FK/index/query compatibility.
2. Migration plan (backfill + cutover + rollback) is approved.
3. Observability and safety guards are specified and testable.
4. CI/integration tests cover partitioned retention behavior and fallback logic.
