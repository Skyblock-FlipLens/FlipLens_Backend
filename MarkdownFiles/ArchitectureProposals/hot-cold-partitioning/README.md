# Hot/Cold Partitioning Spike (PostgreSQL)

## Question
Can we split heavy-churn tables into 2 partitions (`hot` = expected delete, `cold` = expected keep) and move rows with a boolean flag?

## Short Answer
Yes, we can do it, but **using a mutable boolean as the main partition key is usually not the best choice** for this workload.

## Why the Boolean Approach Is Risky
- Updating a partition key (`is_hot=true -> false`) causes row movement across partitions (delete + insert internally).
- With large volumes, this creates additional WAL, index churn, and vacuum pressure.
- It can trade one bottleneck (bulk delete) for another (bulk row movement).

## Better Fit for Current Workload
Your churn is snapshot/retention driven. A better partition key is an **immutable time key** (for example `snapshot_timestamp_epoch_millis` bucket/day).

Then compaction can:
- Keep current partitions as "hot" by time window.
- Mark older partitions as "cold" (read mostly).
- Drop old hot partitions directly when eligible (fast metadata operation), instead of row-by-row deletes.

## Daily Bucket Rules
- For long windows, use UTC 1-day buckets.
- Enforce half-open intervals to prevent overlap: `[day_start_utc, next_day_start_utc)`.
- Store both `bucket_start_utc` and `bucket_end_utc` (or fixed interval metadata), not one ambiguous timestamp.
- Keep a watermark/grace delay before finalizing a day bucket to handle late-arriving rows.

## Rollup-Before-Drop Flow
1. Write raw snapshots into hot time partitions.
2. Roll up raw -> short bucket (for example 1m) -> medium bucket (for example 1h) -> long bucket (1d).
3. Verify rollup success and completeness for the covered time range.
4. Drop only source partitions that are fully materialized into the next tier.
5. Allow correction upserts for finalized long buckets when late data arrives after watermark.

This avoids prediction heuristics and uses deterministic age/window rules.

## Recommended Design
1. Partition parent market/flip tables by time (range/list by snapshot bucket/day).
2. Align dependent high-churn tables so cleanup follows partition lifecycle.
3. Replace delete loops with partition drop/truncate for retention windows.
4. Keep a fallback path for non-partitioned environments.

## Operational DDL Spike (Minimal Example)
```sql
-- Parent (time-based partitioning)
CREATE TABLE market_snapshot (
  id bigserial,
  snapshot_ts timestamptz NOT NULL,
  bucket_start_utc timestamptz NOT NULL,
  item_id text NOT NULL,
  payload jsonb,
  -- Keep PK/unique compatible with partitioning rules.
  -- In Postgres, UNIQUE across partitioned tables must include partition key.
  PRIMARY KEY (id, bucket_start_utc)
) PARTITION BY RANGE (bucket_start_utc);

-- Default partition for unexpected routing / late arrivals
CREATE TABLE market_snapshot_default
  PARTITION OF market_snapshot DEFAULT;

-- One-day partition
CREATE TABLE market_snapshot_2026_03_01
  PARTITION OF market_snapshot
  FOR VALUES FROM ('2026-03-01 00:00:00+00') TO ('2026-03-02 00:00:00+00');

-- Per-partition indexes (local)
CREATE INDEX market_snapshot_2026_03_01_item_ts_idx
  ON market_snapshot_2026_03_01 (item_id, snapshot_ts);
```

Notes:
- Derive `bucket_start_utc` deterministically from UTC day boundary.
- Prefer partition-local indexes over huge global-style indexes.
- Validate uniqueness strategy early (include partition key where required).

## Query/API Partition Pruning Rules
- Use bounded time filters in queries (`>= start` and `< end`).
- Filter by `bucket_start_utc` and/or `snapshot_ts` with explicit constants/parameters.
- Avoid expressions in `WHERE` that hide pruning opportunities (for example `date_trunc(...)` on table column at runtime).
- For "latest snapshot" APIs, default to a small hot window (for example last 48h) unless caller explicitly asks wider range.

## Foreign Keys and Drop-Partition Reality
- Cross-partition FK usage can make partition drop workflows painful.
- For churn-heavy snapshot paths, prefer soft integrity or co-partitioned designs.
- If child tables are retained differently, define lifecycle boundaries clearly before enabling FK constraints.
- Best-case design: child tables share the same partition key and retention schedule as parent ("co-partitioned lifecycle").

## Practical Notes for This Repo
- Current logs show churn mainly around flip lifecycle deletes (`flip`, `flip_step`, `flip_definition`, `flip_current`, `flip_trend_segment`).
- Partitioning only one table helps less if child tables still churn row-by-row.
- If we still want explicit hot/cold semantics, use a derived immutable tier at insert time (or time bucket), not frequent boolean updates.

## Accuracy and Outliers
- Rollups are exact for composable metrics: `count`, `sum`, `min`, `max`, `first`, `last`, weighted sums.
- Avoid relying on plain average only; keep weighted aggregates where relevant.
- Median/quantiles are not exactly composable from already-aggregated medians.
- For robust long-window statistics, store mergeable quantile sketches per bucket (for example t-digest/KLL-style approach) and merge at higher tiers.
- Keep explicit outlier diagnostics (`outlier_count`, caps/winsorized sums, min/max spreads) so anomalies remain visible.

## Late Arrivals and Watermark Contract
- Define `finalize_delay` explicitly (for example 6h, 12h, or 24h).
- Keep current day and previous day write-enabled by default.
- Finalize day buckets only after watermark passes.
- For post-finalization late data, pick one path and document it:
  - Correction upsert in-place (higher WAL, simpler reads), or
  - Separate corrections table merged at read time / periodic reconcile job.

## Retention Playbook (Drop vs Truncate vs Detach)
- `DROP PARTITION`: fastest full lifecycle removal when data is no longer needed.
- `TRUNCATE PARTITION`: fast clear while keeping structure.
- `DETACH PARTITION` + async drop: useful for controlled maintenance windows.

Example policy template:
- Raw snapshots: keep 7d
- 1m buckets: keep 30d
- 1h buckets: keep 180d
- 1d buckets: keep 2y

Only remove a source tier after target tier materialization is complete and verified.

## Rollup Correctness Contract
- Never compute derived metrics as "average of averages" unless mathematically valid for that metric.
- Persist enough base stats per bucket to recompute downstream metrics correctly:
  - `count`, `sum`, `min`, `max`
  - `sum_weights` (for weighted variants)
  - `first_ts`, `last_ts` (for boundary-sensitive logic)
- If quantiles are required long-term, store mergeable sketch payloads (`bytea` or `jsonb`) plus version tag.

## Spike Success Metrics
Track before/after for the same workload window:
- WAL volume
- Autovacuum frequency and dead tuples
- Compactor runtime and blocking/lock side effects
- API query latency (p95/p99) on representative endpoints
- Table/index bloat and total DB growth rate

## Naming and Automation
- Naming convention: `<table>_YYYY_MM_DD` (UTC day).
- Add helper routine: `ensure_partition_exists(day_utc)`.
- Pre-create partitions ahead (for example +14 days) via scheduler/cron.
- Manage default partition drift with regular backfill/reroute job.
- Optional: evaluate `pg_partman` to reduce custom partition management code.

## Suggested Migration Strategy
1. **Phase 0 (safe baseline):** reduce compactor pressure via scheduler interval + smaller delete batch + pause (already possible via env).
2. **Phase 1 (schema spike):** add partitioned shadow tables and dual-write in a feature flag branch.
3. **Phase 2 (query alignment):** make API/read paths pruning-friendly (`>= start AND < end`) and validate explain plans.
4. **Phase 3 (rollup pipeline):** implement deterministic rollup-before-drop and day finalization watermark.
5. **Phase 4 (cutover):** switch reads/writes to partitioned + rolled-up tables, keep rollback path.
6. **Phase 5:** retire row-delete compaction path for partition-aware retention.

## Decision
Proceed with a partitioning spike, but prefer **time-based immutable partitioning** over mutable boolean hot/cold movement.

## Related Proposal
- See `ROLLUP_AND_ITEM_ANOMALY_SEGMENTS.md` for the proposed bucket rollup, item-local anomaly segmentation, and partition-drop gating model.
