# Rollup And Item Anomaly Segments

## Status

Proposal for review before implementation.

This document defines how the compactor should preserve useful market statistics while still dropping old raw partitions. The core idea is:

1. Compute bucket rollups from all valid samples in the bucket.
2. Detect item-local anomaly runs inside the bucket.
3. Materialize anomaly segments for only the affected item.
4. Drop the raw source partition only after rollups and anomaly segments are complete.

This is a follow-up to the hot/cold partitioning spike and keeps the current repo's UTC and deterministic compaction rules.

## Problem

Dropping old partitions only works if the statistics we keep are still useful after raw rows are gone.

Keeping one arbitrary snapshot per time window is not enough because:

- one sample is a weak summary of a whole window
- manipulation spikes can disappear if the chosen keeper lands outside the event
- manipulation spikes can poison aggregates if we use plain averages over the full bucket
- anomalies are often item-local, so splitting the whole bucket is too expensive and creates noise

The compactor therefore needs two outputs per bucket:

- a normal rollup for the non-anomalous behavior
- one or more item-local anomaly segments when an item's behavior clearly diverges

## Scope

In scope:

- `ah_item_snapshot`
- `bz_item_snapshot`
- partition-aware compactor materialization flow
- representative snapshot selection
- anomaly detection and anomaly persistence
- verification gates before partition drop

Not in scope for the first implementation:

- rewriting the full `market_snapshot` JSON blob into anomaly-aware sub-buckets
- global bucket splitting for all items
- multi-step correction logic for late arrivals beyond a simple watermark
- mandatory TimescaleDB adoption for `v1.1`

## Decision Summary

### 1. Keep bucket rollups and anomaly segments as separate artifacts

Do not try to represent a full bucket by a single snapshot.

Instead, persist:

- `*_item_bucket_rollup`: robust statistics for the normal part of the bucket
- `*_item_anomaly_segment`: a simplified row for the anomalous interval of one item

### 2. Split only the affected item, not the whole bucket

If one item shows a manipulation or structural anomaly, only that item gets a local split:

- `pre` normal behavior
- `anomaly` behavior
- `post` normal behavior

All unrelated items stay in the ordinary bucket rollup path.

### 3. Prefer robust statistics over plain averages

Primary statistics should be based on median-style or winsorized metrics, not plain mean-only summaries.

### 4. Support a plain PostgreSQL-first implementation

The first implementation should work with plain PostgreSQL plus the partitioning work already planned. TimescaleDB remains an optional follow-up track for the aggregate tables.

## Terms

- `raw sample`: one row in `ah_item_snapshot` or `bz_item_snapshot`
- `bucket`: a fixed UTC time window, for example `1m`, `2h`, or `1d`
- `bucket rollup`: the persisted summary row for one item in one bucket
- `anomaly segment`: a persisted sub-window for one item whose behavior is clearly outside the normal bucket distribution
- `representative snapshot`: one real raw sample chosen as the closest real sample to the bucket center
- `watermark`: delay after bucket end before the bucket can be finalized and raw data can be dropped

## Retention And Bucket Model

Keep the current canonical ladder as the base rule:

- `0s` to `90s`: keep all raw samples
- `>90s` to `30m`: roll up to `1m`
- `>30m` to `12h`: roll up to `2h`
- `>12h`: roll up to `1d`

For raw partition retention:

- spike/test mode: allow a `1d` raw retention window if needed to validate the pipeline quickly
- target default: keep raw partitions for `7d` before drop

This separates two decisions:

- bucket granularity for statistics
- raw retention horizon before old partitions are removed

## Proposed Schema

The proposal stays close to the current repo shape and keeps AH and BZ separate.

### Bazaar Rollup Table

```sql
create table bz_item_bucket_rollup (
    bucket_start_epoch_millis bigint not null,
    bucket_end_epoch_millis bigint not null,
    bucket_granularity varchar(16) not null,
    product_id varchar(255) not null,
    sample_count integer not null,
    valid_sample_count integer not null,
    anomaly_sample_count integer not null default 0,
    partial boolean not null default false,

    representative_snapshot_ts bigint,

    median_buy_price double precision,
    median_sell_price double precision,
    median_mid_price double precision,
    median_spread double precision,
    p10_mid_price double precision,
    p25_mid_price double precision,
    p75_mid_price double precision,
    p90_mid_price double precision,
    min_mid_price double precision,
    max_mid_price double precision,
    winsorized_avg_mid_price double precision,
    median_buy_volume double precision,
    median_sell_volume double precision,
    min_liquidity double precision,
    max_liquidity double precision,

    first_snapshot_ts bigint,
    last_snapshot_ts bigint,
    created_at_epoch_millis bigint not null,
    updated_at_epoch_millis bigint not null,

    primary key (bucket_start_epoch_millis, bucket_granularity, product_id)
);
```

### Bazaar Anomaly Segment Table

```sql
create table bz_item_anomaly_segment (
    id bigserial primary key,
    bucket_start_epoch_millis bigint not null,
    bucket_end_epoch_millis bigint not null,
    bucket_granularity varchar(16) not null,
    product_id varchar(255) not null,

    segment_start_epoch_millis bigint not null,
    segment_end_epoch_millis bigint not null,
    representative_snapshot_ts bigint,
    peak_snapshot_ts bigint,

    sample_count integer not null,
    anomaly_score double precision not null,
    reason_code varchar(64) not null,
    fragmented boolean not null default false,

    median_buy_price double precision,
    median_sell_price double precision,
    median_mid_price double precision,
    median_spread double precision,
    median_buy_volume double precision,
    median_sell_volume double precision,

    created_at_epoch_millis bigint not null,
    updated_at_epoch_millis bigint not null
);
```

### Auction House Rollup Table

```sql
create table ah_item_bucket_rollup (
    bucket_start_epoch_millis bigint not null,
    bucket_end_epoch_millis bigint not null,
    bucket_granularity varchar(16) not null,
    item_key varchar(255) not null,
    sample_count integer not null,
    valid_sample_count integer not null,
    anomaly_sample_count integer not null default 0,
    partial boolean not null default false,

    representative_snapshot_ts bigint,

    median_bin_lowest5_mean double precision,
    median_bin_p50 double precision,
    median_bin_p95 double precision,
    median_bid_p50 double precision,
    median_bin_count double precision,
    median_ending_soon_count double precision,
    p10_bin_p50 double precision,
    p25_bin_p50 double precision,
    p75_bin_p50 double precision,
    p90_bin_p50 double precision,
    min_bin_p50 double precision,
    max_bin_p50 double precision,
    winsorized_avg_bin_p50 double precision,

    first_snapshot_ts bigint,
    last_snapshot_ts bigint,
    created_at_epoch_millis bigint not null,
    updated_at_epoch_millis bigint not null,

    primary key (bucket_start_epoch_millis, bucket_granularity, item_key)
);
```

### Auction House Anomaly Segment Table

```sql
create table ah_item_anomaly_segment (
    id bigserial primary key,
    bucket_start_epoch_millis bigint not null,
    bucket_end_epoch_millis bigint not null,
    bucket_granularity varchar(16) not null,
    item_key varchar(255) not null,

    segment_start_epoch_millis bigint not null,
    segment_end_epoch_millis bigint not null,
    representative_snapshot_ts bigint,
    peak_snapshot_ts bigint,

    sample_count integer not null,
    anomaly_score double precision not null,
    reason_code varchar(64) not null,
    fragmented boolean not null default false,

    median_bin_lowest5_mean double precision,
    median_bin_p50 double precision,
    median_bin_p95 double precision,
    median_bid_p50 double precision,
    median_bin_count double precision,
    median_ending_soon_count double precision,

    created_at_epoch_millis bigint not null,
    updated_at_epoch_millis bigint not null
);
```

## Metrics Used For Detection

### Bazaar

Derived fields per sample:

- `mid_price = (buy_price + sell_price) / 2` when both exist and are positive
- `relative_spread = (buy_price - sell_price) / mid_price`
- `liquidity = min(buy_volume, sell_volume)`
- `depth_balance = min(buy_volume, sell_volume) / max(buy_volume, sell_volume)` when both sides exist

Detection metrics:

- `log(mid_price)`
- `relative_spread`
- `log1p(liquidity)`
- optional `depth_balance`

### Auction House

Prefer robust aggregate metrics over raw single-listing extremes.

Detection metrics:

- `log(bin_lowest5_mean)` as the main price anchor
- `log(bin_p50)` as secondary price anchor
- `log(bin_p95)` for upper-tail confirmation
- `log1p(bin_count)`
- `log1p(ending_soon_count)`

Avoid using `bin_lowest` alone as the primary anomaly detector because it is too sensitive to one listing.

## Candidate Validity Filter

A sample is eligible for normal statistics only if all of these hold:

- required fields for the market type are present
- price fields are positive
- the item identifier is not blank
- the sample timestamp is inside the bucket

Mark the bucket `partial=true` if:

- `valid_sample_count < min_valid_samples`, or
- `valid_sample_count / expected_sample_count < min_bucket_coverage_ratio`

Recommended starting values:

- `min_valid_samples = 3` for `1m`
- `min_valid_samples = 4` for `2h`
- `min_valid_samples = 2` for `1d`
- `min_bucket_coverage_ratio = 0.35`

## Anomaly Detection Rule

Use robust statistics per item and bucket:

1. Compute the per-metric median.
2. Compute `MAD` for each metric.
3. Convert each sample to a robust z-score:

`robust_z = 0.6745 * (x - median) / max(MAD, epsilon)`

4. Mark a sample as an anomaly candidate only if both are true:

- robust z-score crosses threshold
- absolute or relative movement also crosses a hard floor

This prevents tiny low-variance buckets from creating fake anomalies.

### Recommended Initial Thresholds

Bazaar:

- `abs(robust_z(log(mid_price))) >= 6.0`
- and `abs(mid_price / median_mid_price - 1.0) >= 0.08`

or:

- `robust_z(relative_spread) >= 6.0`
- and `relative_spread >= median_spread * 2.0`

or:

- `robust_z(log1p(liquidity)) <= -6.0`
- and `liquidity <= median_liquidity * 0.25`

Auction House:

- `abs(robust_z(log(bin_lowest5_mean))) >= 6.0`
- and `abs(bin_lowest5_mean / median_bin_lowest5_mean - 1.0) >= 0.10`

or:

- `abs(robust_z(log(bin_p50))) >= 6.0`
- and `abs(bin_p50 / median_bin_p50 - 1.0) >= 0.10`

or:

- `robust_z(log1p(bin_count)) <= -6.0`
- and `bin_count <= median_bin_count * 0.30`

### Persistence Rule

Do not split on a single isolated bad point.

Require at least one of:

- `2` consecutive anomalous samples
- anomaly duration `>= 10s`
- anomaly share `>= 20%` of samples in the bucket

Adjacent anomaly candidates with gap `<= 5s` should be merged into one anomaly run.

## Segmenting Rule

For the first implementation, allow at most one explicit anomaly run per item per bucket.

This yields up to three logical segments:

- `pre-normal`
- `anomaly`
- `post-normal`

If multiple disjoint anomaly runs appear in the same item and bucket:

- persist one anomaly segment covering the union
- set `fragmented=true`
- set `reason_code = 'MULTI_ANOMALY_CLUSTER'`

This keeps the first version deterministic and avoids unbounded fragmentation.

## Representative Snapshot Selection

Representative snapshots are for replay, debugging, and API parity. They are not the main statistics.

### Normal Rollup Representative

Choose the medoid of the non-anomalous samples:

1. compute the bucket median vector for the key metrics
2. normalize each metric by MAD or a safe scale floor
3. choose the real sample with the smallest total normalized distance
4. tie-break by earliest timestamp

### Anomaly Segment Representative

Persist:

- `representative_snapshot_ts`: medoid of anomaly samples
- `peak_snapshot_ts`: highest anomaly-score sample

This allows both "typical anomaly view" and "worst anomaly point".

## Compactor Flow

### Phase A: Raw Ingestion

Writers continue to insert:

- raw `market_snapshot`
- `ah_item_snapshot`
- `bz_item_snapshot`

### Phase B: Bucket Materialization

For each closed bucket older than the watermark:

1. load raw AH or BZ samples for the bucket
2. group by item
3. validate samples
4. detect anomaly candidates
5. merge anomaly candidates into one anomaly run if the persistence rule is met
6. compute normal rollup metrics from the non-anomalous samples
7. compute anomaly segment metrics from the anomalous samples
8. choose representative snapshot references
9. upsert rollup row
10. upsert anomaly segment row if present

### Phase C: Verification

A raw partition can be dropped only if all are true:

- every expected bucket in the partition time range is materialized
- rollup row count is within an acceptable ratio of distinct raw item count
- no bucket is still within watermark
- no bucket is marked in failed or in-progress state
- optional parity query passes for a sample set

### Phase D: Partition Drop

After verification:

- drop the raw source partition
- keep the rollup tables and anomaly segment tables

## Materialization Metadata

Add a small state table so drop decisions are explicit instead of inferred:

```sql
create table item_bucket_materialization_state (
    bucket_start_epoch_millis bigint not null,
    bucket_end_epoch_millis bigint not null,
    bucket_granularity varchar(16) not null,
    market_type varchar(8) not null,
    source_partition varchar(255) not null,
    finalized boolean not null default false,
    failed boolean not null default false,
    raw_row_count bigint not null default 0,
    rollup_row_count bigint not null default 0,
    anomaly_row_count bigint not null default 0,
    finalized_at_epoch_millis bigint,
    updated_at_epoch_millis bigint not null,
    primary key (bucket_start_epoch_millis, bucket_granularity, market_type)
);
```

This table is the concrete gate before partition drop.

## Configuration Proposal

Suggested config keys:

```yaml
config:
  snapshot:
    rollup:
      enabled: true
      watermark-seconds: 30
      raw-retention-days: 7
      test-raw-retention-days: 1
      anomaly:
        enabled: true
        min-valid-samples-1m: 3
        min-valid-samples-2h: 4
        min-valid-samples-1d: 2
        min-bucket-coverage-ratio: 0.35
        z-threshold: 6.0
        bazaar-relative-price-floor: 0.08
        ah-relative-price-floor: 0.10
        spread-multiplier-threshold: 2.0
        liquidity-drop-threshold: 0.25
        ah-bin-count-drop-threshold: 0.30
        consecutive-samples-threshold: 2
        anomaly-duration-threshold-seconds: 10
        merge-gap-seconds: 5
        max-segments-per-item-per-bucket: 1
```

## Plain PostgreSQL Implementation Order

1. Add rollup and anomaly tables plus materialization state table.
2. Implement bucket readers over `ah_item_snapshot` and `bz_item_snapshot`.
3. Implement robust metric helpers:
   - median
   - MAD
   - percentile
   - winsorized mean
4. Implement anomaly detection for Bazaar first.
5. Implement anomaly detection for AH.
6. Implement bucket materialization job for `1m`.
7. Extend the same engine to `2h` and `1d`.
8. Gate partition drop on materialization state.
9. After validation, enable raw partition drop for the chosen horizon.

## TimescaleDB Track

TimescaleDB is a reasonable follow-up for `ah_item_snapshot` and `bz_item_snapshot`, but it should be treated as a separate infrastructure decision.

Good reasons to use it:

- native hypertables for time-series storage
- continuous aggregates for `1m`, `2h`, `1d`
- retention policies for raw chunks
- columnstore on older chunks
- percentile and sketch support via toolkit for better long-horizon rollups

Reasons not to make it the `v1.1` baseline immediately:

- extra operational dependency and cluster setup
- extension install plus restart requirement
- migration complexity while partitioning strategy is still in motion
- not all repo tables benefit equally

Recommended approach:

- `v1.1`: implement the PostgreSQL-first design above
- post-`v1.1` spike: evaluate TimescaleDB only for `ah_item_snapshot` and `bz_item_snapshot`
- do not move `flip` tables or `market_snapshot` JSON storage first

## Recommendation For This Branch

The branch should implement the PostgreSQL-first design first.

Concrete recommendation:

- use the current bucket ladder unchanged
- add item-local anomaly segments only on aggregate tables
- add a materialization state gate before partition drop
- make raw partition retention configurable with `1d` for test and `7d` as the target default
- keep TimescaleDB as a separate spike after the compactor path is proven

## Review Questions

1. Is one anomaly segment per item per bucket enough for `v1.1`, or do we want multiple explicit segments immediately?
2. Should the raw retention default start at `1d` or `7d` in the first production rollout?
3. Do we want to materialize `1m` only first, or all `1m`, `2h`, and `1d` in the first implementation?
4. For AH, should `bin_lowest5_mean` be the primary anchor, or should `bin_p50` be the primary anchor?
5. Should anomaly segments be surfaced in the public API immediately, or kept internal first?
