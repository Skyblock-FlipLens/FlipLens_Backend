# Unified Flip Storage Migration Instructions (No Endpoint Changes)

## Goal

Migrate from snapshot-row flip persistence (`flip` per snapshot) to a scalable model:

- stable `flip_key` identity
- latest-state table for fast reads
- compressed trend segments for history
- optional per-snapshot top-K archive
- optional snapshot item state table

while keeping all current API endpoints and response DTOs unchanged.

Current API contracts to preserve:

- `GET /api/v1/flips`
- `GET /api/v1/flips/{id}`
- `GET /api/v1/flips/filter`
- `GET /api/v1/flips/top/*`
- `GET /api/v1/flips/stats`
- `GET /api/v1/snapshots/{snapshotEpochMillis}/flips`

## Compatibility Rule

No controller contract changes. Compatibility is solved in service/repository and storage layers.

`UnifiedFlipDto` stays unchanged.

## Phase 1: Introduce New Schema (Additive Only)

Create new Flyway migrations (do not modify `V1__init.sql`).

Recommended tables:

1. `flip_definition`
- `flip_key` `varchar(128)` primary key
- `stable_flip_id` `uuid` unique not null
- `flip_type` `varchar(...)` not null
- `result_item_id` `varchar(...)` not null
- `steps_json` `text` not null
- `constraints_json` `text` not null
- `key_version` `integer` not null
- `created_at_epoch_millis` `bigint` not null
- `updated_at_epoch_millis` `bigint` not null

2. `flip_current`
- `flip_key` primary key references `flip_definition(flip_key)`
- `snapshot_timestamp_epoch_millis` `bigint` not null
- metric columns used by `UnifiedFlipDto`: `required_capital`, `expected_profit`, `roi`, `roi_per_hour`, `duration_seconds`, `fees`, `liquidity_score`, `risk_score`, `partial`
- `partial_reasons_json` `text` not null default `'[]'`
- `updated_at_epoch_millis` `bigint` not null

3. `flip_trend_segment`
- `id` `bigserial` primary key
- `flip_key` references `flip_definition(flip_key)` not null
- `valid_from_snapshot_epoch_millis` `bigint` not null
- `valid_to_snapshot_epoch_millis` `bigint` not null
- same metric columns as `flip_current`
- `sample_count` `integer` not null default `1`

4. Optional `flip_top_snapshot`
- `snapshot_timestamp_epoch_millis` `bigint` not null
- `rank` `integer` not null
- `flip_key` not null
- materialized metrics columns
- primary key `(snapshot_timestamp_epoch_millis, rank)`

5. Optional `snapshot_item_state`
- normalized per-item market state for recomputation and analytics
- index `(item_id, snapshot_timestamp_epoch_millis)`

Indexes:

- `flip_current`: `(snapshot_timestamp_epoch_millis)`, `(expected_profit desc)`, `(roi desc)`, `(roi_per_hour desc)`, `(liquidity_score desc)`, `(risk_score asc)`
- `flip_trend_segment`: `(flip_key, valid_from_snapshot_epoch_millis)`
- `flip_top_snapshot`: `(snapshot_timestamp_epoch_millis, rank)`

## Phase 2: Stable Identity (`flip_key`) and Stable `/flips/{id}`

Implement deterministic `flip_key` generation from:

- `flipType`
- normalized ordered steps
- normalized constraints
- result/output identity
- key schema version

Normalization requirements:

- canonical JSON ordering for `paramsJson`
- include explicit step order
- include constraint type and values
- string normalization (`trim`, consistent case where required)

Stable ID mapping:

- derive `stable_flip_id` deterministically from `flip_key` (UUID v5 or equivalent deterministic mapping)
- persist in `flip_definition`

Result:

- `GET /api/v1/flips/{id}` remains valid and stable across snapshots
- no generated random IDs for the same logical flip

## Phase 3: Dual-Write in Generation Pipeline

Update generation flow in `FlipGenerationService`:

1. compute flips as today
2. build `flip_key` for each computed flip
3. upsert `flip_definition`
4. upsert `flip_current` with latest metrics
5. update `flip_trend_segment` using compaction rule
6. optionally write top-K rows to `flip_top_snapshot`
7. keep existing writes to legacy `flip` table during transition window

Use feature flags:

- `config.flip.storage.dual-write-enabled=true`
- `config.flip.storage.read-from-new=false` initially

## Phase 4: Read Path Switch (No Endpoint Changes)

Switch `FlipReadService` internals from legacy `flip` table to new tables.

Mapping strategy by endpoint:

1. `/api/v1/flips`, `/filter`, `/top/*`
- source: `flip_current` + `flip_definition`
- sort/filter in SQL first, then map to `UnifiedFlipDto`

2. `/api/v1/flips/{id}`
- resolve by `stable_flip_id` in `flip_definition`
- merge with `flip_current` metrics

3. `/api/v1/flips/stats`
- compute counts from `flip_current` grouped by type for latest
- for requested snapshot, use `flip_top_snapshot` or on-demand compute path

4. `/api/v1/snapshots/{snapshotEpochMillis}/flips`
- compatibility mode:
  - first try exact stored snapshot materialization (`flip_top_snapshot` if endpoint is top-only use case, or dedicated short-lived `flip_snapshot_result` table if full list is required)
  - fallback to on-demand recomputation from snapshot data when exact list is not materialized

Important:

If exact full-list behavior for arbitrary old snapshots must remain, keep a bounded `flip_snapshot_result` table (partitioned + retention) or accept slower recomputation fallback.

## Phase 5: Trend Segment Compaction Logic

For each new computed metric set:

- find current open/latest segment for `flip_key`
- compare against segment reference metrics
- if changes are below thresholds, extend `valid_to_snapshot_epoch_millis`
- else close segment and create new segment

Suggested default thresholds:

- profit relative change: `< 5%`
- ROI/h relative change: `< 5%`
- liquidity absolute delta: `< 3 points`
- risk absolute delta: `< 3 points`

Tune per flip type later.

## Phase 6: Cutover and Legacy Cleanup

After parity checks pass:

1. set `read-from-new=true`
2. keep dual-write for 1-2 weeks
3. compare endpoint outputs old vs new (automated checks)
4. disable legacy write
5. deprecate legacy `flip` snapshot-heavy reads
6. eventually drop legacy tables in a later migration

### Operational Cutover Flags

Recommended runtime sequence:

1. Baseline dual-write (no read switch):
- `CONFIG_FLIP_STORAGE_DUAL_WRITE_ENABLED=true`
- `CONFIG_FLIP_STORAGE_READ_FROM_NEW=false`
- `CONFIG_FLIP_STORAGE_LEGACY_WRITE_ENABLED=true`

2. Read cutover with rollback safety:
- `CONFIG_FLIP_STORAGE_DUAL_WRITE_ENABLED=true`
- `CONFIG_FLIP_STORAGE_READ_FROM_NEW=true`
- `CONFIG_FLIP_STORAGE_LEGACY_WRITE_ENABLED=true`

3. Legacy write disable (after sustained parity):
- `CONFIG_FLIP_STORAGE_DUAL_WRITE_ENABLED=true`
- `CONFIG_FLIP_STORAGE_READ_FROM_NEW=true`
- `CONFIG_FLIP_STORAGE_LEGACY_WRITE_ENABLED=false`

Before step 2, run one-time backfill if historical legacy data exists but unified tables are empty:

- `POST /internal/admin/instrumentation/flip-storage/backfill/latest`

### Admin Parity Endpoints

Use secured internal endpoints during rollout:

- `GET /internal/admin/instrumentation/flip-storage/config`
- `GET /internal/admin/instrumentation/flip-storage/parity/latest`
- `POST /internal/admin/instrumentation/flip-storage/backfill/latest`
- `POST /internal/admin/instrumentation/flip-storage/backfill/{snapshotEpochMillis}`

## Code Areas To Touch

- `src/main/java/com/skyblockflipper/backend/service/flipping/FlipGenerationService.java`
- `src/main/java/com/skyblockflipper/backend/service/flipping/FlipReadService.java`
- `src/main/java/com/skyblockflipper/backend/repository/FlipRepository.java` (legacy, transition)
- add new repositories for `flip_definition`, `flip_current`, `flip_trend_segment`, optional `flip_top_snapshot`
- add key builder utility (for deterministic `flip_key`)
- add Flyway migration files under `src/main/resources/db/migration`

Controllers can stay unchanged:

- `src/main/java/com/skyblockflipper/backend/api/FlipController.java`
- `src/main/java/com/skyblockflipper/backend/api/SnapshotController.java`

## Testing and Parity Checklist

1. Unit tests
- deterministic `flip_key` generation
- deterministic `stable_flip_id` mapping
- segment compaction behavior around threshold boundaries

2. Integration tests
- dual-write persistence for same snapshot
- read endpoints return same DTO shape and required fields
- `/api/v1/flips/{id}` returns stable object across snapshot updates

3. Regression checks
- compare old vs new for:
  - top profit list
  - filtered list
  - per-type counts
  - stats endpoint values

4. Performance checks
- query latency for `/flips`, `/filter`, `/top/*`
- DB write duration per ingestion cycle
- row growth trend before/after migration

## Risks and Controls

Risk: bad key design causes false merges/splits.  
Control: versioned key schema + golden test vectors.

Risk: on-demand historical fallback is too slow.  
Control: materialize top-K and/or short-window snapshot flip results.

Risk: metric drift between old and new reads.  
Control: parity report job during dual-write period.

## Recommended Start Sequence

1. Add migrations (additive tables + indexes).
2. Implement `flip_key` + deterministic `stable_flip_id`.
3. Add dual-write in generator.
4. Add new read repositories and service adapter.
5. Enable read-from-new behind feature flag in staging.
6. Run parity and performance checks.
7. Production cutover with rollback flag.
