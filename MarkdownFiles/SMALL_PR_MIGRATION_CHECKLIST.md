# Small PR Migration Checklist (Internal Working Doc)

Goal: accurate recent flips, enough historical accuracy, controlled data growth, no expensive hardware requirement.

## Global Rules (always true)

- [ ] No public API contract changes (controllers/DTO fields/endpoint shapes stay unchanged).
- [ ] Schema changes are additive only in rollout PRs (no drop/rename in same release).
- [ ] Never edit `V1__init.sql`; add new versioned migrations only.
- [ ] Backfill jobs are idempotent and rerunnable.
- [ ] Rollback path exists for every cutover step (feature flag back to old read path).
- [ ] Parity checks pass before each cutover.

## PR-0: Baseline + Guardrails

- [ ] Add/confirm feature flags: `dual-write-enabled`, `read-from-new`, `legacy-write-enabled`.
- [ ] Add/confirm parity endpoint/report for old vs new reads.
- [ ] Add/confirm monitoring metrics for write/read parity and mismatch counters.
- [ ] Document rollout and rollback commands.

Exit criteria:
- [ ] Can switch read path without API contract change.
- [ ] Can detect mismatch quickly.

## PR-1: `flip_definition` (identity layer)

- [ ] Add Flyway migration for `flip_definition` + constraints/indexes.
- [ ] Implement deterministic `flip_key` generation.
- [ ] Implement deterministic `stable_flip_id` derivation from `flip_key`.
- [ ] Add upsert write path.
- [ ] Add tests for deterministic key/id (golden cases).

Exit criteria:
- [ ] Same logical flip always maps to same `flip_key` and `stable_flip_id`.

## PR-2: `flip_current` (hot read table)

- [ ] Add Flyway migration for `flip_current` + required FK to `flip_definition`.
- [ ] Add only query-driven indexes (`snapshot_timestamp`, top sort fields).
- [ ] Implement upsert writer.
- [ ] Keep reads on legacy path for now.

Exit criteria:
- [ ] Dual-write to legacy + new works with no write failures.

## PR-3: Backfill latest snapshot to new tables

- [ ] Add/enable backfill job/endpoint for latest snapshot.
- [ ] Ensure idempotency and conflict-safe upserts.
- [ ] Run parity checks on top/filter/counts/stats.

Exit criteria:
- [ ] New tables are not empty.
- [ ] Parity within accepted tolerance.

## PR-4: Read cutover (flagged)

- [ ] Switch read path internally to `flip_current` behind `read-from-new=true`.
- [ ] Keep dual-write on.
- [ ] Keep legacy-write on.
- [ ] Run smoke + parity checks in staging/prod canary.

Exit criteria:
- [ ] No API contract diff.
- [ ] No regression in latency/error budget.
- [ ] Rollback tested (`read-from-new=false`).

## PR-5: `flip_trend_segment` (compressed history)

- [ ] Add Flyway migration for `flip_trend_segment` + `(flip_key, valid_from...)` index.
- [ ] Implement compaction thresholds (profit/roi_per_hour/liquidity/risk).
- [ ] Add boundary tests (just below/above thresholds).

Exit criteria:
- [ ] Trend data generated deterministically.
- [ ] Storage growth remains bounded.

## PR-6: Optional history tables (only if needed)

- [ ] `flip_top_snapshot` only if exact historical top-N is required.
- [ ] `snapshot_item_state` only with bounded retention horizon and partition plan.
- [ ] `flip_snapshot_result` stays OFF by default; add only with explicit exact full-list replay requirement.

Exit criteria:
- [ ] Optional tables are justified by concrete use case and cost estimate.

## PR-7: Performance hardening

- [ ] Measure index write amplification on hot tables.
- [ ] Add BRIN/partitioning only where data volume proves benefit.
- [ ] Use batch/COPY for heavy ingest paths where applicable.

Exit criteria:
- [ ] Throughput and latency targets met on current hardware tier.

## PR-8: Legacy decommission (separate late PR)

- [ ] Keep parity stable for at least 1-2 weeks.
- [ ] Disable `legacy-write-enabled` after stable window.
- [ ] Keep rollback window documented.
- [ ] Only then plan/drop legacy tables in separate migration PR.

Exit criteria:
- [ ] No remaining dependency on legacy tables for live API behavior.

## Per-PR Definition of Done

- [ ] Migration scripts reviewed and reproducible.
- [ ] Backfill/idempotency verified (if applicable).
- [ ] Unit/integration tests added for new behavior.
- [ ] Monitoring/alerts updated.
- [ ] Rollback step written and tested.
- [ ] PR scope is small and single-purpose.
