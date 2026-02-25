# Legacy Decommission Runbook (Internal, PR-08)

This runbook defines the mandatory process to disable legacy flip writes and prepare final legacy storage removal after sustained unified-storage parity.

Scope:
- Internal operations only.
- No public API contract changes.
- Applies after Phase 6 in `MarkdownFiles/UNIFIED_FLIP_MIGRATION_INSTRUCTIONS.md`.

Related docs:
- `MarkdownFiles/UNIFIED_FLIP_MIGRATION_INSTRUCTIONS.md`
- `MarkdownFiles/API_ENDPOINTS.md`
- `MarkdownFiles/SNAPSHOT_RETENTION_POLICY.md`
- `MarkdownFiles/RUNTIME_BLOCKING_INSTRUMENTATION.md`

## 1. Preconditions (Go/No-Go)

All items are required:

- Read path already switched to unified storage:
  - `CONFIG_FLIP_STORAGE_DUAL_WRITE_ENABLED=true`
  - `CONFIG_FLIP_STORAGE_READ_FROM_NEW=true`
  - `CONFIG_FLIP_STORAGE_LEGACY_WRITE_ENABLED=true`
- Sustained parity window completed:
  - Minimum `7` days, target `14` days.
  - Automated parity check cadence: every `5` minutes (or faster).
  - `parityOk=true` in at least `99.5%` of checks in the window.
  - No continuous parity failure streak longer than `15` minutes.
- Freshness and pipeline health:
  - Legacy/latest vs unified/latest snapshot delta `<= 15s`.
  - No unresolved ingestion backlog.
  - Snapshot compaction and retention running according to `SNAPSHOT_RETENTION_POLICY.md`.
- Runtime health guardrails:
  - No new sustained `5xx` regression on `/api/v1/flips*` and `/api/v1/snapshots/*`.
  - No sustained polling-cycle blocked-time regression (see `RUNTIME_BLOCKING_INSTRUMENTATION.md`).

If any precondition fails, do not decommission legacy writes.

## 2. Required Evidence Package

Store evidence in the internal incident/change record before execution:

- Latest `GET /internal/admin/instrumentation/flip-storage/config` response.
- Last `24h` parity summary + full parity window aggregate.
- Snapshot freshness chart/log extract (legacy vs unified markers).
- Public read endpoint error-rate and latency snapshot (`/api/v1/flips*`, `/api/v1/snapshots/*`).
- Explicit approvers:
  - On-call engineer
  - Backend owner for flip storage

## 3. Change Window Procedure

Run in a low-risk deploy window with rollback operator available.

1. Freeze unrelated production changes for the window.
2. Re-check current cutover config and parity immediately before change:
   - `GET /internal/admin/instrumentation/flip-storage/config`
   - `GET /internal/admin/instrumentation/flip-storage/parity/latest`
3. Apply decommission flag change:
   - Keep:
     - `CONFIG_FLIP_STORAGE_DUAL_WRITE_ENABLED=true`
     - `CONFIG_FLIP_STORAGE_READ_FROM_NEW=true`
   - Change:
     - `CONFIG_FLIP_STORAGE_LEGACY_WRITE_ENABLED=false`
4. Wait `2` polling cycles (>= `10s`) and rerun:
   - `GET /internal/admin/instrumentation/flip-storage/config`
   - `GET /internal/admin/instrumentation/flip-storage/parity/latest`
5. Execute smoke checks (must pass):
   - `GET /api/v1/flips`
   - `GET /api/v1/flips/{id}` (known stable ID)
   - `GET /api/v1/flips/filter` (representative filter)
   - `GET /api/v1/flips/stats`
   - `GET /api/v1/snapshots/{snapshotEpochMillis}/flips` (recent snapshot)
6. Start heightened monitoring for `24h`:
   - parity trend
   - snapshot freshness
   - error rate and latency
   - blocked-time metrics

## 4. Rollback Guardrails (Immediate Action)

Trigger immediate rollback if any condition occurs post-change:

- `parityOk=false` for `2` consecutive parity checks.
- Any non-zero `only-legacy` or `only-current` count persisting for `>= 10` minutes.
- Legacy vs unified snapshot timestamp delta `> 30s` for `>= 10` minutes.
- `/api/v1/flips*` or `/api/v1/snapshots/*`:
  - sustained `5xx` increase above baseline by `>= 1%` for `10` minutes, or
  - sustained p95 latency regression `>= 20%` for `15` minutes.

Rollback sequence:

1. Re-enable legacy write immediately:
   - `CONFIG_FLIP_STORAGE_LEGACY_WRITE_ENABLED=true`
2. If user-facing impact continues, revert read path:
   - `CONFIG_FLIP_STORAGE_READ_FROM_NEW=false`
   - Keep `CONFIG_FLIP_STORAGE_DUAL_WRITE_ENABLED=true`
3. Re-run parity and config endpoints.
4. Capture incident notes, timestamp, and mismatched IDs/metrics samples.

## 5. Post-Cutover Hold and Final Removal

Legacy table drop is a separate follow-up change and must not occur in the same window.

Required hold after successful decommission:

- Keep legacy schema intact for at least `7` additional days (recommended `14`).
- Keep parity instrumentation accessible for validation and audits.
- Perform one final daily parity review during hold period.

Only after the hold period:

1. Create a dedicated migration PR to remove legacy tables/indexes.
2. Include rollback strategy for schema removal (backup/restore plan).
3. Execute with a separate approval gate.

## 6. Operational Checklist

Use this as the execution checklist:

- [ ] Preconditions fully satisfied (Section 1).
- [ ] Evidence package attached (Section 2).
- [ ] Change window staffed and rollback owner assigned.
- [ ] Pre-change config/parity checks executed.
- [ ] `CONFIG_FLIP_STORAGE_LEGACY_WRITE_ENABLED=false` applied.
- [ ] Post-change config/parity checks executed.
- [ ] Public endpoint smoke tests passed.
- [ ] 24h heightened monitoring completed without rollback triggers.
- [ ] Post-cutover hold period started and tracked.
- [ ] Follow-up legacy schema removal planned as separate PR.
