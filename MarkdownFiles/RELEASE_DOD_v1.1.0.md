# Definition of Done (DoD) – FlipLens Backend v1.1.0

## 1. Purpose
This DoD defines when work is truly "Done" for the v1.1.0 release stream.
A work item or release is only Done if all applicable criteria below are met with evidence.

## 2. Scope Levels
This DoD has two levels:

1. Increment DoD (applies to every merged change / PBI)
2. Release DoD (applies before publishing v1.1.0)

---

## 3. Increment DoD (Every PBI)

### 3.1 Code & Design
- [ ] Code compiles on CI (`mvn -q -DskipTests compile`).
- [ ] No known regression in affected modules.
- [ ] Logging is actionable (error context, no sensitive data).
- [ ] Config flags/defaults are explicit and documented if changed.

### 3.2 Tests
- [ ] New logic has unit/integration tests, or explicit rationale why not.
- [ ] Existing relevant tests pass.
- [ ] Coverage does not reduce project gate compliance.
- [ ] For non-core infra classes (for example Flyway migrations), excluded from coverage only if annotated and justified.

### 3.3 Database & Migrations
- [ ] Migration is idempotent where possible.
- [ ] Migration failure behavior is safe (clear error, no hidden partial behavior).
- [ ] Lock/timeout behavior is explicitly controlled for expensive operations.
- [ ] Connection/session settings modified by code are restored before returning pooled connections.

### 3.4 Operability
- [ ] Actuator/admin behavior remains compatible.
- [ ] Failure paths are isolated (component-level errors do not crash app).
- [ ] New feature is profile-scoped correctly (`compactor` vs `!compactor`) where intended.

### 3.5 Security & Safety
- [ ] No secrets/tokens in code or logs.
- [ ] Internal endpoints remain guarded as intended.
- [ ] No unsafe shell/tool dependency in distroless runtime paths.

---

## 4. Release DoD (v1.1.0)

## 4.1 Performance Goal – API on Release Baseline Hardware
"Performance is achieved" when all criteria below are true on target profile.

### Hardware profile (release baseline)
Performance validation must pass on the currently used production-like split setup:

- Database host (NAS):
  - CPU: Intel(R) Core(TM) i7-7700 @ 3.60GHz
  - RAM: 16 GB DDR4 (3200 MHz)
  - Storage layout:
    - Database data volume: 2 TB M.2 SSD
    - OS boot volume: separate SATA SSD

- API/Compactor host:
  - Device: Dell OptiPlex 3070
  - CPU: Intel(R) Core(TM) i5-9500T (6 cores) @ up to 3.70GHz
  - RAM: 32 GB

This hardware baseline is the minimum acceptance target for v1.1.0.
Any release candidate is "performance done" only if all performance criteria are met on this setup.

### Load profile (baseline)
- Representative mixed read load + background compactor activity.
- Run duration: minimum 24h continuous.

### Acceptance criteria
- [ ] API availability >= 99.5% during run window.
- [ ] p95 latency for core read endpoints <= agreed threshold (set numeric targets per endpoint).
- [ ] p99 latency for core read endpoints <= agreed threshold.
- [ ] 5xx error rate <= 0.5% (excluding intentional fault-injection windows).
- [ ] No unbounded queue/backlog growth.
- [ ] No sustained CPU saturation (>90%) for >5 minutes continuously.
- [ ] No sustained DB lock/wait storm causing user-visible degradation.

Evidence:
- Dashboard export + raw metrics snapshot + test run metadata archived.

---

## 4.2 Compactor & DB Stability Goal
- [ ] Compactor completes cycles without deadlock/livelock.
- [ ] Diagnostics snapshots are produced on schedule (default 60s).
- [ ] `pg_stat_activity` shows no persistent long-running blocked compactor queries.
- [ ] DB wait summary does not show persistent WAL/lock pressure beyond defined threshold.
- [ ] `pg_stat_statements` top offenders reviewed and no unmitigated critical query remains.
- [ ] Required performance indexes exist in production DB:
  - `idx_flip_snapshot_ts`
  - `idx_flip_step_flip_id`
  - `idx_flip_constraints_flip_id`

Evidence:
- Diagnostics JSON logs, DB snapshots, migration history table, and explain plans for critical queries.

---

## 4.3 Prediction Quality Goal
"Prediction logic is acceptable" when the following are met.

### Offline/backtest
- [ ] Backtest run is reproducible (fixed input snapshot set + versioned config).
- [ ] Minimum metric thresholds met (define numeric targets):
  - Precision / hit-rate
  - Recall (if relevant to strategy)
  - Expected vs realized profit gap
  - Calibration quality by confidence bucket
- [ ] Performance by segment (item class / liquidity tier) reviewed.

### Live shadow validation
- [ ] Shadow mode runs for a defined window (for example 7 days).
- [ ] No severe drift from offline expectations beyond tolerance band.
- [ ] Alerting for prediction degradation is active.

Safety gates:
- [ ] If confidence/calibration drifts beyond threshold, strategy degrades safely (feature flag / conservative filter).

Evidence:
- Backtest report, shadow report, signed-off threshold table.

---

## 4.4 Observability & Incident Readiness
- [ ] Actuator health/metrics reachable in deployment topology.
- [ ] Diagnostics endpoint available for compactor profile.
- [ ] Alerts configured for:
  - API error-rate spike
  - p95 latency breach
  - compactor run failure
  - DB lock/wait anomaly
- [ ] Runbook exists for top 5 incidents:
  - DB lock contention
  - WAL/IO pressure
  - compactor stuck or repeated failure
  - migration timeout/failure
  - prediction drift incident

---

## 4.5 Release Operations
- [ ] Flyway migration validated on staging with production-like data shape.
- [ ] Rollback strategy documented (app rollback + DB contingency).
- [ ] Version/tag/changelog prepared.
- [ ] Final smoke test passed after deployment rehearsal.

---

## 5. Ownership & Sign-off
Required sign-offs before release tag:
- [ ] Backend owner
- [ ] Data/model owner
- [ ] Ops/platform owner

Each sign-off includes:
- Date
- Evidence links
- Explicit Go / No-Go decision

---

## 6. DoD Maintenance
- DoD is reviewed at each retrospective.
- Any recurring failure mode becomes a new DoD criterion.
- Criteria must remain actionable, binary, and achievable within sprint/release cadence.
