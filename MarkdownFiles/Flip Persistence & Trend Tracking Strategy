# Flip Persistence & Trend Tracking Strategy (Snapshot-Driven)

> This document proposes a storage + computation strategy for flip opportunities derived from market snapshots.
> It is designed to keep the UI fast (Top/All lists), enable “improving vs worsening” trend tracking, and prevent unbounded DB growth.

## Context

FlipLens computes *flip opportunities* from *market snapshots*.
- Items are mostly stable (update only on game/NEU changes).
- Snapshots represent market state at a specific point in time.
- Flips are derived from snapshots (potentially **many** per item per snapshot).

The backend already has a clear pipeline vision (ingestion → normalization → computation → persistence → API) and a snapshot pipeline with retention/compaction and feature extraction goals. :contentReference[oaicite:0]{index=0}

## Problem Statement

Persisting *all* flips per snapshot scales poorly:
- Even with a moderate number of snapshots, the cross product `snapshots × items × generated_flips_per_item` explodes.
- We still need to observe how a flip changes over time (“better/worse”), not just the current best list.

## Goals

1. **Fast read API** for flip lists and details (`/api/v1/flips` and `/api/v1/flips/{id}`). :contentReference[oaicite:1]{index=1}
2. **Trend tracking**: quantify improvement/deterioration for a stable flip identity across snapshots.
3. **Bounded storage growth** (no “13M rows every 2 days” situation in perpetuity).
4. Preserve **high fidelity** for recent data and “best flips”, while allowing older data to be compressed.

## Non-Goals

- Storing every intermediate computation artifact forever.
- Building a full OLAP warehouse in the runtime DB (can be a future extension).

---

## Core Idea: Stable Flip Identity (`flip_key`)

To track improvement/deterioration, we must identify “the same flip” across snapshots.

Define a deterministic `flip_key` derived from the flip definition:
- `flipType`
- ordered step/recipe identifiers
- involved input/output item ids
- constraint identifiers + relevant parameters (fees, durations, etc.)

This matches the backend’s domain concept of `Flip`, `Step`, `Constraint`, `Recipe`. :contentReference[oaicite:2]{index=2}

**Rule:** If two derived flips should be compared over time, they must share the same `flip_key`.

---

## Proposed Storage Model (3 layers)

### Layer 1 — Snapshot Item State (authoritative, compact)
Store full market snapshot state per item:
- `snapshot`
- `snapshot_item_state(snapshot_id, item_id, buy, sell, volume, …)`

This is the minimal truth you need for:
- item charts
- recomputation
- validation/backtesting
- feature extraction (risk/liquidity inputs) :contentReference[oaicite:3]{index=3}

### Layer 2 — Flip Current State (fast list reads)
Maintain one row per `flip_key` representing the latest snapshot result:
- `flip_current(flip_key PK, last_snapshot_id, metrics…, updated_at)`

Write path on new snapshot:
1. compute flips
2. `UPSERT` into `flip_current`

This table directly backs “Top Flips” / “All Flips” sorting/filtering by profit/ROI/capital/liquidity/risk.

### Layer 3 — Flip Trend History (compressed, older data)
Instead of storing every flip result for every snapshot, store **trend segments**:

- `flip_trend_segment(flip_key, valid_from_snapshot, valid_to_snapshot, metrics_ref, stats…)`

A segment represents a time interval where the flip metrics did not change “meaningfully”.

#### Segment compaction rule (example)
For a new computed value `v_new` and last segment reference `v_ref`:
- if `abs(v_new - v_ref) / max(|v_ref|, eps) < 0.05` **and** other key metrics remain within thresholds,
  - extend the current segment (`valid_to_snapshot = current_snapshot`)
- else
  - close the segment and start a new one (new `metrics_ref = v_new`)

This preserves:
- exact recent values (Layer 2)
- meaningful changes as distinct segments (Layer 3)
- a clear UI message: “no significant change in this interval”

---

## Optional: Top-K Per Snapshot (high-fidelity for “best flips”)
To keep “best flips” historically exact without storing everything:
- `flip_top_snapshot(snapshot_id, rank, flip_key, metrics…)`

Populate this with a configurable policy, e.g.:
- global Top 2,000 by profit/ROI-h
- per-type Top N
- per-item Top N

This makes “best flips over time” exact and queryable at low cost.

---

## On-Demand Computation (where it fits)

Pure on-demand generation for “All Flips” is risky due to the large search space.
However, on-demand is excellent for:
- item drilldowns (“show flips for Item X”)
- flip explanation pages (“show steps/recipe and current math”)
- ad-hoc analytics

Recommended hybrid:
- lists come from `flip_current` / `flip_top_snapshot`
- details can recompute on-demand from `snapshot_item_state` if needed (or verify cached metrics)

---

## Query Patterns (what we optimize for)

1. **All flips list** (paged) with filters and sorting:
   - read from `flip_current`
2. **Top flips**:
   - either `flip_current` with `ORDER BY` + LIMIT
   - or `flip_top_snapshot` for exact snapshot-based leaderboards
3. **Trend chart for a flip**:
   - read from `flip_trend_segment` and render segments
4. **Item trends**:
   - read from `snapshot_item_state`

---

## Postgres Implementation Notes

### Partitioning
If any table becomes append-heavy (e.g. `flip_top_snapshot` or raw “flip results” if kept):
- partition by snapshot time (daily/weekly)
- retention via `DROP PARTITION` (avoid massive DELETE + VACUUM)

### Indexing (minimal and purposeful)
- `flip_current`: indexes supporting top-list sort/filter (choose based on UI)
- `flip_trend_segment`: `(flip_key, valid_from_snapshot)` for charting
- `snapshot_item_state`: `(item_id, snapshot_id)` for time-series per item

### Writes
Avoid per-row JPA writes for massive batches:
- use batching (hibernate batch settings)
- consider COPY-like bulk loading for snapshot/item state or top lists

---

## UI Mapping

- **All Flips / Top Flips:** read from `flip_current` (fast)
- **“How did this flip change?”** read segments from `flip_trend_segment`
- **Historical leaderboards:** read `flip_top_snapshot`
- **Item page charts:** read `snapshot_item_state`

---

## Rollout Plan

1. Implement `flip_key` generation and persist it everywhere a flip is identified.
2. Create `flip_current` and update the writer job to `UPSERT` latest metrics.
3. Create `flip_trend_segment` and implement the compaction rule.
4. (Optional) Add `flip_top_snapshot` to preserve exact “best flips” history.
5. Add API endpoints for:
   - trend segments per flip
   - (optional) top flips per snapshot

---

## Risks & Mitigations

- **Bad flip_key design** → incorrect trend comparisons  
  Mitigation: versioned key schema + unit tests on canonical examples.

- **Over-aggressive compaction hides meaningful changes**  
  Mitigation: tune thresholds per metric; store both profit and ROI-h thresholds; allow “strict mode” for selected flip types.

- **Write amplification** from too many indexes  
  Mitigation: keep hot-path tables narrow; add indexes only for real UI queries.

---

## Summary

Persist snapshots and item state as the authoritative truth, keep a fast “current flip” table for UI lists, and store compressed trend segments for historical change tracking. This yields:
- fast list endpoints
- accurate “better/worse” tracking
- bounded storage growth
- a clear pathway to richer analytics (risk/liquidity) later :contentReference[oaicite:4]{index=4}
