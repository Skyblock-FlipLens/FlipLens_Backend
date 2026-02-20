# Snapshot Retention Policy (Canonical)

This document defines how snapshots are ingested, retained, compacted, and deleted.
It is aligned with `MarkdownFiles/Risk&Liquidity_Scores.md` and keeps its non-negotiable UTC/`Instant` rules.

---

## 1. Ingestion Cadence

- Run the market ingestion loop every `5` seconds.
- Persist one snapshot per loop tick.
- Source HTTP fetching is adaptive and independent from snapshot cadence:
  - Auctions: base fetch interval `60s`.
  - Bazaar: base fetch interval `20s`.
  - If `lastUpdated` does not advance, the endpoint fetch interval is increased up to a bounded maximum.
  - If new data arrives, the endpoint fetch interval resets to its base interval.
  - Intermediate 5s snapshots reuse the latest successful payload.

---

## 2. Time Semantics (Non-Negotiable)

- All snapshot timestamps use `java.time.Instant`.
- Daily semantics are UTC-only and timezone-independent.
- Day key:
  - `epochDay = floor(snapshotInstant.getEpochSecond() / 86400)`
- For daily anchors, keep exactly one snapshot per `epochDay`: the first snapshot in that UTC day.

Do not use:
- `LocalDate.now()` for day semantics.
- `ZonedDateTime.systemDefault()`.
- Location timezones (for example `Europe/Vienna`) in retention decisions.

---

## 3. Retention Ladder by Age

Age is calculated as:
- `ageSeconds = nowInstant.getEpochSecond() - snapshotInstant.getEpochSecond()`

Retention tiers:

1. `0s` to `90s`:
- Keep all 5s snapshots.

2. `>90s` to `30m`:
- Keep one representative per 1-minute compaction slot.
- Practical effect: after the full-resolution window, only 1-minute spacing remains (for example around `2m30s`, `3m30s`, ...).

3. `>30m` to `12h`:
- Keep one representative per 2-hour compaction slot.

4. `>12h`:
- Keep daily anchors only:
  - one snapshot per UTC `epochDay`,
  - specifically the first snapshot of that day.

All snapshots that are not selected by the active tier rules are deleted.

---

## 4. Deletion Rule

Compaction is deterministic:

- For each tier, compute its slot key.
- Keep exactly one snapshot per slot key (tie-breaker: earliest timestamp in the slot).
- Delete all other snapshots in that tier range.

For the daily tier, the slot key is `epochDay` and the keeper is the first snapshot in that UTC day.

---

## 5. Scoring Usage

Storage retention and scoring horizons are related but not identical:

- Execution liquidity/risk uses latest snapshot.
- Micro features use recent 5s series (last 60s window).
- Macro features use daily anchors.
- Intermediate compacted tiers (1-minute and 2-hour) are for storage efficiency, diagnostics, and optional analytics, not as independent score votes.

---

## 6. Operational Guidance

- Run compaction on a frequent schedule (for example every 30-60 seconds).
- Always compact from newest to oldest tier boundaries to avoid accidental keeper deletion.
- Keep configuration externalized:
  - `rawWindowSeconds = 90`
  - `minuteTierUpperSeconds = 1800`
  - `twoHourTierUpperSeconds = 43200`
  - `minuteIntervalSeconds = 60`
  - `twoHourIntervalSeconds = 7200`

---

## 7. Conflict Resolution

If another document conflicts with this one, keep these invariants:

1. UTC `Instant` semantics only.
2. Daily retention key is `epochDay`.
3. Daily keeper is the first snapshot in each UTC day.
4. Micro-scoring does not treat many snapshots as independent votes.

Hinweis: Endpoint-Details stehen zentral in `MarkdownFiles/API_ENDPOINTS.md` (siehe Market Overview).

