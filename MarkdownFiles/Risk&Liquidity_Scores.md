# Multi-Timescale Snapshotting for Hypixel SkyBlock Bazaar Scoring (5s / 1m / daily)

This repo implements a flip scoring engine (Liquidity + Risk) for Hypixel SkyBlock Bazaar flips using public API data.  
To make scoring stable and resistant to micro-noise, the system stores **multiple snapshot layers** and derives metrics per layer.

---

## Snapshot Layers

### 1. Current / Micro Snapshots (every 5 seconds)

**Purpose:** near-real-time monitoring, microstructure signals, fast detection of regime changes.

- Every 5 seconds we fetch Bazaar quick_status data and write a `Snapshot5s`.
- Each snapshot contains:
  - `timestamp`
  - per-item `buyPrice`, `sellPrice`, `buyMovingWeek`, `sellMovingWeek`, `buyVolume`, `sellVolume`, `buyOrders`, `sellOrders`, etc.
- Retention:
  - keep a rolling window (e.g. **last 60–300 seconds**) using prune-by-age.

**Important:** 5s snapshots are *not* used as independent “votes” in scoring. They are used as a **time series** to compute *one* estimate per metric (e.g. rolling volatility). This prevents “many snapshots” from artificially amplifying any score component.

---

### 2. Minute Snapshot View (derived from 5s)

**Purpose:** stable short-horizon returns/volatility and fill-time stability without separate polling.

We do NOT poll separately every minute. We derive minute-level metrics from the 5s window:

- Define `mid = (buyPrice + sellPrice)/2`
- Use the 5s snapshots covering the last 60 seconds to compute:
  - **1m return**: `r_1m = ln(mid_now / mid_1m_ago)` (using a selected 1m-ago point)
  - **1m micro-volatility**: `sigma_1m = stdev( ln(mid_t / mid_{t-1}) )` across the 5s series in the window
  - Optional: min/max mid over the minute, spread statistics

**Boundary rule (recommended):**
- `mid_1m_ago` is the snapshot with timestamp **closest to (now - 60s)** (tie-breaker: earlier).
- If not available (startup), volatility confidence is downgraded.

---

### 3. Daily Snapshots (one chosen 5s snapshot per day)

**Purpose:** long-horizon baseline, drift detection, and “macro” risk that cannot be inferred from a short 5s/1m window.

Daily snapshots are stored as:
- A deterministic UTC bucket key: `epochDay = floor(snapshotInstant.getEpochSecond() / 86400)`.
- Exactly one `Snapshot1d` per `epochDay`, selected as the **first 5s snapshot in that UTC day**.
- Store as `Snapshot1d[epochDay]` (selected from the 5s stream, not separately polled).

From daily snapshots you can compute long-horizon features:
- **1d return**: `r_1d = ln(mid_today / mid_yesterday)`
- **rolling daily volatility** over N days: `sigma_1d(N)` using daily log returns
- **trend / regime**: moving averages, z-scores, anomaly flags

These features are designed to be robust against short-term manipulation/noise.

---

## Daily Snapshot Semantics (Using `java.time.Instant`)

Daily snapshots are stored using `java.time.Instant` and therefore operate in **absolute UTC time**.  
They are not based on local calendar dates and must never depend on system timezone.

### 1. Defining a Day (canonical)

`epochDay = floor(snapshotInstant.getEpochSecond() / 86400)`

- `snapshotInstant.getEpochSecond()` is UTC epoch seconds from `Instant`.
- `86400` is seconds per UTC day.

This day key is monotonic and independent of DST, server region, JVM timezone, and local calendar assumptions.

### 2. Selecting the Daily Snapshot

We do not poll separately once per day.  
Daily snapshots are derived from the 5-second stream:

- Compute `currentDay = epochDay(snapshot.instant)`.
- If `currentDay > lastStoredDay`, store this snapshot as `Snapshot1d[currentDay]`.

Equivalent replay/backfill rule:
- For each `epochDay`, keep the **earliest** snapshot timestamp in that bucket.

This guarantees deterministic daily anchors without requiring a midnight-exact poll.

### 3. Guardrails

- Do not use `LocalDate.now()` for day semantics.
- Do not use `ZonedDateTime.systemDefault()`.
- Do not use location timezones such as `Europe/Vienna`.
- Use only `Instant` epoch-seconds day bucketing.

---

## How to Integrate Snapshots into the Score Calculations

The scoring model should treat each snapshot layer as producing **one estimate** per metric at evaluation time:

### A) Liquidity Score integration (mostly “current” + optionally “macro guardrails”)

Liquidity is primarily a function of:
- spread
- fill-time (position-size aware)
- bottleneck legs

These are computed using the **latest snapshot** (current market state), because liquidity is an execution-time property.

Optionally, daily snapshots can add guardrails:
- If daily spread/turnover indicates chronic illiquidity (e.g., persistent near-zero movingWeek or extreme spread),
  you can apply a small penalty or flag as “structurally illiquid”.

**Rule of thumb:**
- Liquidity score = based on **latest** prices/spreads/turnover.
- Daily data = used for **flags** and confidence, not to “average away” today’s reality.

### B) Risk Score integration (multi-timescale by design)

Risk benefits from multiple horizons:

1. **Execution risk (spread + fill time)**  
   Computed from the **latest snapshot** (what you face when you execute).

2. **Micro price risk (1 minute)**  
   Computed from the **5s window** as `sigma_1m` and `r_1m`:
- If sigma_1m is high, short-term price movement is violent → higher risk.
- If the engine is intended for very fast flips, weight this more.

3. **Macro price risk (daily)**  
   Computed from daily snapshots:
- `sigma_1d(7)` or `sigma_1d(30)` for longer-term instability
- large `|r_1d|` as momentum/regime shift proxy

### Recommended risk aggregation

Compute separate risk components, then combine with fixed weights:

- `R_exec`   from latest snapshot
- `R_micro`  from 1m window (5s series)
- `R_macro`  from daily series (N days)

Example:
- `R_total = 0.45*R_exec + 0.35*R_micro + 0.20*R_macro`

Weights should be configurable per strategy (fast flipping vs longer holds).

---

## Preventing Manipulation / Bias from “Many Current Snapshots”

Storing lots of 5s snapshots must not “overweight the present” or allow the scoring to be manipulated by the *count* of snapshots.  
The key principle:

> **Snapshots are samples of a time series, not independent observations to be summed.**  
> Every score component must be derived into a single estimate per horizon window.

### 1. Never sum/average “scores per snapshot”

Bad pattern:
- compute liquidity/risk score for every 5s snapshot
- average them
  This creates two problems:
- **window-length dependence** (more snapshots = more weight)
- **double-counting** micro-noise

Good pattern:
- compute **one** sigma_1m from the window
- compute **one** r_1m from the window boundary
- compute **one** latest spread/fill-time from the latest snapshot
  Then feed these single numbers into the score.

### 2. Use time-based weighting (not count-based)

If you do any averaging over snapshots, weight by **time delta**, not number of points:
- for uniform 5s sampling it’s equivalent, but time-weighting remains correct if sampling jitter occurs.

### 3. Robust estimators to resist spikes

Micro windows can contain outliers. Use robust options:
- winsorize returns at percentile caps
- median absolute deviation (MAD) fallback when data is noisy
- clamp fill-time and spread to reasonable caps for normalization

### 4. Confidence metadata (prevents fake precision)

Expose confidence flags:
- `microConfidence = HIGH` only if enough 5s points exist (e.g. >= 10 points over last minute)
- `macroConfidence = HIGH` only if enough daily points exist (e.g. >= 7 days)
  When confidence is low, reduce the weight or fall back to proxy metrics.

### 5. Separate “feature computation” from “score aggregation”

Architecture rule:
- `SnapshotStore` produces **features** (spreadRel, fillTime, sigma_1m, sigma_1d, returns)
- `ScoreEngine` consumes features and applies weights
  This prevents accidental mixing of raw snapshots into scoring.

---

## Practical Data Structures

### Snapshot stores

- `Store5s`: ring buffer / deque, prune entries older than `windowMs` (e.g. 60s or 300s)
- `Store1d`: map keyed by `epochDay` storing the first 5s snapshot in each UTC day bucket

### Feature extraction API

At scoring time, produce:

**From latest snapshot (execution-time):**
- `spreadRel_latest(item)`
- `turnoverBuy_latest(item)`, `turnoverSell_latest(item)`
- fill-time estimates for desired quantities

**From 5s window (micro):**
- `sigma_1m(item)`
- `r_1m(item)`
- micro max drawdown proxy (optional)

**From daily series (macro):**
- `sigma_1d(item, N)`
- `r_1d(item)`
- trend z-score (optional)

These features are computed per item and then combined across legs using bottleneck (min/max) logic.

---

## How Daily Snapshots Improve the Flip Engine

Daily snapshots help answer questions that 5s data cannot:
- Is an item “structurally thin” even if it looks fine right now?
- Are we trading during a regime shift where yesterday-to-today moved violently?
- Is micro-volatility high because of a transient spike, or because the item is generally unstable?

They also allow:
- long-term monitoring dashboards
- anomaly detection (e.g., “today’s mid is 5σ away from 30d mean”)

---

## Summary of Integration Rules (Non-Negotiable)

1. **Liquidity**: computed from **latest** snapshot (execution reality).
2. **Risk**: combination of:
  - latest (execution risk),
  - 1m micro-volatility (from 5s series),
  - daily macro-volatility (from daily anchors).
3. **No snapshot-count bias**:
  - never average “scores per snapshot”
  - derive **one metric per horizon**, then score once.
4. **Always expose confidence** and degrade gracefully when history is insufficient.

---

## Suggested Configuration Defaults

- 5s retention window: 60–300 seconds
- daily keying: `epochDay = floor(epochSeconds/86400)` (UTC, `Instant`-only)
- micro volatility window: last 60 seconds (5s steps)
- macro volatility window: last 7 / 30 days
- risk weights: (exec=0.45, micro=0.35, macro=0.20) configurable
- caps:
  - spreadCap = 5%
  - timeCap = 6h (per-order/per-quote execution window used when scoring short-horizon execution quality).
  - fillTime max clamp = 24h (post-observation cap applied before storing/aggregating historical fill-time metrics).

---

## Intended Consumers

- Repo README / specification for contributors
- Input description for Convex/Codex to implement:
  - snapshot storage
  - feature extraction
  - score engine with multi-timescale risk control
