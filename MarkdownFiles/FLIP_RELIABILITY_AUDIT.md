# Flip Reliability Audit

Date: 2026-04-11

## Goal

The target is not just to show flips that look profitable in the current snapshot.

The real goal is:

- show flips that are reliable
- keep profit meaningful relative to the required entry capital
- estimate whether that edge is likely to weaken soon after the user enters

A simple way to say it:

The problem is not that the API needs a few extra calculations. The real problem is that it currently judges flips mostly by what looks good right now, while the actual goal is to judge flips by what will probably still be good a little later and how reliable that profit is.

## Main Findings

### 1. Reported duration and ROI/h are not aligned with real market execution time

The API computes Bazaar and Auction fill-time signals, but those times are not used for the final `durationSeconds` and `roiPerHour` that the client sees.

Relevant code:

- `src/main/java/com/skyblockflipper/backend/service/flipping/UnifiedFlipDtoMapper.java:93-105`
- `src/main/java/com/skyblockflipper/backend/service/flipping/UnifiedFlipDtoMapper.java:257-270`
- `src/main/java/com/skyblockflipper/backend/service/flipping/UnifiedFlipDtoMapper.java:293-341`
- `src/main/java/com/skyblockflipper/backend/service/flipping/UnifiedFlipDtoMapper.java:708-725`
- `src/main/java/com/skyblockflipper/backend/model/Flipping/Flip.java:86-114`

Impact:

- ROI/h can be heavily overstated
- short static steps can make slow flips look fast
- capital efficiency is misrepresented

### 2. Stored/current auction profit can be wrong under live election modifiers

Live election effects are only included in `loadCurrentContext()`. The main snapshot-based read and persistence paths use `loadContextAsOf()`, which falls back to standard auction tax assumptions.

Relevant code:

- `src/main/java/com/skyblockflipper/backend/service/flipping/FlipCalculationContextService.java:44-52`
- `src/main/java/com/skyblockflipper/backend/service/flipping/FlipCalculationContextService.java:65-96`
- `src/main/java/com/skyblockflipper/backend/service/flipping/storage/UnifiedFlipStorageService.java:93-102`
- `src/main/java/com/skyblockflipper/backend/service/flipping/storage/OnDemandFlipSnapshotService.java:62-63`

Impact:

- persisted Auction flip profit can be systematically off
- tax-sensitive periods such as Derpy are not modeled consistently

### 3. The default "best" ranking is not reliability-first and not capital-first

The default scoring still gives significant weight to raw profit and does not directly optimize for required capital. Recommendation gates are also disabled by default.

Relevant code:

- `src/main/resources/application.yml:31-34`
- `src/main/java/com/skyblockflipper/backend/service/flipping/FlipReadService.java:1368-1391`
- `src/main/java/com/skyblockflipper/backend/service/flipping/FlipReadService.java:1435-1460`
- `src/main/java/com/skyblockflipper/backend/api/controller/FlipController.java:79-114`
- `src/main/java/com/skyblockflipper/backend/api/controller/FlipController.java:117-143`
- `src/main/java/com/skyblockflipper/backend/service/market/DashboardReadService.java:171-176`
- `src/main/java/com/skyblockflipper/backend/service/market/MarketOverviewService.java:70-88`

Impact:

- "best" is still close to "best-looking now"
- entry-capital efficiency is under-modeled
- reliable but smaller flips can lose against noisy high-profit flips

### 4. The system is descriptive, not predictive

There is some statistical usage already, but it is mostly used to penalize risk, not to forecast whether a flip will remain useful.

What exists:

- recent return and volatility features
- structural illiquidity flags
- trend segments stored over time

What does not exist:

- probability the flip is still profitable at `t + delta`
- expected profit shrinkage over the next horizon
- edge half-life
- confidence interval for future usefulness
- a ranking term for "likely to decay soon"

Relevant code:

- `src/main/java/com/skyblockflipper/backend/service/market/MarketTimescaleFeatureService.java:154-176`
- `src/main/java/com/skyblockflipper/backend/service/market/MarketTimescaleFeatureService.java:202-320`
- `src/main/java/com/skyblockflipper/backend/service/flipping/FlipRiskScorer.java:23-52`
- `src/main/java/com/skyblockflipper/backend/service/flipping/FlipRiskScorer.java:114-129`
- `src/main/java/com/skyblockflipper/backend/config/properties/FlippingModelProperties.java:9-12`
- `src/main/java/com/skyblockflipper/backend/service/flipping/storage/UnifiedFlipStorageService.java:124-216`
- `src/main/java/com/skyblockflipper/backend/model/flippingstorage/FlipTrendSegmentEntity.java:22-72`

Impact:

- the API can say "this is volatile"
- the API cannot yet say "this edge will probably disappear soon"

### 5. Auction reliability is weak because the model does not use real sell-through data

Auction quotes are based on active BIN listings, sample count, and spread. That is not the same as modeling how quickly similar items actually sell.

Relevant code:

- `src/main/java/com/skyblockflipper/backend/service/flipping/UnifiedFlipInputMapper.java:83-116`
- `src/main/java/com/skyblockflipper/backend/service/flipping/UnifiedFlipInputMapper.java:291-361`
- `src/main/java/com/skyblockflipper/backend/service/flipping/UnifiedFlipDtoMapper.java:600-613`

There is also a deeper issue:

- ingestion drops claimed auctions in `src/main/java/com/skyblockflipper/backend/service/market/MarketDataProcessingService.java:507-510`
- but recent sales expect claimed auctions in `src/main/java/com/skyblockflipper/backend/service/market/AuctionHouseReadService.java:127-148`

Impact:

- Auction flips are not statistically grounded enough for reliability ranking
- current AH liquidity is closer to listing density than actual fill probability

### 6. Market routing is heuristic, not venue-optimized

If both Bazaar and Auction data exist, the mapper often defaults to Bazaar instead of selecting the venue with the best expected risk-adjusted outcome.

Relevant code:

- `src/main/java/com/skyblockflipper/backend/service/flipping/UnifiedFlipDtoMapper.java:506-509`
- `src/main/java/com/skyblockflipper/backend/service/flipping/UnifiedFlipDtoMapper.java:552-556`

Impact:

- the API does not truly answer "best market flip"
- it answers "best flip under a mostly Bazaar-first assumption"

### 7. Some recipe economics are structurally incomplete

Recipes store only one output item and do not model output quantity or byproducts. Implicit sells also default to amount `1`.

Relevant code:

- `src/main/java/com/skyblockflipper/backend/model/Flipping/Recipe/Recipe.java:23-48`
- `src/main/java/com/skyblockflipper/backend/model/Flipping/Recipe/RecipeToFlipMapper.java:25-30`
- `src/main/java/com/skyblockflipper/backend/service/flipping/UnifiedFlipDtoMapper.java:315-345`
- `src/main/java/com/skyblockflipper/backend/service/flipping/UnifiedFlipDtoMapper.java:941-943`

Impact:

- some Crafting and Forge calculations cannot be fully accurate
- multi-output and retention mechanics are underrepresented

### 8. Flip-type coverage is still below the stated target

The documented and coded target includes more flip families than the current effective serving path supports.

Relevant code/docs:

- `src/main/java/com/skyblockflipper/backend/model/Flipping/Enums/FlipType.java:3-9`
- `src/main/java/com/skyblockflipper/backend/service/flipping/FlipReadService.java:50-55`
- `MarkdownFiles/API_ENDPOINTS.md:205-210`

Impact:

- `SHARD` is not in the enum
- `FUSION` is excluded from current coverage
- the product promise is ahead of the current implementation

## Pipeline Fit

### Polling itself is mostly fit

The adaptive polling design is reasonable for a live API:

- conditional requests
- burst and backoff behavior
- rate-limit handling
- one in-flight request per endpoint
- bounded processing queues

Relevant code:

- `src/main/java/com/skyblockflipper/backend/service/market/polling/AdaptivePoller.java:17-320`
- `src/main/java/com/skyblockflipper/backend/service/market/polling/ChangeDetector.java:17-48`
- `src/main/java/com/skyblockflipper/backend/service/market/polling/ProcessingPipeline.java:17-106`
- `src/main/resources/application.yml:54-113`

Conclusion:

- for staying current without overloading Hypixel, this is a solid direction

### The filtering and snapshot construction are not fit enough yet

The pipeline is currently optimized more for "keep the latest state fresh" than for "preserve the best statistical truth for reliability modeling".

That is the central pipeline issue.

## Additional Rework Areas

### 9. Persisted market snapshots are mixed-state snapshots, not truly atomic market views

When one endpoint updates, the system combines that fresh payload with the cached payload from the other endpoint. The final snapshot timestamp is then the maximum of the two upstream timestamps.

Relevant code:

- `src/main/java/com/skyblockflipper/backend/service/market/MarketDataProcessingService.java:272-315`
- `src/main/java/com/skyblockflipper/backend/service/market/MarketDataProcessingService.java:318-359`
- `src/main/java/com/skyblockflipper/backend/hypixel/HypixelMarketSnapshotMapper.java:29-33`

Impact:

- a stored snapshot can contain fresh Auction data and older Bazaar data, or vice versa
- this is acceptable for a live current-state UI
- this is weaker as statistical ground truth for forecasting and backtesting

### 10. The live Auction path loses metadata before historical replay and flip generation

The adaptive path persists a reduced market snapshot and later regenerates flips from that stored snapshot. But the raw auction mapper drops `itemLore` and `extra`, even though Auction grouping logic depends on those fields.

Relevant code:

- `src/main/java/com/skyblockflipper/backend/service/market/polling/AdaptivePollingCoordinator.java:449-465`
- `src/main/java/com/skyblockflipper/backend/service/flipping/FlipGenerationService.java:104-142`
- `src/main/java/com/skyblockflipper/backend/service/flipping/FlipGenerationService.java:207-212`
- `src/main/java/com/skyblockflipper/backend/hypixel/HypixelMarketSnapshotMapper.java:46-63`
- `src/main/java/com/skyblockflipper/backend/service/flipping/UnifiedFlipInputMapper.java:175-198`

Impact:

- live Auction matching quality can degrade after persistence
- historical replay and on-demand recomputation are weaker than the original live payload
- this especially hurts rare and metadata-sensitive Auction items

### 11. Bazaar depth information is available upstream but discarded

The upstream Bazaar model includes `buy_summary` and `sell_summary`, but the persisted and unified market snapshot keeps only `quick_status`.

Relevant code:

- `src/main/java/com/skyblockflipper/backend/hypixel/model/BazaarProduct.java:19-26`
- `src/main/java/com/skyblockflipper/backend/hypixel/HypixelMarketSnapshotMapper.java:66-94`
- `src/main/java/com/skyblockflipper/backend/model/market/BazaarMarketRecord.java:3-12`
- `src/main/java/com/skyblockflipper/backend/model/market/BzItemSnapshotEntity.java:31-41`

Impact:

- depth-aware slippage modeling is limited
- short-horizon spread decay estimation is limited
- the API has less information than it could have for practical predictive scoring

### 12. The adaptive processing path prefers freshness over high-resolution history

The processing queue can coalesce pending updates, and flip generation also keeps only the latest snapshot to generate if multiple updates arrive close together.

Relevant code:

- `src/main/java/com/skyblockflipper/backend/service/market/polling/ProcessingPipeline.java:41-57`
- `src/main/java/com/skyblockflipper/backend/service/market/polling/AdaptivePollingCoordinator.java:518-535`
- `src/main/resources/application.yml:107-109`

Impact:

- this is good for current-serving performance
- this is not ideal for dense historical training or detailed backtesting
- some short-lived market states may never become flip-generation snapshots

### 13. The historical storage model is compressed, but not yet designed around reliability analytics

The aggregate history tables keep only a reduced feature set. They are useful for lightweight trend views, but not rich enough yet for stronger predictive modeling.

Relevant code:

- `src/main/java/com/skyblockflipper/backend/model/market/AhItemSnapshotEntity.java:31-50`
- `src/main/java/com/skyblockflipper/backend/model/market/BzItemSnapshotEntity.java:31-41`
- `src/main/java/com/skyblockflipper/backend/service/market/SnapshotRetentionProperties.java:12-18`
- `src/main/resources/application.yml:125-145`

Impact:

- AH history does not store sale outcomes or fill-time outcomes
- BZ history stores only top-level price and volume fields
- the retained history is useful for simple trend features, but not yet for a strong reliability model

## What This Means

Right now the backend is better at answering:

"What looks profitable in the current snapshot?"

than answering:

"What is a reliable flip with good profit for the required capital, and is that edge likely to survive long enough to matter?"

That distinction is the main product gap.

## Statistical Direction That Matches The Real Goal

If the product should move in the direction of stock-style usefulness estimation, the next statistical outputs should be things like:

- expected net profit at `t + delta`
- probability the flip still clears a minimum profit threshold at `t + delta`
- expected buy fill time and sell fill time
- expected spread decay or edge decay
- edge half-life
- forecast confidence
- capital-adjusted expected value after liquidity and risk haircuts

This should stay practical. It does not need extreme forecasting. But it does need to move from pure snapshot scoring toward short-horizon expected usefulness.

## Rework Roadmap

### Must rework now

- use real execution time in final `durationSeconds` and `roiPerHour`
- preserve the data needed for Auction reliability modeling instead of dropping claimed sales at ingestion
- stop losing Auction metadata such as `itemLore` and `extra` before replay and flip regeneration
- preserve Bazaar depth information, not just `quick_status`
- move the ranking contract toward reliability and capital efficiency instead of current raw-profit bias
- make snapshot semantics explicit: either keep snapshots atomic enough for analytics, or store per-source freshness so mixed-state snapshots are not treated as full ground truth

### Should rework soon

- build actual sell-time models for Auction and Bazaar exits
- add a short-horizon usefulness model such as expected profit at `t + delta` or probability the flip stays above a profit floor
- route across Bazaar and Auction by expected risk-adjusted outcome instead of default heuristics
- extend recipe modeling to support output quantity, byproducts, and retention mechanics
- separate "current-serving" pipeline behavior from "analytics/training" history behavior if coalescing remains enabled

### Nice differentiators later

- confidence intervals or forecast bands for flip usefulness
- edge half-life or expected decay score
- backtesting over historical snapshots with reproducible model versions
- top historical snapshot materialization for stable leaderboard replay
- multi-step and chain-aware opportunity modeling

## Bottom Line

The current API is already moving beyond a simple snapshot list because it uses liquidity and risk signals. But it still does not fully fulfill the intended goal.

The biggest gap is this:

it ranks flips mostly by current-state economics, while the real goal is to rank flips by reliable near-future usefulness relative to required capital.
