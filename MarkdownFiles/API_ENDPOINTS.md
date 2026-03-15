# SkyblockFlipper Backend API Reference

Version: `v1`  
Base URL (local): `http://localhost:1880`

This document is formatted as a public API reference (similar style to Hypixel docs): each endpoint includes purpose, parameters, and concrete response shape.

## Quick Notes

- Time format: ISO-8601 UTC (`2026-02-19T20:00:00Z`)
- Snapshot path IDs use epoch milliseconds (`snapshotEpochMillis`)
- Pagination follows Spring `Page<T>` response conventions:
- Query: `page`, `size`
- `MAX_PAGE_SIZE = 1000` in `StandardPagination`
- Requests with `size > 1000` are clamped to `1000` (no error)
- Defaults are endpoint-specific and keep previous default window sizes
- Response: `content`, `number`, `size`, `totalElements`, `totalPages`, `first`, `last`, `empty`
- `GET /api/status` is a local health probe and does not call Hypixel or other upstream APIs
- Flip reads default to unified current storage (`flip_current`) with legacy snapshot reads kept for parity/backfill/rollback paths

---

## Health

### `GET /api/status`

Checks API status.

Behavior:
- Returns local application liveness only.
- Safe for external uptime monitors such as Uptime Kuma.

Response:

```json
{
  "status": "ok"
}
```

---

## Market Endpoints

### `GET /api/v1/market/overview`

Compact market overview for dashboard-style UI ("übersicht") with buy/sell/spread, 7d range, volume and flip summary.

Query params:
- `productId` (optional, e.g. `HYPERION`)

Behavior:
- Uses latest available market snapshot.
- If `productId` is set, metrics are calculated for that Bazaar product.
- If `productId` is omitted, it returns a representative overview from the latest snapshot.
- Uses lightweight `bz_item_snapshot` history for the 7-day series and current flip summary data for flip counts/profit.

Response:

```json
{
  "productId": "HYPERION",
  "snapshotTimestamp": "2026-02-20T12:00:00Z",
  "buy": 789400000,
  "buyChangePercent": -0.7,
  "sell": 770700000,
  "sellChangePercent": -1.6,
  "spread": 18700000,
  "spreadPercent": 2.4,
  "sevenDayHigh": 857500000,
  "sevenDayLow": 770700000,
  "volume": 416,
  "averageVolume": 292,
  "activeFlips": 2,
  "bestProfit": 22500000
}
```

---

## Dashboard Endpoints

### `GET /api/v1/dashboard/overview`

High-level landing-page summary for the latest snapshot.

Behavior:
- Uses latest snapshot summary metadata instead of hydrating full raw snapshot payloads.
- Uses latest `bz_item_snapshot` rows for market trend.
- Uses unified current flip storage for top-flip and active-flip counts when available.

Response:

```json
{
  "totalItems": 3749,
  "totalActiveFlips": 512,
  "totalAHListings": 13554560,
  "bazaarProducts": 3749,
  "topFlip": {
    "id": "9cf965a6-4a64-4f8d-95b3-2ba113c2e2d8",
    "outputName": "ENCHANTED_DIAMOND",
    "expectedProfit": 142000
  },
  "marketTrend": "SIDEWAYS",
  "lastUpdated": "2026-02-20T12:00:00Z"
}
```

---

### `GET /api/v1/dashboard/trending`

Lists the strongest 24h movers from the latest available market snapshots.

Query params:
- `limit` (optional, default `10`)

Response:
- `TrendingItemDto[]`

---

## Flip Endpoints

### `GET /api/v1/flips`

List flips (optionally filtered by `flipType` and snapshot).

Query params:
- `flipType` (optional): `AUCTION`, `BAZAAR`, `CRAFTING`, `FORGE`, `KATGRADE`, `FUSION`
- `snapshotTimestamp` (optional, ISO-8601)
- `page` (optional, default `0`)
- `size` (optional, default `50`)

Response:
- `Page<UnifiedFlipDto>`

---

### `GET /api/v1/flips/{id}`

Get a single flip by UUID.

Path params:
- `id` (required UUID)

Response:
- `200` with `UnifiedFlipDto`
- `404` if not found

---

### `GET /api/v1/flips/types`

List all supported `FlipType` enum values.

Response:

```json
{
  "flipTypes": ["AUCTION", "BAZAAR", "CRAFTING", "FORGE", "KATGRADE", "FUSION"]
}
```

---

### `GET /api/v1/flips/stats`

Get type counts for one snapshot.

Query params:
- `flipType` (optional enum)
- `snapshotTimestamp` (optional, ISO-8601)
- `legacySnapshot` (optional boolean, default `false`)

Behavior:
- If `legacySnapshot=true` or `snapshotTimestamp` is present without `flipType`, returns snapshot-style legacy-compatible stats.
- Otherwise returns current summary stats, optionally filtered by `flipType`.
- If no snapshots exist: returns `snapshotTimestamp = null`, `totalFlips = 0`

Response:

```json
{
  "snapshotTimestamp": "2026-02-19T20:00:00Z",
  "totalFlips": 123,
  "byType": [
    { "flipType": "AUCTION", "count": 40 },
    { "flipType": "BAZAAR", "count": 50 }
  ]
}
```

---

### `GET /api/v1/flips/coverage`

Coverage matrix for currently mapped flip families.

Response:

```json
{
  "snapshotTimestamp": "2026-02-19T20:00:00Z",
  "excludedFlipTypes": ["SHARD", "FUSION"],
  "flipTypes": [
    {
      "flipType": "AUCTION",
      "ingestion": "SUPPORTED",
      "calculation": "SUPPORTED",
      "persistence": "SUPPORTED",
      "api": "SUPPORTED",
      "latestSnapshotCount": 7,
      "notes": "Generated from Hypixel market snapshots via MarketFlipMapper."
    }
  ]
}
```

---

### `GET /api/v1/flips/filter`

Advanced flip search with score/profit/risk/capital filtering.

Query params:
- `flipType` (optional enum)
- `snapshotTimestamp` (optional ISO-8601)
- `minLiquidityScore` (optional `double`)
- `maxRiskScore` (optional `double`)
- `minExpectedProfit` (optional `long`)
- `minRoi` (optional `double`)
- `minRoiPerHour` (optional `double`)
- `maxRequiredCapital` (optional `long`)
- `partial` (optional `boolean`)
- `sortBy` (optional, default `EXPECTED_PROFIT`):
- `EXPECTED_PROFIT`, `ROI`, `ROI_PER_HOUR`, `LIQUIDITY_SCORE`, `RISK_SCORE`, `REQUIRED_CAPITAL`, `FEES`, `DURATION_SECONDS`
- `sortDirection` (optional, default `DESC`): `ASC` or `DESC`
- `page` (optional, default `0`)
- `size` (optional, default `50`)

Response:
- `Page<UnifiedFlipDto>`

Example:

`GET /api/v1/flips/filter?flipType=BAZAAR&minLiquidityScore=85&maxRiskScore=20&sortBy=LIQUIDITY_SCORE&sortDirection=DESC&page=0&size=25`

---

### `GET /api/v1/flips/top`

Convenience endpoint returning the best `N` flips as a simple list.

Query params:
- `flipType` (optional enum)
- `snapshotTimestamp` (optional ISO-8601)
- `minLiquidityScore` (optional `double`)
- `maxRiskScore` (optional `double`)
- `minExpectedProfit` (optional `long`)
- `minRoi` (optional `double`)
- `minRoiPerHour` (optional `double`)
- `maxRequiredCapital` (optional `long`)
- `partial` (optional `boolean`)
- `limit` (optional int, default `6`)

Response:
- `UnifiedFlipDto[]`

---

### `GET /api/v1/flips/top/liquidity`

Convenience endpoint: best liquidity first.

Query params:
- `flipType` (optional enum)
- `snapshotTimestamp` (optional ISO-8601)
- `page` (optional, default `0`)
- `size` (optional, default `50`)

Behavior:
- Internally sorts by `LIQUIDITY_SCORE DESC`

Response:
- `Page<UnifiedFlipDto>`

---

### `GET /api/v1/flips/top/low-risk`

Convenience endpoint: lowest risk first.

Query params:
- `flipType` (optional enum)
- `snapshotTimestamp` (optional ISO-8601)
- `page` (optional, default `0`)
- `size` (optional, default `50`)

Behavior:
- Internally sorts by `RISK_SCORE ASC`

Response:
- `Page<UnifiedFlipDto>`

---

### `GET /api/v1/flips/top/best`

Convenience endpoint: flips ranked by combined "goodness" score.

Query params:
- `flipType` (optional enum)
- `snapshotTimestamp` (optional ISO-8601)
- `getbypage` (optional int): paged state after top-6

Behavior:
- This endpoint intentionally does not use standard `page`/`size`.
- If no `getbypage` is provided, endpoint returns only the first `6` ranked flips (`0..5`).
- `Page<FlipGoodnessDto>` metadata for this "featured 6" mode is:
- `number=0`, `size=6`
- `totalElements` is the full ranked dataset size (not capped to 6)
- `totalPages` is computed from that full total and page size 6
- Sorted by computed `goodnessScore DESC`
- Score combines profitability, ROI/h, liquidity, and inverse risk
- `getbypage` uses page size `20` starting at index `6`:
- `getbypage=0` -> `6..25`
- `getbypage=1` -> `26..45`

Response:
- `Page<FlipGoodnessDto>`

`FlipGoodnessDto`:
- `flip` (`UnifiedFlipDto`)
- `goodnessScore` (`double`, `0..100`)
- `breakdown`:
- `roiPerHourScore`
- `profitScore`
- `liquidityScore`
- `inverseRiskScore`
- `partialPenaltyApplied`

---

## Snapshot Endpoints

### `GET /api/v1/snapshots`

List market snapshots.

Query params:
- `page` (optional, default `0`)
- `size` (optional, default `100`)

Response:
- `Page<MarketSnapshotDto>`

`MarketSnapshotDto`:
- `id` (UUID)
- `snapshotTimestamp`
- `auctionCount`
- `bazaarProductCount`
- `createdAt`

---

### `GET /api/v1/snapshots/{snapshotEpochMillis}/flips`

List flips tied to one snapshot timestamp.

Path params:
- `snapshotEpochMillis` (required long)

Query params:
- `flipType` (optional enum)
- `page` (optional, default `0`)
- `size` (optional, default `50`)

Response:
- `Page<UnifiedFlipDto>`

---

## Item Endpoints

### `GET /api/v1/items`

List items.

Query params:
- `itemId` (optional string filter)
- `search` (optional string)
- `category` (optional string)
- `rarity` (optional string)
- `marketplace` (optional enum)
- `page` (optional, default `0`)
- `size` (optional, default `12`)

Response:
- `Page<ItemDto>`

`ItemDto`:
- `id`
- `displayName`
- `minecraftId`
- `rarity`
- `category`
- `infoLinks[]`

---

### `GET /api/v1/items/{itemId}`

Get one item by ID.

Path params:
- `itemId` (required string)

Response:
- `200` with `ItemDto`
- `404` if not found

---

### `GET /api/v1/items/{itemId}/price-history`

Get price history points for one item.

Path params:
- `itemId` (required string)

Query params:
- `range` (optional string, mapped to `PriceHistoryRange`)

Response:
- `PricePointDto[]`

---

### `GET /api/v1/items/{itemId}/score-history`

Get score-history points for one item.

Path params:
- `itemId` (required string)

Response:
- `ScorePointDto[]`

---

### `GET /api/v1/items/{itemId}/quick-stats`

Get quick aggregate stats for one item.

Path params:
- `itemId` (required string)

Response:
- `200` with `ItemQuickStatsDto`
- `404` if not found

---

### `GET /api/v1/items/{itemId}/flips`

List flips related to one item.

Path params:
- `itemId` (required string)

Query params:
- `page` (optional, default `0`)
- `size` (optional, default `20`)

Response:
- `Page<UnifiedFlipDto>`

---

### `GET /api/v1/items/npc-buyable`

List NPC buy offers.

Query params:
- `itemId` (optional string filter)
- `page` (optional, default `0`)
- `size` (optional, default `100`)

Response:
- `Page<NpcShopOfferDto>`

`NpcShopOfferDto`:
- `npcId`
- `npcDisplayName`
- `itemId`
- `itemAmount`
- `costs[]` (`itemId`, `amount`)
- `coinCost`
- `unitCoinCost`

---

## Bazaar Endpoints

### `GET /api/v1/bazaar/{itemId}`

Get the latest Bazaar product view for one item.

Path params:
- `itemId` (required string)

Response:
- `200` with `BazaarProductDto`
- `404` if not found

---

### `GET /api/v1/bazaar/{itemId}/orders`

Get an order-book style response for one Bazaar item.

Path params:
- `itemId` (required string)

Query params:
- `depth` (optional int, default `15`)

Response:
- `BazaarOrderBookDto`

---

### `GET /api/v1/bazaar/quick-flips`

Get quick-spread Bazaar opportunities.

Query params:
- `minSpreadPct` (optional double)
- `limit` (optional int, default `20`)

Response:
- `BazaarQuickFlipDto[]`

---

## Auction House Endpoints

### `GET /api/v1/ah/listings/{itemId}`

List Auction House listings for one item.

Path params:
- `itemId` (required string)

Query params:
- `sortBy` (optional enum)
- `sortDirection` (optional `ASC` or `DESC`)
- `bin` (optional boolean)
- `minStars` (optional int)
- `maxStars` (optional int)
- `reforge` (optional string)
- `page` (optional, default `0`)
- `size` (optional, default `20`)

Response:
- `Page<AhListingDto>`

---

### `GET /api/v1/ah/listings/{itemId}/breakdown`

Get Auction House pricing breakdown for one item.

Path params:
- `itemId` (required string)

Response:
- `AhListingBreakdownDto`

---

### `GET /api/v1/ah/recent-sales/{itemId}`

Get recent sales for one item.

Path params:
- `itemId` (required string)

Query params:
- `limit` (optional int, default `10`)

Response:
- `AhRecentSaleDto[]`

---

## Recipe Endpoints

### `GET /api/v1/recipes`

List recipes.

Query params:
- `outputItemId` (optional string)
- `processType` (optional): `CRAFT`, `FORGE`, `KATGRADE`
- `page` (optional, default `0`)
- `size` (optional, default `100`)

Response:
- `Page<RecipeDto>`

`RecipeDto`:
- `recipeId`
- `outputItemId`
- `processType`
- `processDurationSeconds`
- `ingredients[]` (`itemId`, `amount`)

---

### `GET /api/v1/recipes/{recipeId}/cost`

Get cost breakdown for one recipe.

Path params:
- `recipeId` (required string)

Response:
- `200` with `RecipeCostBreakdownDto`
- `404` if not found

---

## UnifiedFlipDto Schema

Primary fields returned by flip endpoints:

- `id` (UUID)
- `flipType`
- `inputItems[]`
- `outputItems[]`
- `requiredCapital`
- `expectedProfit`
- `roi`
- `roiPerHour`
- `durationSeconds`
- `fees`
- `liquidityScore`
- `riskScore`
- `snapshotTimestamp`
- `partial` / `partialReasons[]`
- `steps[]`
- `constraints[]`

---

## Internal Admin Instrumentation Endpoints

Base path: `/internal/admin/instrumentation`

Security:
- May be local-only (otherwise `403`)
- If configured, requires `X-Admin-Token` header (otherwise `401`)

### `POST /internal/admin/instrumentation/jfr/snapshot`

Creates a JFR snapshot dump.

Response:

```json
{
  "snapshot": "C:\\path\\recording.jfr",
  "createdAt": "2026-02-19T22:40:00Z"
}
```

### `GET /internal/admin/instrumentation/jfr/report/latest`

Returns latest JFR summary object.

### `POST /internal/admin/instrumentation/async-profiler/run`

Runs async-profiler integration.

Possible status codes:
- `404` when async-profiler is disabled
- `500` on profiler execution errors

### `GET /internal/admin/instrumentation/flip-storage/config`

Returns current unified flip storage cutover flags and latest snapshot markers for legacy/new storage.

### `GET /internal/admin/instrumentation/flip-storage/parity/latest`

Runs an on-demand parity report comparing legacy snapshot-flip reads vs new `flip_current` reads.

Report includes:
- flag snapshot (`dualWriteEnabled`, `readFromNew`, `legacyWriteEnabled`)
- comparison snapshot epoch millis
- counts (`legacy`, `current`, intersection, only-legacy, only-current)
- per-type counts
- sampled ID diffs and sampled metric mismatches
- `parityOk` boolean

### `POST /internal/admin/instrumentation/flip-storage/backfill/latest`

Backfills the latest legacy `flip` snapshot into the new unified storage tables (`flip_definition`, `flip_current`, `flip_trend_segment`).

### `POST /internal/admin/instrumentation/flip-storage/backfill/{snapshotEpochMillis}`

Backfills one specific legacy snapshot into unified storage.
