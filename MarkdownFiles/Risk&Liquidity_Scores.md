# Hypixel SkyBlock Flip Scoring Engine (Liquidity + Risk) — Codex Prompt

You are to implement a **Liquidity Score** and **Risk Score** system for Hypixel SkyBlock flips using **only public API data** (primarily Bazaar). The code should be clean, testable, and modular.

## 0) Target Use Case

A “flip” can be:
- **Simple Bazaar flip** (buy item A, sell item A)
- **Crafting/Conversion flip** (buy inputs, craft/convert, sell outputs)

Example crafting flip:
- `2.5 stacks IRON_INGOT (160) => 1 ENCHANTED_IRON`

Scores must be **position-size aware**: liquidity/risk depend on the quantities in the flip.

## 1) Data Source & Assumptions

### Bazaar endpoint
Use:
- `GET https://api.hypixel.net/skyblock/bazaar`

The response contains:
- `products.<PRODUCT_ID>.quick_status` with fields like:
  - `buyPrice`, `sellPrice`
  - `buyVolume`, `sellVolume`
  - `buyMovingWeek`, `sellMovingWeek`  (7-day traded volume)  [oai_citation:0‡Hypixel Forums](https://hypixel.net/threads/bazaar-api-what-is-buymovingweek-and-sellmovingweek.3795521/?utm_source=chatgpt.com)
  - `buyOrders`, `sellOrders`

Notes:
- `buyMovingWeek`/`sellMovingWeek` represent volume over the last 7 days; convert to per-hour by dividing by 168.  [oai_citation:1‡Hypixel Forums](https://hypixel.net/threads/bazaar-api-what-is-buymovingweek-and-sellmovingweek.3795521/?utm_source=chatgpt.com)
- Use the all-products endpoint `/skyblock/bazaar` (not deprecated per-product endpoints).  [oai_citation:2‡Hypixel Forums](https://hypixel.net/threads/bazaar-api.4989082/?utm_source=chatgpt.com)
- Price access path examples: `products.ITEM_ID.quick_status.sellPrice` etc.  [oai_citation:3‡Hypixel Forums](https://hypixel.net/threads/how-i-gonna-get-bazaar-item-sell-price-using-hypixels-api.4617428/?utm_source=chatgpt.com)

### Execution model
Each leg of a flip is executed in one of these modes:
- `INSTANT_BUY`  -> pay `sellPrice`
- `BUY_ORDER`    -> pay `buyPrice` (approx; optionally + tick)
- `INSTANT_SELL` -> receive `buyPrice`
- `SELL_OFFER`   -> receive `sellPrice` (approx; optionally - tick)

The scoring engine must accept an `ExecutionMode` per leg.

## 2) Core Definitions

For an item i:

### Mid price
mid_i = (buyPrice_i + sellPrice_i) / 2

### Relative spread (microstructure risk proxy)
spreadRel_i = (sellPrice_i - buyPrice_i) / mid_i

### Turnover (per hour) from moving week
turnoverBuy_i  = buyMovingWeek_i  / 168
turnoverSell_i = sellMovingWeek_i / 168

Interpretation:
- If you want to **buy instantly**, your fill is limited by sell-side turnover: use `turnoverSell`
- If you want to **sell instantly**, your fill is limited by buy-side turnover: use `turnoverBuy`

### Fill time approximation (hours)
Given desired quantity Q:
- fillTimeBuyHours  ≈ Q / turnoverSell_i   (for INSTANT_BUY)
- fillTimeSellHours ≈ Q / turnoverBuy_i    (for INSTANT_SELL)

For order-based execution (BUY_ORDER / SELL_OFFER), use the same turnover approximation but mark the result as **optimistic** (actual fill may be slower).

If turnover is 0, treat fill time as infinite (or a very large cap).

## 3) Flip Modeling

A flip is a set of legs:

- Inputs: list of `Leg(itemId, qty, side=BUY, executionMode)`
- Outputs: list of `Leg(itemId, qty, side=SELL, executionMode)`

For crafting flips, represent a recipe/conversion as:
- A mapping of input quantities -> output quantities.
- The scoring engine does NOT need to simulate crafting; assume instantaneous craft for now,
  but include a configurable `craftDelaySeconds` and a `craftOverheadRisk` component.

## 4) Liquidity Score (0..100)

Liquidity should reflect:
- low spread (better)
- low fill time for the desired position size (better)
- bottleneck behavior: the flip is only as liquid as its worst leg

### Per-leg liquidity component
Let t = fillTimeHours for that leg (computed based on side+executionMode)
Let s = spreadRel

Use smooth decreasing functions:
f_time(t)   = 1 / (1 + t / T)
f_spread(s) = 1 / (1 + s / S)

Recommended defaults:
- T = 1.0 hour  (time scale)
- S = 0.02      (2% spread scale)

Then:
L_leg = 100 * f_time(t) * f_spread(s)

### Flip liquidity
L_flip = min(L_leg over all legs)

Rationale: a crafting flip bottlenecks on the least-liquid leg (often the output).

## 5) Risk Score (0..100)

Risk aggregates:
1) **Execution risk** (spread + fill time)
2) **Volatility risk** (optional if you store history)
3) **Crafting/exposure risk** (time exposed in inventory + low liquidity)

### 5.1 Execution risk
Normalize spread and fill time:
g_spread(s) = clamp01(s / spreadCap) * 100
g_time(t)   = clamp01(t / timeCap)   * 100

Suggested caps:
- spreadCap = 0.05  (5% spread -> 100)
- timeCap   = 6.0   (6 hours fill -> 100)

For each leg:
R_exec_leg = wS*g_spread(s) + wT*g_time(t)
Defaults: wS=0.5, wT=0.5

Flip-level execution risk:
R_exec = max(R_exec_leg over all legs)   (worst leg dominates)

### 5.2 Volatility risk (optional; if you have local time series)
If you store mid prices by polling:
- r_t = ln(mid_t / mid_{t-1})
- sigma = stdev(r_t) over window (e.g., last 24h or 7d)

Normalize:
R_vol = clamp01(sigma / sigmaCap) * 100
Choose sigmaCap empirically (configurable).

If no history is available:
- Provide a proxy: R_vol_proxy = 0.5*g_spread + 0.5*g_time using worst leg
- Mark output field `volatilityConfidence = "LOW"`

### 5.3 Crafting/exposure risk
Exposure increases with time you must wait to fill inputs and outputs.
Approx:
exposureHours ≈ sum(fillTimeBuyInputs) + craftDelayHours + sum(fillTimeSellOutputs)

Normalize:
R_craft = clamp01(exposureHours / exposureCap) * 100
Default exposureCap = 6.0 hours.

### 5.4 Total risk
R_flip = 0.45*R_exec + 0.35*R_vol + 0.20*R_craft
If using proxy volatility, still compute with it, but include confidence metadata.

## 6) Profit Calculation (for reporting, not part of scoring)

Compute expected cost/revenue based on execution modes:

BUY leg:
- INSTANT_BUY  -> unitCost = sellPrice
- BUY_ORDER    -> unitCost = buyPrice  (optionally + tick)
SELL leg:
- INSTANT_SELL -> unitRev = buyPrice
- SELL_OFFER   -> unitRev = sellPrice (optionally - tick)

profit = sum(outputs.qty * unitRev) - sum(inputs.qty * unitCost)

Support optional fees/taxes as configurable multipliers.

## 7) Implementation Requirements

Implement in (choose one):
- TypeScript (Node 18+) OR Python 3.11+

Must include:
- Data models: `BazaarProduct`, `QuickStatus`, `Leg`, `Flip`, `ScoreConfig`, `ScoreResult`
- Fetcher: `fetchBazaar(): Map<itemId, QuickStatus>`
- Scoring: `scoreFlip(flip, bazaarData, config) -> ScoreResult`

### Output fields
Return:
- `liquidityScore` (0..100)
- `riskScore` (0..100)
- `expectedProfit`
- `perLeg`: array with itemId, qty, side, executionMode, spreadRel, fillTimeHours, L_leg, R_exec_leg
- `bottleneckLeg` (itemId)
- `volatilityConfidence`: "HIGH" if real history used else "LOW"
- `notes`: warnings like turnover=0, missing item, etc.

### Edge cases
- Missing itemId in Bazaar data => mark flip unscorable or set severe penalties + note
- turnoverBuy or turnoverSell = 0 => fillTime = INF (cap to maxTimeForScoring, default 24h)
- mid=0 => guard division
- Quantities can be non-integers? (usually ints, but accept float and treat as units)

## 8) Example: Iron -> Enchanted Iron

Flip definition example:
Inputs:
- IRON_INGOT qty=160 side=BUY executionMode=BUY_ORDER
Outputs:
- ENCHANTED_IRON qty=1 side=SELL executionMode=SELL_OFFER
craftDelaySeconds=10

Run scoring with default config and print JSON.

## 9) Testing

Include unit tests:
- Spread computation
- Fill time computation with known turnover
- Liquidity min(bottleneck)
- Risk max(worst leg) for execution
- Handling turnover=0 and missing item

## 10) Deliverables

Produce:
- Source code
- A runnable CLI:
  - accepts a flip JSON file
  - fetches bazaar live
  - prints ScoreResult as JSON

Be careful with naming:
- `buyPrice` is the price you receive for instant selling (i.e., top buy order price).
- `sellPrice` is the price you pay for instant buying (i.e., top sell offer price).
