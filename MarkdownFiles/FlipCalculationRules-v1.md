# Flip Calculation Rules v1

Status: accepted and implementation target

## 1. Scope

This document defines how the backend computes the unified flip metrics in `UnifiedFlipDto`.

## 2. Inputs

- Flip definition (`Flip`, ordered `Step` list, `Constraint`s)
- Latest market snapshot (`UnifiedFlipInputSnapshot`)
- Live election resource (`/v2/resources/skyblock/election`) for mayor tax modifiers

## 3. Price Resolution

For each priced item:

- Buy-side unit price:
  - prefer Bazaar `buyPrice`
  - fallback Auction `lowestStartingBid`
- Sell-side unit price:
  - prefer Bazaar `sellPrice`
  - fallback Auction `averageObservedPrice`

If neither source exists, computation is partial.

## 4. Required Capital

`requiredCapital` is computed as:

`max(minCapitalConstraint, currentPriceBaseline, computedPeakExposure)`

Where:

- `minCapitalConstraint`: max `MIN_CAPITAL` constraint value or `0`
- `currentPriceBaseline`: total snapshot input cost from all BUY steps
- `computedPeakExposure`: max running exposure over ordered steps

## 5. Revenue, Costs, Profit

- `totalInputCost`: sum BUY step costs
- `grossRevenue`: sum SELL step gross values
- `fees`: sum marketplace fees/taxes on SELL steps
- `expectedProfit = grossRevenue - totalInputCost - fees`

If a flip has no explicit SELL step, one implicit sell is evaluated for `resultItemId` with amount `1`.

## 6. Fee Rules

### 6.1 Bazaar

- Default Bazaar sell tax: `1.25%`
- Net Bazaar proceeds:
  - `net = gross - ceil(gross * 0.0125)`

### 6.2 Auction House (BIN)

AH fee components (preset durations only):

- Listing fee:
  - `< 10,000,000`: `1%`
  - `10,000,000` to `< 100,000,000`: `2%`
  - `>= 100,000,000`: `2.5%`
- Duration add-on:
  - `1h = 20`
  - `6h = 45`
  - `12h = 100`
  - `24h = 350`
  - `48h = 1200`
- Claim tax:
  - `1%` only if sale price is above `1,000,000`
  - capped so payout cannot be reduced below `1,000,000`

Mayor modifiers:

- If active mayor perks include Derpy `QUAD TAXES!!!`, AH tax components are multiplied by `4`.

Notes:

- For custom durations, v1 uses nearest preset fallback by exact matching only.
- If duration is missing, v1 default is `12h`.

## 7. ROI Metrics

- `roi = expectedProfit / requiredCapital` (fraction, not percent)
- `roiPerHour = roi * (3600 / durationSeconds)`

Guard rails:

- if `requiredCapital <= 0`, `roi = null`
- if `durationSeconds <= 0`, `roiPerHour = null`

## 8. Partial Results

When required market/election data is missing, DTO still returns best-effort computed fields and includes:

- `partial = true`
- `partialReasons = [...]`

Typical reasons:

- missing input price
- missing output price
- missing market snapshot
- election endpoint unavailable

## 9. Rounding Policy

Rounding in coin space:

- Costs: `ceil`
- Revenues: `floor`
- Percentage fee components: `ceil`
- Fixed fee components: exact integer

Final integer fields are expressed in whole coins.


