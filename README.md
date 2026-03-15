# SkyblockFlipperBackend

[Language: English | [Deutsch](README.de.md)]

> **API-first engine for Hypixel SkyBlock flips** with a unified data model, reproducible snapshots, and extensible flip analytics.

Current release stream: `1.0.x` stabilization and homelab rollout. `1.1.0` remains reserved for the first public release line. See [CHANGELOG.md](CHANGELOG.md) for patch-level notes.

## Vision

**SkyblockFlipperBackend** aims to provide a stable, versioned, API-first foundation for flip data in the Hypixel SkyBlock ecosystem.

Target state:
- A **unified flip model** across flip categories.
- A clear pipeline: **ingestion -> normalization -> computation -> persistence -> API delivery**.
- Deterministic calculations with a focus on **ROI/ROI-h**, **capital lock-up**, and later **risk/liquidity scoring**.
- Platform-first backend for dashboards, bots, and research tooling.

## Features (Current State)

Currently implemented in this repository:
- Spring Boot 4 backend with Java 21.
- Persistence via Spring Data JPA on PostgreSQL/H2.
- Public read APIs for flips, snapshots, items, item analytics, recipes, bazaar, auction-house data, dashboard summaries, and market overview.
- Source clients for:
  - Hypixel Auction API (single page and multi-page fetch).
  - Hypixel Bazaar API (`/skyblock/bazaar`) including `quick_status` and summary structures.
  - NEU item data ingestion (download/refresh from the NotEnoughUpdates repo).
- Adaptive market snapshot pipeline:
  - independent auctions/bazaar pollers with coalescing pipeline semantics.
  - normalization, persistence, retention compaction, rollups, and diagnostics hooks.
- Flip domain structure with:
  - `Flip`, `Step`, `Constraint`, `Recipe`.
  - total/active/passive duration calculation per flip.
- Unified flip generation and persistence:
  - `FlipGenerationService` generates market and recipe-backed flips per snapshot.
  - writes can dual-write into legacy snapshot rows and unified storage (`flip_definition`, `flip_current`, `flip_trend_segment`).
  - current read paths default to unified storage.
- Unified flip DTO mapping with ROI, ROI/h, fees, required capital, liquidity score, risk score, and partial-data flags.
- Lightweight local `/api/status` health probe with no upstream Hypixel dependency.
- Scheduling infrastructure (thread pool + scheduled jobs).
- Resilient Hypixel client behavior (HTTP/network failures are logged).
- `fetchAllAuctions()` is fail-fast on incomplete page fetches to avoid persisting false empty market states.
- Dockerfile + docker-compose for containerized runtime.

## Architecture

### Overview

```text
[Hypixel API]        [NEU Repo / Items]
      |                     |
      v                     v
 HypixelClient         NEUClient + Filter/Mapper
      |                     |
      +--------- Ingestion & Normalization --------+
                                                    v
                                          Domain Model (Flip/Step/Recipe)
                                                    |
                                                    v
                                           Spring Data Repositories
                                                    |
                                                    v
                                                REST API
```

### Tech Stack

- **Runtime:** Java 21
- **Framework:** Spring Boot 4 (`web`, `validation`, `actuator`)
- **Persistence:** Spring Data JPA
- **Databases:** PostgreSQL (runtime), H2 (tests)
- **Scheduling:** `@EnableScheduling`, `@Scheduled`, `ThreadPoolTaskScheduler`
- **External clients:**
  - Hypixel REST via `RestClient`
  - NEU repo download/refresh via `HttpClient` + ZIP extraction
- **Build/Test:** Maven Wrapper, Surefire, JaCoCo
- **Container:** Multi-stage Docker build + Distroless runtime image

### Components (Simplified)

- **API layer:** `StatusController`, `FlipController`, `DashboardController`, `MarketController`, `ItemController`, `RecipeController`, `SnapshotController`, `BazaarController`, `AuctionHouseController`
- **Source jobs:** scheduled refresh/ingestion jobs (`SourceJobs`)
- **Domain/model:** flips, steps, constraints, recipes, market snapshots
- **Repositories/storage:** legacy snapshot repositories plus unified flip storage tables (`flip_definition`, `flip_current`, `flip_trend_segment`)

## Supported Flip Types

### Already present in `FlipType`
- **Auction** (`AUCTION`)
- **Bazaar** (`BAZAAR`)
- **Crafting** (`CRAFTING`)
- **Forge** (`FORGE`)
- **Katgrade** (`KATGRADE`)
- **Fusion** (`FUSION`)

### Target Coverage (Roadmap)
- Auction flips
- Bazaar flips
- Craft flips
- Forge flips
- Katgrade flips
- Shard flips
- Fusion flips

> Note: Core domain objects are already present; full end-to-end coverage for all target flip types is still in progress.

## Coverage Snapshot (As-Is)

Status legend: `Done` = implemented in production code path, `Partial` = available but not fully wired, `Missing` = not yet implemented, `TBD` = intentionally deferred pending a licensed shard-fusion recipe source.

| Flip Type | Ingestion | Computation | Persistence | API | Status |
|-----------|-----------|-------------|-------------|-----|--------|
| Auction   | Done (adaptive Hypixel auctions -> snapshots) | Done (`MarketFlipMapper` + `UnifiedFlipDtoMapper` + `FlipEconomicsService`) | Done (`FlipGenerationService` -> `UnifiedFlipStorageService`, optional legacy snapshot rows) | Done (`/api/v1/flips`, `/api/v1/ah`, `/api/v1/dashboard`) | Active |
| Bazaar    | Done (adaptive Hypixel bazaar -> snapshots) | Done (`MarketFlipMapper` + `UnifiedFlipDtoMapper` + `FlipEconomicsService`) | Done (`FlipGenerationService` -> `UnifiedFlipStorageService`, optional legacy snapshot rows) | Done (`/api/v1/flips`, `/api/v1/bazaar`, `/api/v1/market/overview`) | Active |
| Craft     | Done (NEU recipes parsed/stored with items) | Done (`RecipeToFlipMapper` + `UnifiedFlipDtoMapper` + `FlipEconomicsService`) | Done (`FlipGenerationService` -> `UnifiedFlipStorageService`, optional legacy snapshot rows) | Done (`/api/v1/flips?flipType=CRAFTING`, `/api/v1/recipes`) | Active |
| Forge     | Done (NEU forge recipes parsed/stored with items) | Done (`RecipeToFlipMapper` + `UnifiedFlipDtoMapper` + `FlipEconomicsService`) | Done (`FlipGenerationService` -> `UnifiedFlipStorageService`, optional legacy snapshot rows) | Done (`/api/v1/flips?flipType=FORGE`, `/api/v1/recipes`) | Active |
| Shard     | TBD (blocked: shard-fusion recipe source pending) | TBD | TBD | TBD | TBD |
| Fusion    | TBD (blocked: shard-fusion recipe source pending; enum exists) | Partial (generic DTO supports it) | TBD | Partial (`/api/v1/flips` can read if rows exist) | TBD |

Additional note:
- `KATGRADE` is implemented as a first-class type in code, but it is not listed in the original target table.

## Unified Flip Schema (Short)

Planned core fields:
- `id`, `flipType`, `snapshotTimestamp`
- `inputItems`, `outputItems`, `steps`, `constraints`
- `requiredCapital`, `expectedProfit`, `fees`
- `roi`, `roiPerHour`, `durationSeconds`
- `liquidityScore`, `riskScore`

Short example:
```json
{
  "id": "uuid",
  "flipType": "FORGE",
  "requiredCapital": 1250000,
  "expectedProfit": 185000,
  "roi": 0.148,
  "roiPerHour": 0.032,
  "durationSeconds": 16600
}
```

## API Endpoints (Current + Planned)

### Available now
- `GET /api/status`
- `GET /api/v1/flips`, `/filter`, `/top`, `/top/*`, `/stats`, `/stats/snapshot`, `/coverage`, `/types`, `/{id}`
- `GET /api/v1/dashboard/overview`, `GET /api/v1/dashboard/trending`
- `GET /api/v1/market/overview`
- `GET /api/v1/items`, `GET /api/v1/items/{itemId}`, `GET /api/v1/items/{itemId}/price-history`, `GET /api/v1/items/{itemId}/score-history`, `GET /api/v1/items/{itemId}/quick-stats`, `GET /api/v1/items/{itemId}/flips`, `GET /api/v1/items/npc-buyable`
- `GET /api/v1/recipes`, `GET /api/v1/recipes/{recipeId}/cost`
- `GET /api/v1/snapshots`, `GET /api/v1/snapshots/{timestamp}/flips`
- `GET /api/v1/bazaar/{itemId}`, `GET /api/v1/bazaar/{itemId}/orders`, `GET /api/v1/bazaar/quick-flips`
- `GET /api/v1/ah/listings/{itemId}`, `GET /api/v1/ah/listings/{itemId}/breakdown`, `GET /api/v1/ah/recent-sales/{itemId}`

For the full request/response reference, see [MarkdownFiles/API_ENDPOINTS.md](MarkdownFiles/API_ENDPOINTS.md).

### Planned toward `1.1.0`
- complete shard/fusion source decision and end-to-end pipeline
- cache and further harden the slowest aggregate read paths
- finish release-grade observability, SLOs, and operational runbooks

### API Design Principles
- Versioned routes via `/api/v1/...`
- Consistent DTOs across flip types
- Deterministic responses per snapshot
- Extensible evolution without breaking changes (`deprecate-first`)

## Next Milestones (Toward `1.1.0`)

1. Close the remaining shard/fusion source decision and pipeline gaps.
2. Add cache layers for the heaviest aggregate reads after the SQL path is stable.
3. Finish hot/cold storage rollout validation, parity automation, and rollback documentation.
4. Lock release SLOs, release notes discipline, and public-release operational runbooks.

## Final Validation Gate

Before considering implementation complete, run a live end-to-end smoke test against real upstream data.
- Execute a full refresh cycle (Hypixel + NEU), generation cycle, and read API verification on a clean DB.
- Verify snapshot determinism (`/api/v1/snapshots/{timestamp}/flips` equals expected snapshot-bound results).
- Verify no-op/regen behavior is correct across consecutive cycles and after NEU refresh.
- Verify recommendation economics: flips presented as recommendations must be net-profitable at the tested snapshot (`expectedProfit > 0` after fees/taxes), and not just mathematically valid.
- Spot-check a sample of top-ranked flips against the same snapshot inputs to confirm profit direction and order are plausible.
- Record the run timestamp, environment, and key metrics in release notes.

## Run (Local & Docker)

### Requirements
- Java 21
- Docker (optional)

### Local

```bash
./mvnw clean test
./mvnw spring-boot:run
```

Notes:
- Default profile expects:
  - `SPRING_DATASOURCE_URL`
  - `SPRING_DATASOURCE_USERNAME`
  - `SPRING_DATASOURCE_PASSWORD`
- Server port is controlled by `SERVER_PORT` (fallback in config file).
- Flyway baselining is opt-in via `FLYWAY_BASELINE_ON_MIGRATE` (default: `false`).
- Optional Hypixel API key:
  - `CONFIG_HYPIXEL_API_KEY`

Example:

```bash
export SPRING_DATASOURCE_URL='jdbc:postgresql://localhost:5432/skyblock'
export SPRING_DATASOURCE_USERNAME='postgres'
export SPRING_DATASOURCE_PASSWORD='postgres'
export SERVER_PORT=8080
export FLYWAY_BASELINE_ON_MIGRATE=false
./mvnw spring-boot:run
```

### Docker

```bash
docker compose up --build
```

Service will then be available via `docker-compose.yml` on port `1880` by default.
You can override this by setting `SERVER_PORT`, for example:

```bash
SERVER_PORT=8080 docker compose up --build
```

To run directly from this Git repository (without cloning locally), use `docker-compose.repo.yml`:

```bash
REPO_GIT_URL='https://github.com/crafter32/SkyblockFlipperBackend.git' \
REPO_GIT_REF='main' \
SERVER_PORT=8080 \
SPRING_DATASOURCE_USERNAME=postgres \
SPRING_DATASOURCE_PASSWORD=postgres \
FLYWAY_BASELINE_ON_MIGRATE=false \
POSTGRES_USER=postgres \
POSTGRES_PASSWORD=postgres \
docker compose -f docker-compose.repo.yml up --build
```

Use `REPO_GIT_URL`/`REPO_GIT_REF` to point to a fork or different branch/tag.

For direct image runs (`docker run`), the `Dockerfile` sets a default `SERVER_PORT=8080`.

## Roadmap (Short)

### P0 - Critical
- End-to-end pipeline per flip type (ingest -> compute -> persist -> serve)
- Snapshot-bound deterministic reads
- Missing core read endpoints (`/api/v1/items`, `/api/v1/recipes`, `/api/v1/snapshots`)
- Shard-fusion recipes remain `TBD` pending licensed source availability

### P1 - Important
- Explicit as-of/snapshot selectors in public API
- Time-weighted metrics (`ROI/h`, active vs. passive time)
- Capital lock-up and resource constraints (e.g. forge slots)
- Unified, centralized fee/tax policy

### P2 - Differentiation
- Liquidity and risk scoring
- Risk-adjusted ranking vs. raw profit sorting
- Slippage/fill-probability model
- Multi-step flip chains (DAG)
- Backtesting API for historical snapshots

USP focus:
- Unified API abstraction across flip types
- Reproducible snapshots for analytics/backtesting
- Risk/liquidity-normalized decision support

## Contributing

Contributions are welcome.

Recommended process:
1. Create a branch (`feature/...`, `fix/...`).
2. Add or update tests with your changes.
3. Open a pull request with clear scope and impact.
4. Keep API contracts stable and backward compatible.

Guidelines:
- Keep PRs small and focused.
- Avoid breaking changes without a versioning strategy.
- Integrate new flip types through the unified model.


