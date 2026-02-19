# SkyblockFlipperBackend

[Language: English | [Deutsch](README.de.md)]

> **API-first engine for Hypixel SkyBlock flips** with a unified data model, reproducible snapshots, and extensible flip analytics.

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
- Persistence via Spring Data JPA.
- Public read API for flips:
  - `GET /api/v1/flips` (paging + optional `flipType` filter)
  - `GET /api/v1/flips/{id}`
- Public read API for NPC-shop offers:
  - `GET /api/v1/items/npc-buyable` (paging + optional `itemId` filter)
- Source clients for:
  - Hypixel Auction API (single page and multi-page fetch).
  - Hypixel Bazaar API (`/skyblock/bazaar`) including `quick_status` and summary structures.
  - NEU item data ingestion (download/refresh from the NotEnoughUpdates repo).
- Market snapshot pipeline:
  - polling, normalization, persistence, retention compaction.
  - timescale feature extraction for risk/liquidity inputs.
- Flip domain structure with:
  - `Flip`, `Step`, `Constraint`, `Recipe`.
  - total/active/passive duration calculation per flip.
- Unified flip DTO mapping with ROI, ROI/h, fees, required capital, liquidity score, risk score, and partial-data flags.
- Flip read endpoints are implemented, but production flip generation/persistence (`flipRepository.save(...)`) is still missing.
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

- **API layer:** `StatusController`, `FlipController`, `ItemController`
- **Source jobs:** scheduled refresh/ingestion jobs (`SourceJobs`)
- **Domain/model:** flips, steps, constraints, recipes, market snapshots
- **Repositories:** `FlipRepository`, `RecipeRepository`, `ItemRepository`, etc.

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
| Auction   | Done (Hypixel auctions -> snapshots) | Partial (works if flips exist) | Missing (no flip writer job/service) | Partial (`/api/v1/flips` read-only) | In progress |
| Bazaar    | Done (Hypixel bazaar -> snapshots) | Partial (works if flips exist) | Missing (no flip writer job/service) | Partial (`/api/v1/flips` read-only) | In progress |
| Craft     | Partial (NEU recipes parsed/stored with items) | Partial (step-based mapping exists) | Missing (no recipe->flip persistence flow) | Missing (`/api/v1/recipes` not exposed) | In progress |
| Forge     | Partial (NEU forge recipes parsed/stored with items) | Partial (duration/resource model exists) | Missing (no recipe->flip persistence flow) | Missing (`/api/v1/recipes` not exposed) | In progress |
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
- `GET /api/status` - basic health/connectivity check (currently triggers an auction fetch).
- `GET /api/v1/flips` - paged unified flip list with optional `flipType` filter.
- `GET /api/v1/flips/{id}` - detail view for a flip by UUID.
- `GET /api/v1/items/npc-buyable` - paged NPC-shop offer data with optional `itemId`.

Not exposed publicly yet:
- Bazaar data (currently available internally via `HypixelClient#fetchBazaar()`).
- Full item/recipe read API (`/api/v1/items`, `/api/v1/recipes`, ...).

### Planned v1 endpoints
- `GET /api/v1/flips` (filtering/sorting/pagination)
- `GET /api/v1/flips/{id}` (detail view)
- `GET /api/v1/items` (NEU-backed item metadata)
- `GET /api/v1/items/npc-buyable` (NPC-buyable offers, optional `itemId` filter)
- `GET /api/v1/recipes` (craft/forge recipes)
- `GET /api/v1/snapshots`
- `GET /api/v1/snapshots/{timestamp}/flips`

### API Design Principles
- Versioned routes via `/api/v1/...`
- Consistent DTOs across flip types
- Deterministic responses per snapshot
- Extensible evolution without breaking changes (`deprecate-first`)

## Implementation Checklist (P0/P1)

### P0 - Critical
1. Implement production flip generation and persistence.
- Create `src/main/java/com/skyblockflipper/backend/service/flipping/FlipGenerationService.java` to map persisted recipes to concrete `Flip` rows.
- Use `RecipeRepository`, `FlipRepository`, and `RecipeToFlipMapper` in one deterministic write flow.
- Call the service from `src/main/java/com/skyblockflipper/backend/config/Jobs/SourceJobs.java` after market snapshot refresh and/or NEU refresh.

2. Add snapshot binding for generated flips.
- Extend `src/main/java/com/skyblockflipper/backend/model/Flipping/Flip.java` with explicit snapshot binding (timestamp or snapshot reference).
- Keep `/api/v1/flips` deterministic per snapshot instead of "latest market + live election only".
- Update mapping/query logic in `src/main/java/com/skyblockflipper/backend/service/flipping/FlipReadService.java`.

3. Expose missing read endpoints for core entities.
- Add `src/main/java/com/skyblockflipper/backend/api/RecipeController.java` (`GET /api/v1/recipes`).
- Add `src/main/java/com/skyblockflipper/backend/api/SnapshotController.java` (`GET /api/v1/snapshots`, `GET /api/v1/snapshots/{timestamp}/flips`).
- Extend `src/main/java/com/skyblockflipper/backend/api/ItemController.java` with `GET /api/v1/items`.

4. Shard-fusion coverage is currently `TBD`.
- Blocked until a reliable, permissively licensed shard-fusion recipe source is selected.
- No shard-fusion ingestion/computation implementation is planned until that source decision is resolved.

### P1 - Important
1. Support explicit as-of context in flip calculation.
- Extend `src/main/java/com/skyblockflipper/backend/service/flipping/FlipCalculationContextService.java` with snapshot/as-of lookup methods.
- Add optional snapshot query params in `src/main/java/com/skyblockflipper/backend/api/FlipController.java`.

2. Move fee/tax logic into a dedicated policy component.
- Extract auction/bazaar tax logic from `src/main/java/com/skyblockflipper/backend/service/flipping/UnifiedFlipDtoMapper.java`.
- Add `src/main/java/com/skyblockflipper/backend/service/flipping/FeePolicyService.java` (or similar) for consistent, testable rules.

3. Strengthen contract tests for deterministic API output.
- Add endpoint tests for snapshot-specific reads in `src/test/java/com/skyblockflipper/backend/api`.
- Add integration tests for end-to-end generation flow in `src/test/java/com/skyblockflipper/backend/service/flipping`.

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
- Optional Hypixel API key:
  - `CONFIG_HYPIXEL_API_KEY`

Example:

```bash
export SPRING_DATASOURCE_URL='jdbc:postgresql://localhost:5432/skyblock'
export SPRING_DATASOURCE_USERNAME='postgres'
export SPRING_DATASOURCE_PASSWORD='postgres'
export SERVER_PORT=8080
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
