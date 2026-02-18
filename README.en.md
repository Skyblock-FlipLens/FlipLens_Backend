# SkyblockFlipperBackend

[Language: English | [Deutsch](README.md)]

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
- Flip domain structure with:
  - `Flip`, `Step`, `Constraint`, `Recipe`.
  - total/active/passive duration calculation per flip.
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

- **API layer:** `StatusController`
- **Source jobs:** scheduled refresh/ingestion jobs (`SourceJobs`)
- **Domain/model:** flips, steps, constraints, recipes
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

Service will then be available via `docker-compose.yml` on port `8080`.

## Roadmap (Short)

### P0 - Critical
- Unified flip DTO and stable read API (`/api/v1/flips`, `/api/v1/flips/{id}`)
- End-to-end pipeline per flip type (ingest -> compute -> persist -> serve)
- Katgrade as its own flip type (not merged into generic crafting)
- Consistent profit/fee computation

### P1 - Important
- Snapshot system (point-in-time reproducibility)
- Time-weighted metrics (`ROI/h`, active vs. passive time)
- Capital lock-up and resource constraints (e.g. forge slots)

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
