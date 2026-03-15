# SkyblockFlipperBackend

[Language: [English](README.md) | Deutsch]

> **API-First Engine für Hypixel SkyBlock Flips** – einheitliches Datenmodell, reproduzierbare Snapshots und erweiterbare Flip-Analytik.

Aktueller Release-Stream: `1.0.x` für Stabilisierung und Homelab-Rollout. `1.1.0` bleibt für die erste öffentliche Release-Linie reserviert. Patch-Änderungen stehen in [CHANGELOG.md](CHANGELOG.md).

## Vision

**SkyblockFlipperBackend** soll die technische Grundlage für eine stabile, versionierbare Flip-API im Hypixel-SkyBlock-Ökosystem werden.

Zielbild:
- Ein **Unified Flip Model** über alle Flip-Arten hinweg.
- Eine klare Pipeline von **Ingestion → Normalisierung → Berechnung → Persistenz → API-Auslieferung**.
- Fokus auf **deterministische Berechnungen**, **ROI/ROI-h**, **Kapitalbindung** und später **Risk/Liquidity-Scoring**.
- API-First statt UI-First: Das Backend ist als Plattform gedacht, auf der Dashboards, Bots oder Research-Tools aufsetzen können.

## Funktionen (Ist-Stand)

Aktueller Stand (im Repository vorhanden):
- Spring Boot 4 Backend mit Java 21.
- Persistenz mit Spring Data JPA auf PostgreSQL/H2.
- Öffentliche Read-APIs für Flips, Snapshots, Items, Item-Analytik, Rezepte, Bazaar, Auction-House-Daten, Dashboard-Summaries und Market-Overview.
- Datenquellen-Clients für:
  - Hypixel Auction API (einzelne Seite + Multi-Page Fetch).
  - Hypixel Bazaar API (`/skyblock/bazaar`) inkl. `quick_status` und Summary-Strukturen.
  - NEU-Item-Daten (Download/Refresh aus dem NotEnoughUpdates-Repo).
- Adaptive Market-Snapshot-Pipeline:
  - getrennte Auctions-/Bazaar-Poller mit coalescing Pipeline-Semantik.
  - Normalisierung, Persistenz, Retention-Compaction, Rollups und Diagnostics-Hooks.
- Geplante/angelegte Domain-Struktur für Flips mit:
  - `Flip`, `Step`, `Constraint`, `Recipe`.
  - Berechnung von Gesamt-/Aktiv-/Passivdauer pro Flip.
- Unified-Flip-Generierung und Persistenz:
  - `FlipGenerationService` erzeugt markt- und rezeptbasierte Flips pro Snapshot.
  - Writes können parallel in Legacy-Snapshot-Rows und Unified Storage (`flip_definition`, `flip_current`, `flip_trend_segment`) gehen.
  - aktuelle Read-Pfade nutzen standardmäßig Unified Storage.
- Unified-Flip-DTO-Mapping mit ROI, ROI/h, Fees, Required Capital, Liquidity Score, Risk Score und Partial-Flags.
- Leichter lokaler `/api/status`-Health-Check ohne Upstream-Hypixel-Abhängigkeit.
- Scheduling-Infrastruktur (ThreadPool + geplante Jobs).
- Robuste Fehlerbehandlung im Hypixel-Client (HTTP/Netzwerkfehler werden geloggt).
- `fetchAllAuctions()` arbeitet fail-fast bei unvollständigen Seitenabrufen, um keine leeren Marktzustände zu persistieren.
- Dockerfile + docker-compose für Container-Betrieb.

## Architektur

### Überblick

```text
[Hypixel API]        [NEU Repo / Items]
      |                     |
      v                     v
 HypixelClient         NEUClient + Filter/Mapper
      |                     |
      +--------- Ingestion & Normalisierung --------+
                                                    v
                                          Domain Model (Flip/Step/Recipe)
                                                    |
                                                    v
                                           Spring Data Repositories
                                                    |
                                                    v
                                                REST API
```

### Technologie-Stack

- **Runtime:** Java 21
- **Framework:** Spring Boot 4 (`web`, `validation`, `actuator`)
- **Persistenz:** Spring Data JPA
- **Datenbanken:** PostgreSQL (Betrieb), H2 (Tests)
- **Scheduling:** `@EnableScheduling`, `@Scheduled`, `ThreadPoolTaskScheduler`
- **Externe Clients:**
  - Hypixel REST via `RestClient`
  - NEU-Repo Download/Refresh via `HttpClient` + ZIP-Extraktion
- **Build/Test:** Maven Wrapper, Surefire, JaCoCo
- **Container:** Multi-stage Docker Build + Distroless Runtime Image

### Komponenten (vereinfacht)

- **API Layer:** `StatusController`, `FlipController`, `DashboardController`, `MarketController`, `ItemController`, `RecipeController`, `SnapshotController`, `BazaarController`, `AuctionHouseController`
- **Source Jobs:** periodische Refresh-/Ingestion-Jobs (`SourceJobs`)
- **Domain/Model:** Flips, Steps, Constraints, Recipes, Market Snapshots
- **Repositories/Storage:** Legacy-Snapshot-Repositories plus Unified-Flip-Storage (`flip_definition`, `flip_current`, `flip_trend_segment`)

## Unterstützte Flip-Typen

### Bereits im Domain-Modell als `FlipType` vorhanden
- **Auction** (`AUCTION`)
- **Bazaar** (`BAZAAR`)
- **Crafting** (`CRAFTING`)
- **Forge** (`FORGE`)
- **Katgrade** (`KATGRADE`)
- **Fusion** (`FUSION`)

### Zielbild (Roadmap)
- Auction Flips
- Bazaar Flips
- Craft Flips
- Forge Flips
- Katgrade Flips
- Shard Flips
- Fusion Flips

> Hinweis: Aktuell sind im Code bereits die grundlegenden Flip-Domainobjekte vorhanden; die vollständige End-to-End-Abdeckung aller Ziel-Fliptypen ist als nächster Ausbauschritt zu sehen.

## Coverage-Snapshot (Ist-Zustand)

Status-Legende: `Done` = produktiver Codepfad vorhanden, `Partial` = teilweise vorhanden aber nicht vollständig verdrahtet, `Missing` = noch nicht implementiert, `TBD` = bewusst zurückgestellt, bis eine lizenzierte Datenquelle für Shard-Fusion-Rezepte vorliegt.

| Flip-Typ | Ingestion | Berechnung | Persistenz | API | Status |
|----------|-----------|------------|------------|-----|--------|
| Auction  | Done (adaptive Hypixel Auctions -> Snapshots) | Done (`MarketFlipMapper` + `UnifiedFlipDtoMapper` + `FlipEconomicsService`) | Done (`FlipGenerationService` -> `UnifiedFlipStorageService`, optionale Legacy-Snapshot-Rows) | Done (`/api/v1/flips`, `/api/v1/ah`, `/api/v1/dashboard`) | Aktiv |
| Bazaar   | Done (adaptive Hypixel Bazaar -> Snapshots) | Done (`MarketFlipMapper` + `UnifiedFlipDtoMapper` + `FlipEconomicsService`) | Done (`FlipGenerationService` -> `UnifiedFlipStorageService`, optionale Legacy-Snapshot-Rows) | Done (`/api/v1/flips`, `/api/v1/bazaar`, `/api/v1/market/overview`) | Aktiv |
| Craft    | Done (NEU-Rezepte werden geparst/gespeichert) | Done (`RecipeToFlipMapper` + `UnifiedFlipDtoMapper` + `FlipEconomicsService`) | Done (`FlipGenerationService` -> `UnifiedFlipStorageService`, optionale Legacy-Snapshot-Rows) | Done (`/api/v1/flips?flipType=CRAFTING`, `/api/v1/recipes`) | Aktiv |
| Forge    | Done (NEU-Forge-Rezepte werden geparst/gespeichert) | Done (`RecipeToFlipMapper` + `UnifiedFlipDtoMapper` + `FlipEconomicsService`) | Done (`FlipGenerationService` -> `UnifiedFlipStorageService`, optionale Legacy-Snapshot-Rows) | Done (`/api/v1/flips?flipType=FORGE`, `/api/v1/recipes`) | Aktiv |
| Shard    | TBD (blockiert: Datenquelle für Shard-Fusion-Rezepte ausstehend) | TBD | TBD | TBD | TBD |
| Fusion   | TBD (blockiert: Datenquelle für Shard-Fusion-Rezepte ausstehend; Enum vorhanden) | Partial (generisches DTO unterstützt Typ) | TBD | Partial (`/api/v1/flips` liest, falls Rows existieren) | TBD |

Zusätzlicher Hinweis:
- `KATGRADE` ist im Code als eigener Typ implementiert, steht aber nicht in der ursprünglichen Ziel-Tabelle.

## Unified Flip Schema (Kurzfassung)

Geplante Kernfelder:
- `id`, `flipType`, `snapshotTimestamp`
- `inputItems`, `outputItems`, `steps`, `constraints`
- `requiredCapital`, `expectedProfit`, `fees`
- `roi`, `roiPerHour`, `durationSeconds`
- `liquidityScore`, `riskScore`

Beispiel (gekürzt):
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

## API-Endpunkte (Ist + Planung)

### Bereits vorhanden
- `GET /api/status`
- `GET /api/v1/flips`, `/filter`, `/top`, `/top/*`, `/stats`, `/stats/snapshot`, `/coverage`, `/types`, `/{id}`
- `GET /api/v1/dashboard/overview`, `GET /api/v1/dashboard/trending`
- `GET /api/v1/market/overview`
- `GET /api/v1/items`, `GET /api/v1/items/{itemId}`, `GET /api/v1/items/{itemId}/price-history`, `GET /api/v1/items/{itemId}/score-history`, `GET /api/v1/items/{itemId}/quick-stats`, `GET /api/v1/items/{itemId}/flips`, `GET /api/v1/items/npc-buyable`
- `GET /api/v1/recipes`, `GET /api/v1/recipes/{recipeId}/cost`
- `GET /api/v1/snapshots`, `GET /api/v1/snapshots/{timestamp}/flips`
- `GET /api/v1/bazaar/{itemId}`, `GET /api/v1/bazaar/{itemId}/orders`, `GET /api/v1/bazaar/quick-flips`
- `GET /api/v1/ah/listings/{itemId}`, `GET /api/v1/ah/listings/{itemId}/breakdown`, `GET /api/v1/ah/recent-sales/{itemId}`

Die vollständige Request-/Response-Referenz steht in [MarkdownFiles/API_ENDPOINTS.md](MarkdownFiles/API_ENDPOINTS.md).

### Geplant Richtung `1.1.0`

- Entscheidung zur Shard-/Fusion-Datenquelle und vollständige Pipeline
- Cache-Layer und weitere Latenz-Härtung für die teuersten Aggregat-Reads
- Release-taugliche Observability, SLOs und operative Runbooks

### API-Design-Prinzipien
- Versionierung über `/api/v1/...`
- Konsistente DTOs über alle Flip-Typen
- Deterministische Antworten pro Snapshot
- Erweiterbar ohne Breaking Changes (deprecate-first)

## Nächste Meilensteine (Richtung `1.1.0`)

1. Verbleibende Shard-/Fusion-Lücken nach Datenquellen-Entscheidung schließen.
2. Cache-Layer für die teuersten Aggregat-Reads ergänzen, sobald der SQL-Pfad stabil ist.
3. Hot/Cold-Storage-Rollout mit Parity-Automation und Rollback-Dokumentation fertigziehen.
4. Release-SLOs, Release-Notes-Disziplin und operative Runbooks finalisieren.

## Finales Validierungs-Gate

Bevor die Implementierung als abgeschlossen gilt, muss ein Live-End-to-End-Smoke-Test mit echten Upstream-Daten laufen.
- Vollen Refresh-Zyklus ausführen (Hypixel + NEU), danach Generate-Zyklus und Read-API-Verifikation auf sauberer DB.
- Snapshot-Determinismus prüfen (`/api/v1/snapshots/{timestamp}/flips` muss snapshot-gebundene Ergebnisse liefern).
- Korrektes No-Op-/Regenerate-Verhalten über mehrere Zyklen und nach NEU-Refresh prüfen.
- Empfehlungs-Ökonomie prüfen: als Empfehlung ausgegebene Flips müssen im getesteten Snapshot netto profitabel sein (`expectedProfit > 0` nach Fees/Taxes), nicht nur formal berechenbar.
- Stichprobe der Top-Flips gegen dieselben Snapshot-Inputs gegenprüfen, damit Profit-Richtung und Ranking plausibel sind.
- Run-Zeitpunkt, Umgebung und Kernmetriken in den Release-Notizen dokumentieren.

## Starten (Lokal & Docker)

### Voraussetzungen
- Java 21
- Docker (optional, für Containerbetrieb)

### Lokal

```bash
./mvnw clean test
./mvnw spring-boot:run
```

Hinweise:
- Das Standardprofil erwartet DB-Variablen:
  - `SPRING_DATASOURCE_URL`
  - `SPRING_DATASOURCE_USERNAME`
  - `SPRING_DATASOURCE_PASSWORD`
- Der Server-Port ist über `SERVER_PORT` steuerbar (Default fallback im Config-File).
- Flyway-Baselining ist opt-in über `FLYWAY_BASELINE_ON_MIGRATE` (Default: `false`).
- Optional kann ein Hypixel API Key gesetzt werden:
  - `CONFIG_HYPIXEL_API_KEY`

Beispiel:

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

Danach läuft der Service via `docker-compose.yml` standardmäßig auf Port `1880`.
Du kannst das mit `SERVER_PORT` überschreiben, zum Beispiel:

```bash
SERVER_PORT=8080 docker compose up --build
```


Um direkt aus diesem Git-Repository zu starten (ohne lokalen Checkout), nutze `docker-compose.repo.yml`:

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

Mit `REPO_GIT_URL`/`REPO_GIT_REF` kannst du auch einen Fork oder einen anderen Branch/Tag verwenden.

Beim direkten Start des Images (`docker run`) setzt das `Dockerfile` standardmäßig `SERVER_PORT=8080`.

## Roadmap (Kurz)

### P0 – Kritisch
- End-to-End Pipeline je Flip-Typ (Ingestion → Compute → Persist → Serve)
- Snapshot-gebundene deterministische Reads
- Fehlende Kern-Read-Endpunkte (`/api/v1/items`, `/api/v1/recipes`, `/api/v1/snapshots`)
- Shard-Fusion-Rezepte bleiben `TBD`, bis eine lizenzierte Datenquelle verfügbar ist

### P1 – Wichtig
- Explizite As-Of-/Snapshot-Selektoren in der Public API
- Zeitgewichtete ROI-Kennzahlen (`ROI/h`, aktive vs. passive Zeit)
- Kapitalbindungslogik und Ressourcen-Constraints (z. B. Forge-Slots)
- Vereinheitlichte, zentralisierte Fee-/Tax-Policy

### P2 – Differenzierung
- Liquidity Score + Risk Score
- Risk-adjusted Ranking statt reinem Profit-Sorting
- Slippage/Fill-Probability Modell
- Multi-Step Flip Chains (DAG) inkl. Optimierung
- Backtesting API für historische Snapshots

USP-Fokus:
- Einheitlicher API-Contract für alle Flip-Typen.
- Reproduzierbare Snapshots für Analyse und Backtesting.
- Risiko-/Liquiditäts-normalisierte Bewertung statt reinem Profit-Ranking.

## Mitwirken

Beiträge sind willkommen.

Empfohlener Ablauf:
1. Fork/Branch erstellen (`feature/...`, `fix/...`).
2. Änderungen mit Tests ergänzen.
3. Pull Request mit klarer Beschreibung (Problem, Lösung, Auswirkungen) öffnen.
4. Auf konsistente API-Verträge und Rückwärtskompatibilität achten.

Leitlinien:
- Kleine, fokussierte PRs.
- Keine Breaking Changes ohne Versionierungsstrategie.
- Neue Flip-Typen über das Unified Model integrieren.
## Update: Market Overview Endpoint

- Neuer Endpunkt verfügbar: `GET /api/v1/market/overview` (optionaler Query-Parameter: `productId`). Er liefert eine kompakte Marktübersicht mit Kauf/Verkauf/Spread, 7-Tage-Spanne, Volumen-Durchschnitten, aktiven Flips und bestem Profit für schnelle Dashboard-Nutzung.

