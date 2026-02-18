# SkyblockFlipperBackend

[Language: [English](README.md) | Deutsch]

> **API-First Engine für Hypixel SkyBlock Flips** – einheitliches Datenmodell, reproduzierbare Snapshots und erweiterbare Flip-Analytik.

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
- Persistenz mit Spring Data JPA.
- Öffentliche Read-API für Flips:
  - `GET /api/v1/flips` (Paging + optionaler `flipType`-Filter)
  - `GET /api/v1/flips/{id}`
- Öffentliche Read-API für NPC-Shop-Offers:
  - `GET /api/v1/items/npc-buyable` (Paging + optionaler `itemId`-Filter)
- Datenquellen-Clients für:
  - Hypixel Auction API (einzelne Seite + Multi-Page Fetch).
  - Hypixel Bazaar API (`/skyblock/bazaar`) inkl. `quick_status` und Summary-Strukturen.
  - NEU-Item-Daten (Download/Refresh aus dem NotEnoughUpdates-Repo).
- Geplante/angelegte Domain-Struktur für Flips mit:
  - `Flip`, `Step`, `Constraint`, `Recipe`.
  - Berechnung von Gesamt-/Aktiv-/Passivdauer pro Flip.
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

- **API Layer:** `StatusController`
- **Source Jobs:** periodische Refresh-/Ingestion-Jobs (`SourceJobs`)
- **Domain/Model:** Flips, Steps, Constraints, Recipes
- **Repositories:** `FlipRepository`, `RecipeRepository`, `ItemRepository`, etc.

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
- `GET /api/status` – einfacher Health-/Connectivity-Check (triggert aktuell einen Auction-Fetch).
- `GET /api/v1/flips` – paginierte Liste auf Unified-Flip-Basis, optional filterbar über `flipType`.
- `GET /api/v1/flips/{id}` – Detailansicht eines Flips per UUID.
- `GET /api/v1/items/npc-buyable` – paginierte NPC-Shop-Offerdaten, optional mit `itemId`.

Aktuell noch nicht als öffentliche Endpunkte verfügbar:
- Bazaar-Daten (liegen über `HypixelClient#fetchBazaar()` intern vor).
- Vollständige Item/Recipe-Read-API (`/api/v1/items`, `/api/v1/recipes`, ...).

### Geplante v1-Endpunkte

- `GET /api/v1/flips` (Filter/Sortierung/Pagination)
- `GET /api/v1/flips/{id}` (Detailansicht)
- `GET /api/v1/items` (NEU-basierte Item-Metadaten)
- `GET /api/v1/items/npc-buyable` (NPC-kaufbare Offers, optionaler `itemId`-Filter)
- `GET /api/v1/recipes` (Craft/Forge-Rezepte)
- `GET /api/v1/snapshots`
- `GET /api/v1/snapshots/{timestamp}/flips`

### API-Design-Prinzipien
- Versionierung über `/api/v1/...`
- Konsistente DTOs über alle Flip-Typen
- Deterministische Antworten pro Snapshot
- Erweiterbar ohne Breaking Changes (deprecate-first)

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
- Optional kann ein Hypixel API Key gesetzt werden:
  - `CONFIG_HYPIXEL_API_KEY`

Beispiel:

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

Danach läuft der Service via `docker-compose.yml` auf Port `8080`.

## Roadmap (Kurz)

### P0 – Kritisch
- Unified Flip DTO und stabile Read-API (`/api/v1/flips`, `/api/v1/flips/{id}`)
- End-to-End Pipeline je Flip-Typ (Ingestion → Compute → Persist → Serve)
- Katgrade als eigener Flip-Typ (nicht mit Crafting zusammengelegt)
- Konsistente Profit-/Fee-Berechnung

### P1 – Wichtig
- Snapshot-System (zeitpunktgenaue Reproduzierbarkeit)
- Zeitgewichtete ROI-Kennzahlen (`ROI/h`, aktive vs. passive Zeit)
- Kapitalbindungslogik und Ressourcen-Constraints (z. B. Forge-Slots)

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
