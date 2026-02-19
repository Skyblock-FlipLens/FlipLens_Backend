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
- Market-Snapshot-Pipeline:
  - Polling, Normalisierung, Persistenz, Retention-Compaction.
  - Timescale-Feature-Berechnung für Risk-/Liquidity-Signale.
- Geplante/angelegte Domain-Struktur für Flips mit:
  - `Flip`, `Step`, `Constraint`, `Recipe`.
  - Berechnung von Gesamt-/Aktiv-/Passivdauer pro Flip.
- Unified-Flip-DTO-Mapping mit ROI, ROI/h, Fees, Required Capital, Liquidity Score, Risk Score und Partial-Flags.
- Flip-Read-Endpoints sind vorhanden, aber die produktive Flip-Generierung/Persistenz (`flipRepository.save(...)`) fehlt aktuell noch.
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

- **API Layer:** `StatusController`, `FlipController`, `ItemController`
- **Source Jobs:** periodische Refresh-/Ingestion-Jobs (`SourceJobs`)
- **Domain/Model:** Flips, Steps, Constraints, Recipes, Market Snapshots
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

## Coverage-Snapshot (Ist-Zustand)

Status-Legende: `Done` = produktiver Codepfad vorhanden, `Partial` = teilweise vorhanden aber nicht vollständig verdrahtet, `Missing` = noch nicht implementiert, `TBD` = bewusst zurückgestellt, bis eine lizenzierte Datenquelle für Shard-Fusion-Rezepte vorliegt.

| Flip-Typ | Ingestion | Berechnung | Persistenz | API | Status |
|----------|-----------|------------|------------|-----|--------|
| Auction  | Done (Hypixel Auctions -> Snapshots) | Partial (funktioniert, wenn Flips existieren) | Missing (kein Flip-Writer-Job/Service) | Partial (`/api/v1/flips` read-only) | In Arbeit |
| Bazaar   | Done (Hypixel Bazaar -> Snapshots) | Partial (funktioniert, wenn Flips existieren) | Missing (kein Flip-Writer-Job/Service) | Partial (`/api/v1/flips` read-only) | In Arbeit |
| Craft    | Partial (NEU-Rezepte werden geparst/gespeichert) | Partial (Step-basiertes Mapping vorhanden) | Missing (kein Recipe->Flip-Persistenzfluss) | Missing (`/api/v1/recipes` fehlt) | In Arbeit |
| Forge    | Partial (NEU-Forge-Rezepte werden geparst/gespeichert) | Partial (Duration-/Resource-Modell vorhanden) | Missing (kein Recipe->Flip-Persistenzfluss) | Missing (`/api/v1/recipes` fehlt) | In Arbeit |
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

## Umsetzungs-Checklist (P0/P1)

### P0 – Kritisch
1. Produktive Flip-Generierung und Persistenz implementieren.
- `src/main/java/com/skyblockflipper/backend/service/flipping/FlipGenerationService.java` anlegen, um persistierte Rezepte in konkrete `Flip`-Rows zu überführen.
- `RecipeRepository`, `FlipRepository` und `RecipeToFlipMapper` in einem deterministischen Write-Flow verbinden.
- Service in `src/main/java/com/skyblockflipper/backend/config/Jobs/SourceJobs.java` nach Snapshot-Refresh und/oder NEU-Refresh triggern.

2. Snapshot-Bindung für generierte Flips einführen.
- `src/main/java/com/skyblockflipper/backend/model/Flipping/Flip.java` um explizite Snapshot-Bindung erweitern (Timestamp oder Snapshot-Referenz).
- `/api/v1/flips` deterministisch pro Snapshot machen statt rein "latest market + live election".
- Query/Mapping in `src/main/java/com/skyblockflipper/backend/service/flipping/FlipReadService.java` anpassen.

3. Fehlende Read-Endpunkte für Kernobjekte veröffentlichen.
- `src/main/java/com/skyblockflipper/backend/api/RecipeController.java` ergänzen (`GET /api/v1/recipes`).
- `src/main/java/com/skyblockflipper/backend/api/SnapshotController.java` ergänzen (`GET /api/v1/snapshots`, `GET /api/v1/snapshots/{timestamp}/flips`).
- `src/main/java/com/skyblockflipper/backend/api/ItemController.java` um `GET /api/v1/items` erweitern.

4. Shard-Fusion-Abdeckung ist aktuell `TBD`.
- Blockiert, bis eine verlässliche und permissiv lizenzierte Datenquelle für Shard-Fusion-Rezepte festgelegt ist.
- Vor dieser Entscheidung ist keine Implementierung für Ingestion/Berechnung von Shard-Fusion-Flips geplant.

### P1 – Wichtig
1. Expliziten As-Of-Kontext in der Flip-Berechnung unterstützen.
- `src/main/java/com/skyblockflipper/backend/service/flipping/FlipCalculationContextService.java` um Snapshot-/As-Of-Lookup erweitern.
- Optionale Snapshot-Query-Parameter in `src/main/java/com/skyblockflipper/backend/api/FlipController.java` ergänzen.

2. Fee-/Tax-Logik in dedizierten Policy-Service auslagern.
- Auction-/Bazaar-Tax-Logik aus `src/main/java/com/skyblockflipper/backend/service/flipping/UnifiedFlipDtoMapper.java` extrahieren.
- `src/main/java/com/skyblockflipper/backend/service/flipping/FeePolicyService.java` (oder ähnlich) als zentrale Regelkomponente ergänzen.

3. Contract-Tests für deterministische API-Ausgabe ausbauen.
- Endpoint-Tests für snapshot-spezifische Reads in `src/test/java/com/skyblockflipper/backend/api` ergänzen.
- Integrations-Tests für den End-to-End-Generate-Flow in `src/test/java/com/skyblockflipper/backend/service/flipping` ergänzen.

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

Danach läuft der Service via `docker-compose.yml` standardmäßig auf Port `1880`.
Du kannst das mit `SERVER_PORT` überschreiben, zum Beispiel:

```bash
SERVER_PORT=8080 docker compose up --build
```

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
