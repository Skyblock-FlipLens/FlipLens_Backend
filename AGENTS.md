# Agenda – SkyblockFlipperBackend Audit & Unified Flip API

## 1. Ziel des Projekts
Entwicklung einer API-First Plattform für Hypixel SkyBlock Flips mit einem einheitlichen Datenmodell.

Unterstützte Flip-Typen (Zielzustand):
- Auction Flips
- Bazaar Flips
- Craft Flips
- Forge Flips
- Shard Flips
- Fusion Flips

Die API soll:
- Alle Flip-Typen konsistent abbilden
- Zeit- und Kapitalbindung berücksichtigen
- ROI und ROI/h berechnen
- Snapshot-basiert arbeiten
- Erweiterbar und versionierbar sein

---

## 2. Analyseziele für das bestehende Projekt

### 2.1 Projekt-Inventur
- Spring Boot Version
- Java Version
- Datenbank (Postgres/H2)
- Scheduler/Jobs
- Externe Datenquellen (Hypixel API, NEU Repo)
- Layer-Struktur (Controller / Service / Repository / Model / Jobs)

### 2.2 API-Analyse
- Existierende Endpoints
- DTO-Struktur
- Query-Parameter
- Response-Formate
- Fehlende Standard-Endpunkte (z. B. /flips, /flips/{id}, /items, /recipes)

---

## 3. Flip-Typ Coverage (Ist-Zustand prüfen)

Für jeden Flip-Typ prüfen:
1. Datenbeschaffung (Ingestion)
2. Profit-Berechnung
3. Persistenz (DB-Entity/Repository)
4. API-Ausgabe (Controller/DTO)

| Flip-Typ | Ingestion | Berechnung | Persistenz | API | Status |
|-----------|------------|------------|-------------|------|--------|
| Auction   |            |            |             |      |        |
| Bazaar    |            |            |             |      |        |
| Craft     |            |            |             |      |        |
| Forge     |            |            |             |      |        |
| Shard     |            |            |             |      |        |
| Fusion    |            |            |             |      |        |

---

## 4. Unified Flip Schema (Zielarchitektur)

Ein generisches Flip-Modell sollte enthalten:

- id
- flipType
- inputItems[]
- outputItems[]
- requiredCapital
- expectedProfit
- roi
- roiPerHour
- durationSeconds
- fees
- liquidityScore
- riskScore
- snapshotTimestamp

Ziel:
Alle Flip-Typen sollen in dieses Schema normalisiert werden.

---

## 5. Architektur-Gaps identifizieren

### P0 – Kritisch
- Einheitliches Flip-DTO
- Basale Read-API
- Vollständige Pipeline (Ingestion → Compute → Persist → Serve)

### P1 – Wichtig
- Snapshot-System
- Zeitgewichtete ROI-Berechnung
- Kapitalbindung
- Konsistente Gebühren-/Tax-Logik

### P2 – Differenzierungsmerkmale
- Liquidity-Score
- Risk-Adjusted ROI
- Slippage-Modell
- Multi-Step Flip Chains (DAG)
- Backtesting-Funktionalität

---

## 6. Potenzielle USPs / Marktlücken

1. Unified Flip Abstraction Layer (API-First statt UI-First)
2. Risk & Liquidity Normalized Profit
3. Multi-Step Chain Engine
4. Snapshot & Backtesting API
5. Versioniertes, stabiles Public API Contract

---

## 7. Nächste Schritte

1. Bestehende Implementierungen je Flip-Typ mappen
2. Fehlende Layer pro Typ identifizieren
3. Unified Domain Model definieren
4. API-Contract festlegen (v1)
5. Snapshot-Strategie entwerfen
6. Optional: Monetarisierungsmodell evaluieren

---

## 8. Technische Leitprinzipien

- API-First Design
- Saubere Layer-Trennung (Controller / Service / Domain / Persistence)
- Keine Flip-Typ-spezifischen Sonderlogiken außerhalb der Domain
- Erweiterbarkeit für neue Flip-Arten
- Deterministische Berechnungen
- Reproduzierbare Snapshots

---

Dieses Dokument dient als Grundlage für die systematische Analyse und Weiterentwicklung des SkyblockFlipperBackend-Projekts.

Für aktuelle Endpoint-Details siehe `MarkdownFiles/API_ENDPOINTS.md`.

