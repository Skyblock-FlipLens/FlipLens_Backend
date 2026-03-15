# Flip Type Coverage (Item #1)

Current implementation map for the active target flip types:

| Flip Type | Ingestion | Calculation | Persistence | API |
|-----------|-----------|-------------|-------------|-----|
| Auction   | Hypixel market snapshot pipeline (`MarketDataProcessingService` + `UnifiedFlipInputMapper`) | `MarketFlipMapper` + `UnifiedFlipDtoMapper` + `FlipEconomicsService` | `FlipGenerationService` -> `UnifiedFlipStorageService.persistSnapshotFlips(...)` with optional legacy `FlipRepository.saveAll(...)` | `GET /api/v1/flips?flipType=AUCTION`, `GET /api/v1/ah/listings/{itemId}` |
| Bazaar    | Hypixel market snapshot pipeline (`MarketDataProcessingService` + `UnifiedFlipInputMapper`) | `MarketFlipMapper` + `UnifiedFlipDtoMapper` + `FlipEconomicsService` | `FlipGenerationService` -> `UnifiedFlipStorageService.persistSnapshotFlips(...)` with optional legacy `FlipRepository.saveAll(...)` | `GET /api/v1/flips?flipType=BAZAAR`, `GET /api/v1/bazaar/{itemId}` |
| Craft     | NEU sync (`SourceJobs.copyRepoDaily`) -> recipe model | `RecipeToFlipMapper` + `UnifiedFlipDtoMapper` + `FlipEconomicsService` | `FlipGenerationService` -> `UnifiedFlipStorageService.persistSnapshotFlips(...)` with optional legacy `FlipRepository.saveAll(...)` | `GET /api/v1/flips?flipType=CRAFTING`, `GET /api/v1/recipes` |
| Forge     | NEU sync (`SourceJobs.copyRepoDaily`) -> recipe model | `RecipeToFlipMapper` + `UnifiedFlipDtoMapper` + `FlipEconomicsService` | `FlipGenerationService` -> `UnifiedFlipStorageService.persistSnapshotFlips(...)` with optional legacy `FlipRepository.saveAll(...)` | `GET /api/v1/flips?flipType=FORGE`, `GET /api/v1/recipes` |

Out of scope in this phase:
- Shard
- Fusion

New endpoint added for this mapping:
- `GET /api/v1/flips/coverage`

Current runtime default:
- read paths serve from unified current storage (`flip_current`)
- legacy snapshot rows remain for parity checks, backfill, and rollback safety


