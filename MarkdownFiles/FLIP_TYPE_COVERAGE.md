# Flip Type Coverage (Item #1)

Current implementation map for the active target flip types:

| Flip Type | Ingestion | Calculation | Persistence | API |
|-----------|-----------|-------------|-------------|-----|
| Auction   | Hypixel market snapshot pipeline (`MarketDataProcessingService` + `UnifiedFlipInputMapper`) | `MarketFlipMapper` + `UnifiedFlipDtoMapper` + `FlipEconomicsService` | `FlipGenerationService` -> `FlipRepository.saveAll(...)` | `GET /api/v1/flips?flipType=AUCTION` |
| Bazaar    | Hypixel market snapshot pipeline (`MarketDataProcessingService` + `UnifiedFlipInputMapper`) | `MarketFlipMapper` + `UnifiedFlipDtoMapper` + `FlipEconomicsService` | `FlipGenerationService` -> `FlipRepository.saveAll(...)` | `GET /api/v1/flips?flipType=BAZAAR` |
| Craft     | NEU sync (`SourceJobs.copyRepoDaily`) -> recipe model | `RecipeToFlipMapper` + `UnifiedFlipDtoMapper` + `FlipEconomicsService` | `FlipGenerationService` -> `FlipRepository.saveAll(...)` | `GET /api/v1/flips?flipType=CRAFTING` |
| Forge     | NEU sync (`SourceJobs.copyRepoDaily`) -> recipe model | `RecipeToFlipMapper` + `UnifiedFlipDtoMapper` + `FlipEconomicsService` | `FlipGenerationService` -> `FlipRepository.saveAll(...)` | `GET /api/v1/flips?flipType=FORGE` |

Out of scope in this phase:
- Shard
- Fusion

New endpoint added for this mapping:
- `GET /api/v1/flips/coverage`
