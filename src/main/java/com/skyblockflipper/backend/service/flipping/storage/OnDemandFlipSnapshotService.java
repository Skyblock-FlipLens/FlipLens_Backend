package com.skyblockflipper.backend.service.flipping.storage;

import com.skyblockflipper.backend.api.UnifiedFlipDto;
import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.model.Flipping.Flip;
import com.skyblockflipper.backend.model.Flipping.Recipe.Recipe;
import com.skyblockflipper.backend.model.Flipping.Recipe.RecipeToFlipMapper;
import com.skyblockflipper.backend.model.market.UnifiedFlipInputSnapshot;
import com.skyblockflipper.backend.repository.RecipeRepository;
import com.skyblockflipper.backend.service.flipping.FlipCalculationContext;
import com.skyblockflipper.backend.service.flipping.FlipCalculationContextService;
import com.skyblockflipper.backend.service.flipping.MarketFlipMapper;
import com.skyblockflipper.backend.service.flipping.UnifiedFlipDtoMapper;
import com.skyblockflipper.backend.service.flipping.UnifiedFlipInputMapper;
import com.skyblockflipper.backend.service.market.MarketSnapshotPersistenceService;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Service
public class OnDemandFlipSnapshotService {

    private final RecipeRepository recipeRepository;
    private final RecipeToFlipMapper recipeToFlipMapper;
    private final MarketSnapshotPersistenceService marketSnapshotPersistenceService;
    private final UnifiedFlipInputMapper unifiedFlipInputMapper;
    private final MarketFlipMapper marketFlipMapper;
    private final UnifiedFlipDtoMapper unifiedFlipDtoMapper;
    private final FlipCalculationContextService flipCalculationContextService;
    private final FlipIdentityService flipIdentityService;

    public OnDemandFlipSnapshotService(RecipeRepository recipeRepository,
                                       RecipeToFlipMapper recipeToFlipMapper,
                                       MarketSnapshotPersistenceService marketSnapshotPersistenceService,
                                       UnifiedFlipInputMapper unifiedFlipInputMapper,
                                       MarketFlipMapper marketFlipMapper,
                                       UnifiedFlipDtoMapper unifiedFlipDtoMapper,
                                       FlipCalculationContextService flipCalculationContextService,
                                       FlipIdentityService flipIdentityService) {
        this.recipeRepository = recipeRepository;
        this.recipeToFlipMapper = recipeToFlipMapper;
        this.marketSnapshotPersistenceService = marketSnapshotPersistenceService;
        this.unifiedFlipInputMapper = unifiedFlipInputMapper;
        this.marketFlipMapper = marketFlipMapper;
        this.unifiedFlipDtoMapper = unifiedFlipDtoMapper;
        this.flipCalculationContextService = flipCalculationContextService;
        this.flipIdentityService = flipIdentityService;
    }

    @Transactional(readOnly = true)
    public List<UnifiedFlipDto> computeSnapshotDtos(Instant snapshotTimestamp, FlipType flipType) {
        if (snapshotTimestamp == null) {
            return List.of();
        }

        long snapshotEpochMillis = snapshotTimestamp.toEpochMilli();
        FlipCalculationContext context = flipCalculationContextService.loadContextAsOf(snapshotTimestamp);

        List<Flip> generated = new ArrayList<>();
        List<Recipe> recipes = recipeRepository.findAll(Sort.by("recipeId").ascending());
        for (Recipe recipe : recipes) {
            Flip mapped = recipeToFlipMapper.fromRecipe(recipe);
            if (mapped == null) {
                continue;
            }
            mapped.setSnapshotTimestampEpochMillis(snapshotEpochMillis);
            generated.add(mapped);
        }

        Optional<UnifiedFlipInputSnapshot> marketInput = marketSnapshotPersistenceService
                .asOf(snapshotTimestamp)
                .map(unifiedFlipInputMapper::map);
        marketInput.ifPresent(inputSnapshot -> {
            List<Flip> marketFlips = marketFlipMapper.fromMarketSnapshot(inputSnapshot);
            for (Flip marketFlip : marketFlips) {
                marketFlip.setSnapshotTimestampEpochMillis(snapshotEpochMillis);
            }
            generated.addAll(marketFlips);
        });

        return generated.stream()
                .filter(flip -> flipType == null || flip.getFlipType() == flipType)
                .map(flip -> {
                    UnifiedFlipDto dto = unifiedFlipDtoMapper.toDto(flip, context);
                    if (dto == null) {
                        return null;
                    }
                    return UnifiedFlipDtoIdMapper.withId(dto, flipIdentityService.derive(flip).stableFlipId());
                })
                .filter(Objects::nonNull)
                .toList();
    }
}
