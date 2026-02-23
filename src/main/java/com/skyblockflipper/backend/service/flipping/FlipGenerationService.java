package com.skyblockflipper.backend.service.flipping;

import com.skyblockflipper.backend.model.Flipping.Flip;
import com.skyblockflipper.backend.model.Flipping.Recipe.Recipe;
import com.skyblockflipper.backend.model.Flipping.Recipe.RecipeToFlipMapper;
import com.skyblockflipper.backend.model.market.UnifiedFlipInputSnapshot;
import com.skyblockflipper.backend.repository.FlipRepository;
import com.skyblockflipper.backend.repository.RecipeRepository;
import com.skyblockflipper.backend.service.flipping.storage.FlipStorageProperties;
import com.skyblockflipper.backend.service.flipping.storage.UnifiedFlipStorageService;
import com.skyblockflipper.backend.service.market.MarketSnapshotPersistenceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Service
public class FlipGenerationService {

    private final FlipRepository flipRepository;
    private final RecipeRepository recipeRepository;
    private final RecipeToFlipMapper recipeToFlipMapper;
    private final MarketSnapshotPersistenceService marketSnapshotPersistenceService;
    private final UnifiedFlipInputMapper unifiedFlipInputMapper;
    private final MarketFlipMapper marketFlipMapper;
    private final UnifiedFlipStorageService unifiedFlipStorageService;
    private final FlipStorageProperties flipStorageProperties;

    public FlipGenerationService(FlipRepository flipRepository,
                                 RecipeRepository recipeRepository,
                                 RecipeToFlipMapper recipeToFlipMapper) {
        this(flipRepository, recipeRepository, recipeToFlipMapper, null, null, null, null, null);
    }

    @Autowired
    public FlipGenerationService(FlipRepository flipRepository,
                                 RecipeRepository recipeRepository,
                                 RecipeToFlipMapper recipeToFlipMapper,
                                 MarketSnapshotPersistenceService marketSnapshotPersistenceService,
                                 UnifiedFlipInputMapper unifiedFlipInputMapper,
                                 MarketFlipMapper marketFlipMapper,
                                 UnifiedFlipStorageService unifiedFlipStorageService,
                                 FlipStorageProperties flipStorageProperties) {
        this.flipRepository = flipRepository;
        this.recipeRepository = recipeRepository;
        this.recipeToFlipMapper = recipeToFlipMapper;
        this.marketSnapshotPersistenceService = marketSnapshotPersistenceService;
        this.unifiedFlipInputMapper = unifiedFlipInputMapper;
        this.marketFlipMapper = marketFlipMapper;
        this.unifiedFlipStorageService = unifiedFlipStorageService;
        this.flipStorageProperties = flipStorageProperties;
    }

    @Transactional
    public GenerationResult generateIfMissingForSnapshot(Instant snapshotTimestamp) {
        if (snapshotTimestamp == null) {
            return new GenerationResult(0, 0, true);
        }
        long snapshotEpochMillis = snapshotTimestamp.toEpochMilli();
        if (existsSnapshotInActiveStorage(snapshotEpochMillis)) {
            return new GenerationResult(0, 0, true);
        }
        return regenerateForSnapshot(snapshotTimestamp);
    }

    @Transactional
    public GenerationResult regenerateForSnapshot(Instant snapshotTimestamp) {
        if (snapshotTimestamp == null) {
            return new GenerationResult(0, 0, true);
        }

        long snapshotEpochMillis = snapshotTimestamp.toEpochMilli();
        List<Recipe> recipes = recipeRepository.findAll(Sort.by("recipeId").ascending());
        Optional<UnifiedFlipInputSnapshot> marketInputSnapshot = loadMarketInputSnapshot(snapshotTimestamp);
        if (recipes.isEmpty() && marketInputSnapshot.isEmpty()) {
            return new GenerationResult(0, 0, true);
        }
        if (isLegacyWriteEnabled()) {
            flipRepository.deleteBySnapshotTimestampEpochMillis(snapshotEpochMillis);
        }

        List<Flip> generatedFlips = new ArrayList<>(recipes.size() + (marketInputSnapshot.isPresent() ? 128 : 0));
        int skipped = 0;
        for (Recipe recipe : recipes) {
            Flip mapped = recipeToFlipMapper.fromRecipe(recipe);
            if (mapped == null) {
                skipped++;
                continue;
            }
            generatedFlips.add(mapped);
        }
        marketInputSnapshot.ifPresent(snapshot -> generatedFlips.addAll(marketFlipMapper.fromMarketSnapshot(snapshot)));
        for (Flip flip : generatedFlips) {
            flip.setSnapshotTimestampEpochMillis(snapshotEpochMillis);
        }

        if (!generatedFlips.isEmpty() && isLegacyWriteEnabled()) {
            flipRepository.saveAll(generatedFlips);
        }
        if (!generatedFlips.isEmpty() && isDualWriteEnabled() && unifiedFlipStorageService != null) {
            unifiedFlipStorageService.persistSnapshotFlips(generatedFlips, snapshotTimestamp);
        }
        return new GenerationResult(generatedFlips.size(), skipped, false);
    }

    private boolean existsSnapshotInActiveStorage(long snapshotEpochMillis) {
        if (isDualWriteEnabled() && !isLegacyWriteEnabled() && unifiedFlipStorageService != null) {
            return unifiedFlipStorageService.existsForSnapshot(snapshotEpochMillis);
        }
        return flipRepository.existsBySnapshotTimestampEpochMillis(snapshotEpochMillis);
    }

    private boolean isDualWriteEnabled() {
        return flipStorageProperties == null || flipStorageProperties.isDualWriteEnabled();
    }

    private boolean isLegacyWriteEnabled() {
        return flipStorageProperties == null || flipStorageProperties.isLegacyWriteEnabled();
    }

    private Optional<UnifiedFlipInputSnapshot> loadMarketInputSnapshot(Instant snapshotTimestamp) {
        if (marketSnapshotPersistenceService == null || unifiedFlipInputMapper == null || marketFlipMapper == null) {
            return Optional.empty();
        }
        return marketSnapshotPersistenceService.asOf(snapshotTimestamp).map(unifiedFlipInputMapper::map);
    }

    public record GenerationResult(
            int generatedCount,
            int skippedCount,
            boolean noOp
    ) {
    }
}
