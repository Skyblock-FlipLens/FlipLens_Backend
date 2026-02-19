package com.skyblockflipper.backend.service.flipping;

import com.skyblockflipper.backend.model.Flipping.Flip;
import com.skyblockflipper.backend.model.Flipping.Recipe.Recipe;
import com.skyblockflipper.backend.model.Flipping.Recipe.RecipeToFlipMapper;
import com.skyblockflipper.backend.model.market.UnifiedFlipInputSnapshot;
import com.skyblockflipper.backend.repository.FlipRepository;
import com.skyblockflipper.backend.repository.RecipeRepository;
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

    public FlipGenerationService(FlipRepository flipRepository,
                                 RecipeRepository recipeRepository,
                                 RecipeToFlipMapper recipeToFlipMapper) {
        this(flipRepository, recipeRepository, recipeToFlipMapper, null, null, null);
    }

    @Autowired
    public FlipGenerationService(FlipRepository flipRepository,
                                 RecipeRepository recipeRepository,
                                 RecipeToFlipMapper recipeToFlipMapper,
                                 MarketSnapshotPersistenceService marketSnapshotPersistenceService,
                                 UnifiedFlipInputMapper unifiedFlipInputMapper,
                                 MarketFlipMapper marketFlipMapper) {
        this.flipRepository = flipRepository;
        this.recipeRepository = recipeRepository;
        this.recipeToFlipMapper = recipeToFlipMapper;
        this.marketSnapshotPersistenceService = marketSnapshotPersistenceService;
        this.unifiedFlipInputMapper = unifiedFlipInputMapper;
        this.marketFlipMapper = marketFlipMapper;
    }

    @Transactional
    public GenerationResult generateIfMissingForSnapshot(Instant snapshotTimestamp) {
        if (snapshotTimestamp == null) {
            return new GenerationResult(0, 0, true);
        }
        long snapshotEpochMillis = snapshotTimestamp.toEpochMilli();
        if (flipRepository.existsBySnapshotTimestampEpochMillis(snapshotEpochMillis)) {
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
        flipRepository.deleteBySnapshotTimestampEpochMillis(snapshotEpochMillis);

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

        if (!generatedFlips.isEmpty()) {
            flipRepository.saveAll(generatedFlips);
        }
        return new GenerationResult(generatedFlips.size(), skipped, false);
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
