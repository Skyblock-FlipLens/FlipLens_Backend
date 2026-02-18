package com.skyblockflipper.backend.service.flipping;

import com.skyblockflipper.backend.model.Flipping.Flip;
import com.skyblockflipper.backend.model.Flipping.Recipe.Recipe;
import com.skyblockflipper.backend.model.Flipping.Recipe.RecipeToFlipMapper;
import com.skyblockflipper.backend.repository.FlipRepository;
import com.skyblockflipper.backend.repository.RecipeRepository;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Service
public class FlipGenerationService {

    private final FlipRepository flipRepository;
    private final RecipeRepository recipeRepository;
    private final RecipeToFlipMapper recipeToFlipMapper;

    public FlipGenerationService(FlipRepository flipRepository,
                                 RecipeRepository recipeRepository,
                                 RecipeToFlipMapper recipeToFlipMapper) {
        this.flipRepository = flipRepository;
        this.recipeRepository = recipeRepository;
        this.recipeToFlipMapper = recipeToFlipMapper;
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
        if (recipes.isEmpty()) {
            return new GenerationResult(0, 0, true);
        }
        flipRepository.deleteBySnapshotTimestampEpochMillis(snapshotEpochMillis);

        List<Flip> generatedFlips = new ArrayList<>(recipes.size());
        int skipped = 0;
        for (Recipe recipe : recipes) {
            Flip mapped = recipeToFlipMapper.fromRecipe(recipe);
            if (mapped == null) {
                skipped++;
                continue;
            }
            mapped.setSnapshotTimestampEpochMillis(snapshotEpochMillis);
            generatedFlips.add(mapped);
        }

        if (!generatedFlips.isEmpty()) {
            flipRepository.saveAll(generatedFlips);
        }
        return new GenerationResult(generatedFlips.size(), skipped, false);
    }

    public record GenerationResult(
            int generatedCount,
            int skippedCount,
            boolean noOp
    ) {
    }
}
