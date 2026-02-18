package com.skyblockflipper.backend.service.flipping;

import com.skyblockflipper.backend.NEU.model.Item;
import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.model.Flipping.Flip;
import com.skyblockflipper.backend.model.Flipping.Recipe.Recipe;
import com.skyblockflipper.backend.model.Flipping.Recipe.RecipeIngredient;
import com.skyblockflipper.backend.model.Flipping.Recipe.RecipeProcessType;
import com.skyblockflipper.backend.model.Flipping.Recipe.RecipeToFlipMapper;
import com.skyblockflipper.backend.repository.FlipRepository;
import com.skyblockflipper.backend.repository.RecipeRepository;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.domain.Sort;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FlipGenerationServiceTest {

    @Test
    void generateIfMissingSkipsWhenSnapshotAlreadyGenerated() {
        FlipRepository flipRepository = mock(FlipRepository.class);
        RecipeRepository recipeRepository = mock(RecipeRepository.class);
        RecipeToFlipMapper mapper = mock(RecipeToFlipMapper.class);
        FlipGenerationService service = new FlipGenerationService(flipRepository, recipeRepository, mapper);
        Instant snapshot = Instant.parse("2026-02-18T21:30:00Z");

        when(flipRepository.existsBySnapshotTimestampEpochMillis(snapshot.toEpochMilli())).thenReturn(true);

        FlipGenerationService.GenerationResult result = service.generateIfMissingForSnapshot(snapshot);

        assertTrue(result.noOp());
        verify(flipRepository, never()).saveAll(any());
        verify(flipRepository, never()).deleteBySnapshotTimestampEpochMillis(anyLong());
    }

    @Test
    void generateIfMissingMapsRecipesAndPersistsFlipsForSnapshot() {
        FlipRepository flipRepository = mock(FlipRepository.class);
        RecipeRepository recipeRepository = mock(RecipeRepository.class);
        RecipeToFlipMapper mapper = mock(RecipeToFlipMapper.class);
        FlipGenerationService service = new FlipGenerationService(flipRepository, recipeRepository, mapper);
        Instant snapshot = Instant.parse("2026-02-18T21:30:00Z");

        Item outputItem = Item.builder().id("ENCHANTED_HAY_BALE").build();
        Recipe recipe = new Recipe(
                "ENCHANTED_HAY_BALE:craft:0",
                outputItem,
                RecipeProcessType.CRAFT,
                0L,
                List.of(new RecipeIngredient("HAY_BLOCK", 144))
        );
        Flip mappedFlip = new Flip(null, FlipType.CRAFTING, List.of(), "ENCHANTED_HAY_BALE", List.of());

        when(flipRepository.existsBySnapshotTimestampEpochMillis(snapshot.toEpochMilli())).thenReturn(false);
        when(recipeRepository.findAll(any(Sort.class))).thenReturn(List.of(recipe));
        when(mapper.fromRecipe(recipe)).thenReturn(mappedFlip);

        FlipGenerationService.GenerationResult result = service.generateIfMissingForSnapshot(snapshot);

        assertEquals(1, result.generatedCount());
        assertEquals(0, result.skippedCount());
        assertEquals(snapshot.toEpochMilli(), mappedFlip.getSnapshotTimestampEpochMillis());

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<Flip>> savedFlipsCaptor = ArgumentCaptor.forClass(List.class);
        verify(flipRepository).deleteBySnapshotTimestampEpochMillis(snapshot.toEpochMilli());
        verify(flipRepository).saveAll(savedFlipsCaptor.capture());
        assertEquals(1, savedFlipsCaptor.getValue().size());
        assertEquals(snapshot.toEpochMilli(), savedFlipsCaptor.getValue().getFirst().getSnapshotTimestampEpochMillis());
    }
}
