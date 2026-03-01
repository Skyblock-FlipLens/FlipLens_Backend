package com.skyblockflipper.backend.service.flipping;

import com.skyblockflipper.backend.NEU.model.Item;
import com.skyblockflipper.backend.api.dto.RecipeDto;
import com.skyblockflipper.backend.model.Flipping.Recipe.Recipe;
import com.skyblockflipper.backend.model.Flipping.Recipe.RecipeIngredient;
import com.skyblockflipper.backend.model.Flipping.Recipe.RecipeProcessType;
import com.skyblockflipper.backend.repository.RecipeRepository;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RecipeReadServiceTest {

    @Test
    void listRecipesUsesFindAllWhenNoFiltersProvided() {
        RecipeRepository repository = mock(RecipeRepository.class);
        RecipeReadService service = new RecipeReadService(repository);
        PageRequest pageable = PageRequest.of(0, 10);
        Recipe recipe = recipe("r1", "GOLDEN_PLATE", RecipeProcessType.CRAFT);
        when(repository.findAll(pageable)).thenReturn(new PageImpl<>(List.of(recipe), pageable, 1));

        Page<RecipeDto> result = service.listRecipes(null, null, pageable);

        assertEquals(1, result.getTotalElements());
        assertEquals("r1", result.getContent().getFirst().recipeId());
        verify(repository).findAll(pageable);
    }

    @Test
    void listRecipesUsesProcessTypeOnlyWhenItemMissing() {
        RecipeRepository repository = mock(RecipeRepository.class);
        RecipeReadService service = new RecipeReadService(repository);
        PageRequest pageable = PageRequest.of(0, 10);
        Recipe recipe = recipe("r2", "REFINED_DIAMOND", RecipeProcessType.FORGE);
        when(repository.findAllByProcessType(RecipeProcessType.FORGE, pageable))
                .thenReturn(new PageImpl<>(List.of(recipe), pageable, 1));

        Page<RecipeDto> result = service.listRecipes(" ", RecipeProcessType.FORGE, pageable);

        assertEquals("REFINED_DIAMOND", result.getContent().getFirst().outputItemId());
        verify(repository).findAllByProcessType(RecipeProcessType.FORGE, pageable);
    }

    @Test
    void listRecipesUsesOutputItemOnlyWhenProcessMissing() {
        RecipeRepository repository = mock(RecipeRepository.class);
        RecipeReadService service = new RecipeReadService(repository);
        PageRequest pageable = PageRequest.of(0, 10);
        Recipe recipe = recipe("r3", "HYPERION", RecipeProcessType.CRAFT);
        when(repository.findAllByOutputItem_Id("HYPERION", pageable))
                .thenReturn(new PageImpl<>(List.of(recipe), pageable, 1));

        Page<RecipeDto> result = service.listRecipes("hyperion", null, pageable);

        assertEquals("HYPERION", result.getContent().getFirst().outputItemId());
        verify(repository).findAllByOutputItem_Id("HYPERION", pageable);
    }

    @Test
    void listRecipesUsesCombinedFiltersAndMapsIngredients() {
        RecipeRepository repository = mock(RecipeRepository.class);
        RecipeReadService service = new RecipeReadService(repository);
        PageRequest pageable = PageRequest.of(0, 10);
        Recipe recipe = recipe("r4", "GOLDEN_PLATE", RecipeProcessType.CRAFT);
        when(repository.findAllByOutputItem_IdAndProcessType("GOLDEN_PLATE", RecipeProcessType.CRAFT, pageable))
                .thenReturn(new PageImpl<>(List.of(recipe), pageable, 1));

        Page<RecipeDto> result = service.listRecipes("golden_plate", RecipeProcessType.CRAFT, pageable);

        RecipeDto dto = result.getContent().getFirst();
        assertEquals("r4", dto.recipeId());
        assertEquals(2, dto.ingredients().size());
        assertEquals("ENCHANTED_GOLD_BLOCK", dto.ingredients().getFirst().itemId());
        verify(repository).findAllByOutputItem_IdAndProcessType("GOLDEN_PLATE", RecipeProcessType.CRAFT, pageable);
    }

    private Recipe recipe(String id, String outputItemId, RecipeProcessType type) {
        return new Recipe(
                id,
                Item.builder().id(outputItemId).build(),
                type,
                0L,
                List.of(
                        new RecipeIngredient("ENCHANTED_GOLD_BLOCK", 32),
                        new RecipeIngredient("ENCHANTED_REDSTONE_BLOCK", 8)
                )
        );
    }
}
