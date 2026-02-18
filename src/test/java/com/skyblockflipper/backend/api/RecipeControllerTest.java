package com.skyblockflipper.backend.api;

import com.skyblockflipper.backend.model.Flipping.Recipe.RecipeProcessType;
import com.skyblockflipper.backend.service.flipping.RecipeReadService;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RecipeControllerTest {

    @Test
    void listRecipesDelegatesToService() {
        RecipeReadService recipeReadService = mock(RecipeReadService.class);
        RecipeController controller = new RecipeController(recipeReadService);
        Pageable pageable = PageRequest.of(0, 100);
        RecipeDto dto = new RecipeDto(
                "ENCHANTED_HAY_BALE:craft:0",
                "ENCHANTED_HAY_BALE",
                RecipeProcessType.CRAFT,
                0L,
                List.of(new RecipeDto.IngredientDto("HAY_BLOCK", 144))
        );
        Page<RecipeDto> expected = new PageImpl<>(List.of(dto), pageable, 1);

        when(recipeReadService.listRecipes("ENCHANTED_HAY_BALE", RecipeProcessType.CRAFT, pageable)).thenReturn(expected);

        Page<RecipeDto> response = controller.listRecipes("ENCHANTED_HAY_BALE", RecipeProcessType.CRAFT, pageable);

        assertEquals(expected, response);
        verify(recipeReadService).listRecipes("ENCHANTED_HAY_BALE", RecipeProcessType.CRAFT, pageable);
    }
}
