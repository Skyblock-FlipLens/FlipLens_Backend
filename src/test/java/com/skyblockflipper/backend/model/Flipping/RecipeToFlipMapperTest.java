package com.skyblockflipper.backend.model.Flipping;

import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.model.Flipping.Enums.StepType;
import com.skyblockflipper.backend.model.Flipping.Recipe.Recipe;
import com.skyblockflipper.backend.model.Flipping.Recipe.RecipeIngredient;
import com.skyblockflipper.backend.model.Flipping.Recipe.RecipeProcessType;
import com.skyblockflipper.backend.model.Flipping.Recipe.RecipeRequirement;
import com.skyblockflipper.backend.model.Flipping.Recipe.RecipeToFlipMapper;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecipeToFlipMapperTest {

    @Test
    void mapsForgeRecipeToFlipWithConstraints() {
        Recipe recipe = new Recipe(
                "forge_recipe_1",
                "REFINED_DIAMOND",
                RecipeProcessType.FORGE,
                3600L,
                List.of(
                        new RecipeIngredient("DIAMOND", 160),
                        new RecipeIngredient("COAL", 1)
                ),
                List.of(
                        RecipeRequirement.minForgeSlots(2),
                        RecipeRequirement.recipeUnlocked("FORGE_REFINED_DIAMOND")
                )
        );

        RecipeToFlipMapper mapper = new RecipeToFlipMapper();
        Flip flip = mapper.fromRecipe(recipe);

        assertEquals(FlipType.FORGE, flip.getFlipType());
        assertEquals("REFINED_DIAMOND", flip.getResultItemId());
        assertEquals(3, flip.getSteps().size());
        assertEquals(StepType.FORGE, flip.getSteps().getLast().getType());
        assertTrue(flip.requiresForgeSlot());
        assertEquals(2, flip.getConstraints().size());
    }
}
