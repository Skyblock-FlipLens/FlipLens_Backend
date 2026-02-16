package com.skyblockflipper.backend.model.Flipping.Recipe;

import com.skyblockflipper.backend.NEU.model.Item;
import com.skyblockflipper.backend.model.Flipping.Enums.ConstraintType;
import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.model.Flipping.Enums.StepType;
import com.skyblockflipper.backend.model.Flipping.Flip;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecipeToFlipMapperTest {

    private final RecipeToFlipMapper mapper = new RecipeToFlipMapper();

    @Test
    void mapsForgeRecipeWithRequirements() {
        Recipe recipe = new Recipe(
                "r1",
                item("out"),
                RecipeProcessType.FORGE,
                -5L,
                List.of(new RecipeIngredient("item", 2))
        );

        Flip flip = mapper.fromRecipe(recipe);

        assertEquals(FlipType.FORGE, flip.getFlipType());
        assertEquals("out", flip.getResultItemId());
        assertEquals(2, flip.getConstraints().size());
        assertEquals(ConstraintType.RECIPE_UNLOCKED, flip.getConstraints().get(0).getType());
        assertEquals("r1", flip.getConstraints().get(0).getStringValue());
        assertEquals(ConstraintType.MIN_FORGE_SLOTS, flip.getConstraints().get(1).getType());
        assertEquals(StepType.BUY, flip.getSteps().get(0).getType());
        assertEquals(StepType.FORGE, flip.getSteps().get(1).getType());
        assertTrue(flip.getSteps().get(1).getBaseDurationSeconds() >= 0);
    }

    private Item item(String id) {
        return Item.builder().id(id).displayName("name").build();
    }
}
