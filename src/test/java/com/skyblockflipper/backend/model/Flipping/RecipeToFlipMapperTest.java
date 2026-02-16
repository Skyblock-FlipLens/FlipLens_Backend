package com.skyblockflipper.backend.model.Flipping;

import com.skyblockflipper.backend.NEU.model.Item;
import com.skyblockflipper.backend.model.Flipping.Enums.ConstraintType;
import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.model.Flipping.Enums.StepType;
import com.skyblockflipper.backend.model.Flipping.Recipe.*;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecipeToFlipMapperTest {

    @Test
    void mapsForgeRecipeToFlipWithConstraints() {
        Recipe recipe = new Recipe(
                "forge_recipe_1",
                item("REFINED_DIAMOND"),
                RecipeProcessType.FORGE,
                3600L,
                List.of(
                        new RecipeIngredient("DIAMOND", 160),
                        new RecipeIngredient("COAL", 1)
                )
        );

        RecipeToFlipMapper mapper = new RecipeToFlipMapper();
        Flip flip = mapper.fromRecipe(recipe);

        assertEquals(FlipType.FORGE, flip.getFlipType());
        assertEquals("REFINED_DIAMOND", flip.getResultItemId());
        assertEquals(3, flip.getSteps().size());
        assertEquals(StepType.FORGE, flip.getSteps().getLast().getType());
        assertEquals(2, flip.getConstraints().size());
        assertEquals(ConstraintType.RECIPE_UNLOCKED, flip.getConstraints().get(0).getType());
        assertEquals("forge_recipe_1", flip.getConstraints().get(0).getStringValue());
        assertEquals(ConstraintType.MIN_FORGE_SLOTS, flip.getConstraints().get(1).getType());
        assertEquals(1, flip.getConstraints().get(1).getIntValue());
    }

    private Item item(String id) {
        return Item.builder().id(id).displayName("name").build();
    }

    @Test
    void mapsKatgradeRecipeToKatgradeFlipWithWaitStep() {
        Recipe recipe = new Recipe(
                "kat_recipe_1",
                item("ARMADILLO;5"),
                RecipeProcessType.KATGRADE,
                3600L,
                List.of(
                        new RecipeIngredient("ARMADILLO;4", 1),
                        new RecipeIngredient("FROZEN_SCUTE", 1)
                )
        );

        RecipeToFlipMapper mapper = new RecipeToFlipMapper();
        Flip flip = mapper.fromRecipe(recipe);

        assertEquals(FlipType.KATGRADE, flip.getFlipType());
        assertEquals("ARMADILLO;5", flip.getResultItemId());
        assertEquals(3, flip.getSteps().size());
        assertEquals(StepType.WAIT, flip.getSteps().getLast().getType());
        assertEquals(1, flip.getConstraints().size());
        assertEquals(ConstraintType.RECIPE_UNLOCKED, flip.getConstraints().getFirst().getType());
        assertEquals("kat_recipe_1", flip.getConstraints().getFirst().getStringValue());
    }
}
