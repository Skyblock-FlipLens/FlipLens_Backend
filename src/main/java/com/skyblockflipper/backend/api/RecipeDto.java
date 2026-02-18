package com.skyblockflipper.backend.api;

import com.skyblockflipper.backend.model.Flipping.Recipe.RecipeProcessType;

import java.util.List;

public record RecipeDto(
        String recipeId,
        String outputItemId,
        RecipeProcessType processType,
        long processDurationSeconds,
        List<IngredientDto> ingredients
) {
    public RecipeDto {
        ingredients = ingredients == null ? List.of() : List.copyOf(ingredients);
    }

    public record IngredientDto(
            String itemId,
            int amount
    ) {
    }
}
