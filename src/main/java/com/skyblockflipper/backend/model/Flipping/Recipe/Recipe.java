package com.skyblockflipper.backend.model.Flipping.Recipe;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public record Recipe(String recipeId, String outputItemId, RecipeProcessType processType, long processDurationSeconds,
                     List<RecipeIngredient> ingredients, List<RecipeRequirement> requirements) {

    public Recipe(String recipeId, String outputItemId, RecipeProcessType processType, long processDurationSeconds,
                  List<RecipeIngredient> ingredients, List<RecipeRequirement> requirements) {
        this.recipeId = Objects.requireNonNull(recipeId, "recipeId");
        this.outputItemId = Objects.requireNonNull(outputItemId, "outputItemId");
        this.processType = Objects.requireNonNull(processType, "processType");
        this.processDurationSeconds = processDurationSeconds;
        this.ingredients = ingredients == null ? new ArrayList<>() : new ArrayList<>(ingredients);
        this.requirements = requirements == null ? new ArrayList<>() : new ArrayList<>(requirements);
    }

}
