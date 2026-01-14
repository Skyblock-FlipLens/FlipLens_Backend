package com.skyblockflipper.backend.model.Flipping.Recipe;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Recipe {

    private final String recipeId;
    private final String outputItemId;
    private final RecipeProcessType processType;
    private final long processDurationSeconds;
    private final List<RecipeIngredient> ingredients;
    private final List<RecipeRequirement> requirements;

    public Recipe(String recipeId, String outputItemId, RecipeProcessType processType, long processDurationSeconds,
                  List<RecipeIngredient> ingredients, List<RecipeRequirement> requirements) {
        this.recipeId = Objects.requireNonNull(recipeId, "recipeId");
        this.outputItemId = Objects.requireNonNull(outputItemId, "outputItemId");
        this.processType = Objects.requireNonNull(processType, "processType");
        this.processDurationSeconds = processDurationSeconds;
        this.ingredients = ingredients == null ? new ArrayList<>() : new ArrayList<>(ingredients);
        this.requirements = requirements == null ? new ArrayList<>() : new ArrayList<>(requirements);
    }

    public String getRecipeId() {
        return recipeId;
    }

    public String getOutputItemId() {
        return outputItemId;
    }

    public RecipeProcessType getProcessType() {
        return processType;
    }

    public long getProcessDurationSeconds() {
        return processDurationSeconds;
    }

    public List<RecipeIngredient> getIngredients() {
        return ingredients;
    }

    public List<RecipeRequirement> getRequirements() {
        return requirements;
    }
}
