package com.skyblockflipper.backend.model.Flipping.Recipe;

import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.model.Flipping.Constraint;
import com.skyblockflipper.backend.model.Flipping.Flip;
import com.skyblockflipper.backend.model.Flipping.Step;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class RecipeToFlipMapper {

    private static final long DEFAULT_BUY_SECONDS = 30L;

    public Flip fromRecipe(Recipe recipe) {
        List<Step> steps = new ArrayList<>();
        for (RecipeIngredient ingredient : recipe.getIngredients()) {
            steps.add(Step.forBuyMarketBased(DEFAULT_BUY_SECONDS, buildParamsJson(ingredient)));
        }

        steps.add(buildProcessStep(recipe));

        return new Flip(
                UUID.randomUUID(),
                mapFlipType(recipe.getProcessType()),
                steps,
                recipe.getOutputItem().getId(),
                buildConstraints(recipe)
        );
    }

    private Step buildProcessStep(Recipe recipe) {
        long durationSeconds = Math.max(0L, recipe.getProcessDurationSeconds());
        return switch (recipe.getProcessType()) {
            case FORGE -> Step.forForgeFixed(durationSeconds);
            case CRAFT -> Step.forCraftInstant(durationSeconds);
            case KATGRADE -> Step.forWaitFixed(durationSeconds);
        };
    }

    private FlipType mapFlipType(RecipeProcessType processType) {
        return switch (processType) {
            case FORGE -> FlipType.FORGE;
            case KATGRADE -> FlipType.KATGRADE;
            case CRAFT -> FlipType.CRAFTING;
        };
    }

    private List<Constraint> buildConstraints(Recipe recipe) {
        List<Constraint> constraints = new ArrayList<>();
        constraints.add(Constraint.recipeUnlocked(recipe.getRecipeId()));
        if (recipe.getProcessType() == RecipeProcessType.FORGE) {
            constraints.add(Constraint.minForgeSlots(1));
        }
        return constraints;
    }

    private String buildParamsJson(RecipeIngredient ingredient) {
        return "{\"itemId\":\"" + ingredient.getItemId() + "\",\"amount\":" + ingredient.getAmount() + "}";
    }
}
