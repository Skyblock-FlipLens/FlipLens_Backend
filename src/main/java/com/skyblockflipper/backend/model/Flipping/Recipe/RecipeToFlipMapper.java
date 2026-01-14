package com.skyblockflipper.backend.model.Flipping.Recipe;

import com.skyblockflipper.backend.model.Flipping.Constraint;
import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.model.Flipping.Flip;
import com.skyblockflipper.backend.model.Flipping.Step;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class RecipeToFlipMapper {

    private static final long DEFAULT_BUY_SECONDS = 30L;

    public Flip fromRecipe(Recipe recipe) {
        List<Step> steps = new ArrayList<>();
        for (RecipeIngredient ingredient : recipe.ingredients()) {
            steps.add(Step.forBuyMarketBased(DEFAULT_BUY_SECONDS, buildParamsJson(ingredient)));
        }

        steps.add(buildProcessStep(recipe));

        List<Constraint> constraints = new ArrayList<>();
        for (RecipeRequirement requirement : recipe.requirements()) {
            Constraint constraint = mapConstraint(requirement);
            if (constraint != null) {
                constraints.add(constraint);
            }
        }

        return new Flip(UUID.randomUUID(), mapFlipType(recipe.processType()), steps, recipe.outputItemId(), constraints);
    }

    private Step buildProcessStep(Recipe recipe) {
        long durationSeconds = Math.max(0L, recipe.processDurationSeconds());
        return switch (recipe.processType()) {
            case FORGE -> Step.forForgeFixed(durationSeconds);
            case CRAFT -> Step.forCraftInstant(durationSeconds);
        };
    }

    private FlipType mapFlipType(RecipeProcessType processType) {
        return processType == RecipeProcessType.FORGE ? FlipType.FORGE : FlipType.CRAFTING;
    }

    private Constraint mapConstraint(RecipeRequirement requirement) {
        return switch (requirement.getType()) {
            case MIN_FORGE_SLOTS -> Constraint.minForgeSlots(requirement.getIntValue() == null ? 0 : requirement.getIntValue());
            case RECIPE_UNLOCKED -> Constraint.recipeUnlocked(requirement.getStringValue());
            case MIN_CAPITAL -> Constraint.minCapital(requirement.getLongValue() == null ? 0L : requirement.getLongValue());
        };
    }

    private String buildParamsJson(RecipeIngredient ingredient) {
        return "{\"itemId\":\"" + ingredient.itemId() + "\",\"amount\":" + ingredient.amount() + "}";
    }
}
