package com.skyblockflipper.backend.model.Flipping.Recipe;

import lombok.Getter;

import java.util.Objects;

@Getter
public class RecipeRequirement {

    private final RecipeRequirementType type;
    private final String stringValue;
    private final Integer intValue;
    private final Long longValue;

    private RecipeRequirement(RecipeRequirementType type, String stringValue, Integer intValue, Long longValue) {
        this.type = Objects.requireNonNull(type, "type");
        this.stringValue = stringValue;
        this.intValue = intValue;
        this.longValue = longValue;
    }

    public static RecipeRequirement minForgeSlots(int slots) {
        return new RecipeRequirement(RecipeRequirementType.MIN_FORGE_SLOTS, null, slots, null);
    }

    public static RecipeRequirement recipeUnlocked(String recipeId) {
        return new RecipeRequirement(RecipeRequirementType.RECIPE_UNLOCKED, recipeId, null, null);
    }

    public static RecipeRequirement minCapital(long coins) {
        return new RecipeRequirement(RecipeRequirementType.MIN_CAPITAL, null, null, coins);
    }

}
