package com.skyblockflipper.backend.api;

import java.util.List;

public record RecipeCostBreakdownDto(
        String recipeId,
        String outputItemId,
        List<IngredientCostDto> ingredients,
        long totalCraftCost,
        long outputSellPrice,
        long profit,
        double profitPct
) {
    public RecipeCostBreakdownDto {
        ingredients = ingredients == null ? List.of() : List.copyOf(ingredients);
    }

    public record IngredientCostDto(
            String itemId,
            int amount,
            long pricePerUnit,
            long totalCost
    ) {
    }
}
