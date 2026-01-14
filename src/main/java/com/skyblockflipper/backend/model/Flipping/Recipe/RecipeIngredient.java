package com.skyblockflipper.backend.model.Flipping.Recipe;

import java.util.Objects;

public record RecipeIngredient(String itemId, int amount) {

    public RecipeIngredient(String itemId, int amount) {
        this.itemId = Objects.requireNonNull(itemId, "itemId");
        this.amount = amount;
    }

}
