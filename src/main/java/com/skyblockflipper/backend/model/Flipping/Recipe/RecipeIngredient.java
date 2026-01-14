package com.skyblockflipper.backend.model.Flipping.Recipe;

import java.util.Objects;

public class RecipeIngredient {

    private final String itemId;
    private final int amount;

    public RecipeIngredient(String itemId, int amount) {
        this.itemId = Objects.requireNonNull(itemId, "itemId");
        this.amount = amount;
    }

    public String getItemId() {
        return itemId;
    }

    public int getAmount() {
        return amount;
    }
}
