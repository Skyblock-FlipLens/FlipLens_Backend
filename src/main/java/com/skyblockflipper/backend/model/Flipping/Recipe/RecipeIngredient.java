package com.skyblockflipper.backend.model.Flipping.Recipe;

import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.Objects;

@Getter
@Entity
@Table(name = "recipe_ingredients")
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class RecipeIngredient {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "item_internal_name", nullable = false)
    private String itemId;

    @Column(name = "amount", nullable = false)
    private int amount;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "recipe_id", nullable = false)
    private Recipe recipe;

    public RecipeIngredient(String itemId, int amount) {
        this.itemId = Objects.requireNonNull(itemId);
        this.amount = amount;
    }

    void setRecipe(Recipe recipe) {
        this.recipe = recipe;
    }
}
