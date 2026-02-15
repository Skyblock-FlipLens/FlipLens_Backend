package com.skyblockflipper.backend.model.Flipping.Recipe;

import com.skyblockflipper.backend.NEU.model.Item;
import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Getter
@Entity
@Table(name = "recipes")
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Recipe {

    @Id
    @Column(name = "recipe_id", nullable = false, updatable = false)
    private String recipeId;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "output_item_id", nullable = false)
    private Item outputItem;

    @Enumerated(EnumType.STRING)
    @Column(name = "process_type", nullable = false)
    private RecipeProcessType processType;

    @Column(name = "process_duration_seconds", nullable = false)
    private long processDurationSeconds;

    @OneToMany(mappedBy = "recipe", cascade = CascadeType.ALL, orphanRemoval = true)
    private final List<RecipeIngredient> ingredients = new ArrayList<>();

    public Recipe(String recipeId, Item outputItem, RecipeProcessType processType, long processDurationSeconds,
                  List<RecipeIngredient> ingredients) {
        this.recipeId = Objects.requireNonNull(recipeId, "recipeId");
        updateFrom(outputItem, processType, processDurationSeconds, ingredients);
    }

    public void updateFrom(Item outputItem, RecipeProcessType processType, long processDurationSeconds,
                           List<RecipeIngredient> ingredients) {
        this.outputItem = Objects.requireNonNull(outputItem, "outputItem");
        this.processType = Objects.requireNonNull(processType, "processType");
        this.processDurationSeconds = processDurationSeconds;
        setIngredients(ingredients);
    }

    public void setIngredients(List<RecipeIngredient> newIngredients) {
        clearIngredients();
        if (newIngredients != null) {
            newIngredients.forEach(this::addIngredient);
        }
    }

    public void addIngredient(RecipeIngredient ingredient) {
        Objects.requireNonNull(ingredient, "ingredient");
        ingredient.setRecipe(this);
        this.ingredients.add(ingredient);
    }

    public void removeIngredient(RecipeIngredient ingredient) {
        if (ingredient != null && this.ingredients.remove(ingredient)) {
            ingredient.setRecipe(null);
        }
    }

    private void clearIngredients() {
        this.ingredients.forEach(ingredient -> ingredient.setRecipe(null));
        this.ingredients.clear();
    }
}
