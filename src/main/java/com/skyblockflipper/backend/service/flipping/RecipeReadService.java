package com.skyblockflipper.backend.service.flipping;

import com.skyblockflipper.backend.api.RecipeDto;
import com.skyblockflipper.backend.model.Flipping.Recipe.Recipe;
import com.skyblockflipper.backend.model.Flipping.Recipe.RecipeIngredient;
import com.skyblockflipper.backend.model.Flipping.Recipe.RecipeProcessType;
import com.skyblockflipper.backend.repository.RecipeRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

@Service
public class RecipeReadService {

    private final RecipeRepository recipeRepository;

    public RecipeReadService(RecipeRepository recipeRepository) {
        this.recipeRepository = recipeRepository;
    }

    @Transactional(readOnly = true)
    public Page<RecipeDto> listRecipes(String outputItemId, RecipeProcessType processType, Pageable pageable) {
        String normalizedOutputItemId = normalize(outputItemId);
        Page<Recipe> recipes;
        if (normalizedOutputItemId.isEmpty() && processType == null) {
            recipes = recipeRepository.findAll(pageable);
        } else if (normalizedOutputItemId.isEmpty()) {
            recipes = recipeRepository.findAllByProcessType(processType, pageable);
        } else if (processType == null) {
            recipes = recipeRepository.findAllByOutputItem_Id(normalizedOutputItemId, pageable);
        } else {
            recipes = recipeRepository.findAllByOutputItem_IdAndProcessType(normalizedOutputItemId, processType, pageable);
        }
        return recipes.map(this::toDto);
    }

    private RecipeDto toDto(Recipe recipe) {
        List<RecipeDto.IngredientDto> ingredients = new ArrayList<>();
        for (RecipeIngredient ingredient : recipe.getIngredients()) {
            ingredients.add(new RecipeDto.IngredientDto(
                    ingredient.getItemId(),
                    ingredient.getAmount()
            ));
        }
        return new RecipeDto(
                recipe.getRecipeId(),
                recipe.getOutputItem().getId(),
                recipe.getProcessType(),
                recipe.getProcessDurationSeconds(),
                ingredients
        );
    }

    private String normalize(String value) {
        return value == null ? "" : value.trim().toUpperCase(Locale.ROOT);
    }
}
