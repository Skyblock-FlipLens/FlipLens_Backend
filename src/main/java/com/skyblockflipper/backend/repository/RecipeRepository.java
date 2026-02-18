package com.skyblockflipper.backend.repository;

import com.skyblockflipper.backend.model.Flipping.Recipe.Recipe;
import com.skyblockflipper.backend.model.Flipping.Recipe.RecipeProcessType;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

public interface RecipeRepository extends JpaRepository<Recipe, String> {
    Page<Recipe> findAllByProcessType(RecipeProcessType processType, Pageable pageable);

    Page<Recipe> findAllByOutputItem_Id(String outputItemId, Pageable pageable);

    Page<Recipe> findAllByOutputItem_IdAndProcessType(String outputItemId,
                                                      RecipeProcessType processType,
                                                      Pageable pageable);
}
