package com.skyblockflipper.backend.api;

import com.skyblockflipper.backend.model.Flipping.Recipe.RecipeProcessType;
import com.skyblockflipper.backend.service.flipping.RecipeCostService;
import com.skyblockflipper.backend.service.flipping.RecipeReadService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/recipes")
@RequiredArgsConstructor
public class RecipeController {

    private final RecipeReadService recipeReadService;
    private final RecipeCostService recipeCostService;

    @GetMapping
    public Page<RecipeDto> listRecipes(
            @RequestParam(required = false) String outputItemId,
            @RequestParam(required = false) RecipeProcessType processType,
            @PageableDefault(size = 100, sort = "recipeId") Pageable pageable
    ) {
        return recipeReadService.listRecipes(outputItemId, processType, pageable);
    }

    @GetMapping("/{recipeId}/cost")
    public ResponseEntity<RecipeCostBreakdownDto> recipeCost(@PathVariable String recipeId) {
        return recipeCostService.costBreakdown(recipeId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
