package com.skyblockflipper.backend.NEU;

import com.skyblockflipper.backend.NEU.model.Item;
import com.skyblockflipper.backend.model.Flipping.Recipe.Recipe;
import com.skyblockflipper.backend.model.Flipping.Recipe.RecipeIngredient;
import com.skyblockflipper.backend.model.Flipping.Recipe.RecipeProcessType;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class NEUItemMapperTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final NEUItemMapper mapper = new NEUItemMapper();

    @Test
    void mapCraftingGridRecipe() throws Exception {
        Item item = mapResource("test_jsons/ARMOR_OF_YOG_BOOTS.json");

        assertNotNull(item);
        assertEquals("ARMOR_OF_YOG_BOOTS", item.getId());
        assertEquals(1, item.getRecipes().size());

        Recipe recipe = item.getRecipes().getFirst();
        assertEquals(RecipeProcessType.CRAFT, recipe.getProcessType());

        Map<String, Integer> ingredientCounts = toCounts(recipe);
        assertEquals(2, ingredientCounts.size());
        assertEquals(40, ingredientCounts.get("YOGGIE"));
        assertEquals(1, ingredientCounts.get("FLAME_BREAKER_BOOTS"));
    }

    @Test
    void mapKatgradeRecipeIncludesInputAndItems() throws Exception {
        Item item = mapResource("test_jsons/ARMADILLO;5.json");

        assertNotNull(item);
        assertEquals("ARMADILLO;5", item.getId());
        assertEquals(1, item.getRecipes().size());

        Recipe recipe = item.getRecipes().getFirst();
        assertEquals(RecipeProcessType.CRAFT, recipe.getProcessType());
        assertEquals(3600, recipe.getProcessDurationSeconds());

        Map<String, Integer> ingredientCounts = toCounts(recipe);
        assertEquals(2, ingredientCounts.size());
        assertEquals(1, ingredientCounts.get("ARMADILLO;4"));
        assertEquals(1, ingredientCounts.get("FROZEN_SCUTE"));
    }

    @Test
    void mapItemWithNoRecipes() throws Exception {
        Item item = mapResource("test_jsons/ARMADILLO;0.json");

        assertNotNull(item);
        assertEquals("ARMADILLO;0", item.getId());
        assertTrue(item.getRecipes().isEmpty());
    }

    @Test
    void mapForgeRecipeFromInputs() {
        JsonNode node = readInline("""
                {
                  "internalname": "FORGE_ITEM",
                  "recipes": [
                    {
                      "type": "forge",
                      "duration": 120,
                      "inputs": [
                        {"item": "IRON_INGOT", "count": 2},
                        "COAL:1"
                      ]
                    }
                  ]
                }
                """);

        Item item = mapper.fromJson(node);
        assertNotNull(item);
        assertEquals("FORGE_ITEM", item.getId());
        assertEquals(1, item.getRecipes().size());

        Recipe recipe = item.getRecipes().getFirst();
        assertEquals(RecipeProcessType.FORGE, recipe.getProcessType());
        assertEquals(120, recipe.getProcessDurationSeconds());

        Map<String, Integer> ingredientCounts = toCounts(recipe);
        assertEquals(2, ingredientCounts.size());
        assertEquals(2, ingredientCounts.get("IRON_INGOT"));
        assertEquals(1, ingredientCounts.get("COAL"));
    }

    @Test
    void mapCraftingRecipeFromSlotsList() {
        JsonNode node = readInline("""
                {
                  "id": "SLOT_ITEM",
                  "recipes": [
                    {
                      "type": "crafting",
                      "slots": { "A1": "STONE:3" }
                    }
                  ]
                }
                """);

        Item item = mapper.fromJson(node);
        assertNotNull(item);
        assertEquals("SLOT_ITEM", item.getId());
        assertEquals(1, item.getRecipes().size());

        Recipe recipe = item.getRecipes().getFirst();
        assertEquals(RecipeProcessType.CRAFT, recipe.getProcessType());
        Map<String, Integer> ingredientCounts = toCounts(recipe);
        assertEquals(Map.of("STONE", 3), ingredientCounts);
    }

    @Test
    void mapNullOrMissingIdReturnsNull() {
        JsonNode node = readInline("{\"displayname\":\"No Id\"}");

        Item item = mapper.fromJson(node);
        assertNull(item);
    }

    @Test
    void mapLoreAsStringAndIgnoresNonArrayInfo() {
        JsonNode node = readInline("""
                {
                  "internalname": "LORE_ITEM",
                  "lore": "single line lore",
                  "info": "not-a-list"
                }
                """);

        Item item = mapper.fromJson(node);
        assertNotNull(item);
        assertEquals("LORE_ITEM", item.getId());
        assertEquals("single line lore", item.getLore());
        assertEquals(List.of(), item.getInfoLinks());
    }

    @Test
    void mapUnknownTypeWithSlotsFallsBackToCrafting() {
        JsonNode node = readInline("""
                {
                  "id": "FALLBACK_ITEM",
                  "recipes": [
                    {
                      "type": "weird",
                      "slots": {
                        "A1": "STONE:bad",
                        "A2": "null"
                      }
                    }
                  ]
                }
                """);

        Item item = mapper.fromJson(node);
        assertNotNull(item);
        assertEquals("FALLBACK_ITEM", item.getId());
        assertEquals(1, item.getRecipes().size());

        Recipe recipe = item.getRecipes().getFirst();
        assertEquals(RecipeProcessType.CRAFT, recipe.getProcessType());
        Map<String, Integer> ingredientCounts = toCounts(recipe);
        assertEquals(Map.of("STONE", 1), ingredientCounts);
    }

    @Test
    void mapNullNodeReturnsNull() {
        Item item = mapper.fromJson(null);
        assertNull(item);
    }

    private Item mapResource(String resourcePath) throws IOException {
        JsonNode node = readResource(resourcePath);
        return mapper.fromJson(node);
    }

    private JsonNode readResource(String resourcePath) throws IOException {
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
            if (input == null) {
                throw new IOException("Missing test resource: " + resourcePath);
            }
            return objectMapper.readTree(input);
        }
    }

    private JsonNode readInline(String json) {
        return objectMapper.readTree(json);
    }

    private Map<String, Integer> toCounts(Recipe recipe) {
        return recipe.getIngredients().stream()
                .collect(Collectors.toMap(RecipeIngredient::getItemId, RecipeIngredient::getAmount));
    }
}
