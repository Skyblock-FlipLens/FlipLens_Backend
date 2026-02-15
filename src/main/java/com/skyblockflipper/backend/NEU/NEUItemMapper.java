package com.skyblockflipper.backend.NEU;

import com.skyblockflipper.backend.NEU.model.Item;
import com.skyblockflipper.backend.model.Flipping.Recipe.Recipe;
import com.skyblockflipper.backend.model.Flipping.Recipe.RecipeIngredient;
import com.skyblockflipper.backend.model.Flipping.Recipe.RecipeProcessType;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Component
public class NEUItemMapper {

    private static final List<String> CRAFT_KEYS = List.of(
            "A1", "A2", "A3",
            "B1", "B2", "B3",
            "C1", "C2", "C3"
    );

    public Item fromJson(JsonNode node) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return null;
        }
        String id = firstNonBlank(node, "id", "internalname");
        if (id.isEmpty()) {
            return null;
        }

        Item item = Item.builder()
                .id(id)
                .displayName(firstNonBlank(node, "displayname", "display_name", "name"))
                .minecraftId(firstNonBlank(node, "minecraft_id", "minecraftid"))
                .rarity(firstNonBlank(node, "tier", "rarity"))
                .category(firstNonBlank(node, "category"))
                .lore(readLore(node))
                .infoLinks(readInfoLinks(node))
                .build();

        List<Recipe> recipes = mapRecipes(node, item);
        item.setRecipes(recipes);
        return item;
    }

    private List<Recipe> mapRecipes(JsonNode node, Item item) {
        List<Recipe> recipes = new ArrayList<>();
        int craftIndex = 0;
        int forgeIndex = 0;

        JsonNode directRecipe = node.path("recipe");
        List<RecipeIngredient> craftIngredients = ingredientsFromCraftingGrid(directRecipe);
        if (!craftIngredients.isEmpty()) {
            recipes.add(new Recipe(buildRecipeId(item.getId(), "craft", craftIndex++), item,
                    RecipeProcessType.CRAFT, 0, craftIngredients));
        }

        JsonNode recipeList = node.path("recipes");
        if (recipeList.isArray()) {
            for (JsonNode recipeNode : recipeList) {
                String type = getString(recipeNode.path("type")).toLowerCase(Locale.ROOT);
                boolean isForge = "forge".equals(type) || "forging".equals(type);
                boolean isKatgrade = "katgrade".equals(type);
                boolean isCraft = "crafting".equals(type) || "craft".equals(type) || isKatgrade;

                List<RecipeIngredient> ingredients = new ArrayList<>();
                if (isForge) {
                    ingredients = ingredientsFromForgeRecipe(recipeNode);
                } else if (isKatgrade) {
                    ingredients = ingredientsFromKatgrade(recipeNode);
                } else if (isCraft) {
                    ingredients = ingredientsFromCraftingGrid(recipeNode.path("slots"));
                    if (ingredients.isEmpty()) {
                        ingredients = ingredientsFromCraftingGrid(recipeNode);
                    }
                } else if (hasCraftingGrid(recipeNode.path("slots")) || hasCraftingGrid(recipeNode)) {
                    ingredients = ingredientsFromCraftingGrid(recipeNode.path("slots"));
                    if (ingredients.isEmpty()) {
                        ingredients = ingredientsFromCraftingGrid(recipeNode);
                    }
                }

                if (ingredients.isEmpty()) {
                    continue;
                }

                if (isForge) {
                    long duration = readDurationSeconds(recipeNode);
                    recipes.add(new Recipe(buildRecipeId(item.getId(), "forge", forgeIndex++), item,
                            RecipeProcessType.FORGE, duration, ingredients));
                } else {
                    long duration = isKatgrade ? readDurationSeconds(recipeNode) : 0;
                    recipes.add(new Recipe(buildRecipeId(item.getId(), "craft", craftIndex++), item,
                            RecipeProcessType.CRAFT, duration, ingredients));
                }
            }
        }

        return recipes;
    }

    private List<RecipeIngredient> ingredientsFromCraftingGrid(JsonNode grid) {
        if (grid == null || grid.isMissingNode() || grid.isNull() || !grid.isObject()) {
            return List.of();
        }
        Map<String, Integer> counts = new LinkedHashMap<>();
        for (String key : CRAFT_KEYS) {
            if (!grid.has(key)) {
                continue;
            }
            ParsedIngredient ingredient = parseIngredientString(getString(grid.path(key)));
            if (ingredient != null) {
                mergeIngredient(counts, ingredient.itemId, ingredient.amount);
            }
        }
        return toIngredients(counts);
    }

    private List<RecipeIngredient> ingredientsFromForgeRecipe(JsonNode recipeNode) {
        Map<String, Integer> counts = new LinkedHashMap<>();

        JsonNode inputs = recipeNode.path("inputs");
        if (inputs.isArray()) {
            for (JsonNode input : inputs) {
                ParsedIngredient ingredient = parseIngredientNode(input);
                if (ingredient != null) {
                    mergeIngredient(counts, ingredient.itemId, ingredient.amount);
                }
            }
        } else if (inputs.isObject()) {
            for (JsonNode input : inputs) {
                ParsedIngredient ingredient = parseIngredientNode(input);
                if (ingredient != null) {
                    mergeIngredient(counts, ingredient.itemId, ingredient.amount);
                }
            }
        }

        JsonNode slots = recipeNode.path("slots");
        if (counts.isEmpty() && slots.isObject()) {
            for (String key : CRAFT_KEYS) {
                if (!slots.has(key)) {
                    continue;
                }
                ParsedIngredient ingredient = parseIngredientString(getString(slots.path(key)));
                if (ingredient != null) {
                    mergeIngredient(counts, ingredient.itemId, ingredient.amount);
                }
            }
        }

        if (counts.isEmpty()) {
            JsonNode items = recipeNode.path("items");
            if (items.isArray()) {
                for (JsonNode input : items) {
                    ParsedIngredient ingredient = parseIngredientNode(input);
                    if (ingredient != null) {
                        mergeIngredient(counts, ingredient.itemId, ingredient.amount);
                    }
                }
            }
        }

        return toIngredients(counts);
    }

    private List<RecipeIngredient> ingredientsFromKatgrade(JsonNode recipeNode) {
        Map<String, Integer> counts = new LinkedHashMap<>();

        String input = getString(recipeNode.path("input"));
        if (!input.isEmpty()) {
            ParsedIngredient ingredient = parseIngredientString(input);
            if (ingredient != null) {
                mergeIngredient(counts, ingredient.itemId, ingredient.amount);
            }
        }

        JsonNode items = recipeNode.path("items");
        if (items.isArray()) {
            for (JsonNode item : items) {
                ParsedIngredient ingredient = parseIngredientNode(item);
                if (ingredient != null) {
                    mergeIngredient(counts, ingredient.itemId, ingredient.amount);
                }
            }
        }

        return toIngredients(counts);
    }

    private ParsedIngredient parseIngredientNode(JsonNode node) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return null;
        }
        if (node.isString()) {
            return parseIngredientString(node.asString());
        }

        String itemId = firstNonBlank(node, "item", "id", "internalname");
        if (itemId.isEmpty()) {
            return null;
        }
        int amount = readInt(node, "count", "amount", "qty");
        return new ParsedIngredient(itemId, amount);
    }

    private ParsedIngredient parseIngredientString(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        if (trimmed.isEmpty() || "null".equalsIgnoreCase(trimmed)) {
            return null;
        }
        String[] parts = trimmed.split(":");
        String itemId = parts[0].trim();
        if (itemId.isEmpty()) {
            return null;
        }
        int amount = 1;
        if (parts.length > 1) {
            try {
                amount = Integer.parseInt(parts[1].trim());
            } catch (NumberFormatException ignored) {
            }
        }
        return new ParsedIngredient(itemId, amount);
    }

    private long readDurationSeconds(JsonNode recipeNode) {
        long duration = readLong(recipeNode, "duration", "durationSeconds", "time", "timeSeconds");
        return duration < 0 ? 0 : duration;
    }

    private String readLore(JsonNode node) {
        JsonNode lore = node.path("lore");
        if (lore.isArray()) {
            StringBuilder sb = new StringBuilder();
            for (JsonNode line : lore) {
                String text = getString(line);
                if (!text.isEmpty()) {
                    if (!sb.isEmpty()) {
                        sb.append('\n');
                    }
                    sb.append(text);
                }
            }
            return sb.isEmpty() ? null : sb.toString();
        }
        String loreText = getString(lore);
        return loreText.isEmpty() ? null : loreText;
    }

    private List<String> readInfoLinks(JsonNode node) {
        JsonNode info = node.path("info");
        List<String> links = new ArrayList<>();
        if (!info.isArray()) {
            return links;
        }
        for (JsonNode entry : info) {
            String text = getString(entry);
            if (text.startsWith("http://") || text.startsWith("https://")) {
                links.add(text);
            }
        }
        return links;
    }

    private boolean hasCraftingGrid(JsonNode recipe) {
        if (recipe == null || recipe.isMissingNode() || recipe.isNull() || !recipe.isObject()) {
            return false;
        }
        for (String key : CRAFT_KEYS) {
            if (recipe.has(key)) {
                return true;
            }
        }
        return false;
    }

    private void mergeIngredient(Map<String, Integer> counts, String itemId, int amount) {
        if (itemId == null || itemId.isEmpty() || amount <= 0) {
            return;
        }
        counts.merge(itemId, amount, Integer::sum);
    }

    private List<RecipeIngredient> toIngredients(Map<String, Integer> counts) {
        if (counts.isEmpty()) {
            return List.of();
        }
        List<RecipeIngredient> ingredients = new ArrayList<>(counts.size());
        for (Map.Entry<String, Integer> entry : counts.entrySet()) {
            ingredients.add(new RecipeIngredient(entry.getKey(), entry.getValue()));
        }
        return ingredients;
    }

    private String buildRecipeId(String itemId, String type, int index) {
        return itemId + ":" + type + ":" + index;
    }

    private String firstNonBlank(JsonNode node, String... fieldNames) {
        for (String field : fieldNames) {
            String value = getString(node.path(field));
            if (!value.isEmpty()) {
                return value;
            }
        }
        return "";
    }

    private String getString(JsonNode node) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return "";
        }
        String value = node.asString();
        return value == null ? "" : value;
    }

    private int readInt(JsonNode node, String... fieldNames) {
        for (String field : fieldNames) {
            JsonNode value = node.path(field);
            if (!value.isMissingNode() && value.isNumber()) {
                return value.asInt();
            }
            if (value.isString()) {
                try {
                    return Integer.parseInt(value.asString().trim());
                } catch (NumberFormatException ignored) {
                    // fall through
                }
            }
        }
        return 1;
    }

    private long readLong(JsonNode node, String... fieldNames) {
        for (String field : fieldNames) {
            JsonNode value = node.path(field);
            if (!value.isMissingNode() && value.isNumber()) {
                return value.asLong();
            }
            if (value.isString()) {
                try {
                    return Long.parseLong(value.asString().trim());
                } catch (NumberFormatException ignored) {
                    // fall through
                }
            }
        }
        return -1;
    }

    private record ParsedIngredient(String itemId, int amount) {
    }
}
