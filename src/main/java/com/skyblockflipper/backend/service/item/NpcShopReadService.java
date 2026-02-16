package com.skyblockflipper.backend.service.item;

import com.skyblockflipper.backend.NEU.NEUClient;
import com.skyblockflipper.backend.api.NpcShopOfferDto;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import tools.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

@Service
public class NpcShopReadService {

    private static final String NPC_SHOP = "NPC_SHOP";
    private static final String SKYBLOCK_COIN = "SKYBLOCK_COIN";

    private final NEUClient neuClient;

    public NpcShopReadService(NEUClient neuClient) {
        this.neuClient = neuClient;
    }

    public Page<NpcShopOfferDto> listNpcBuyableOffers(String itemId, Pageable pageable) {
        List<NpcShopOfferDto> offers;
        try {
            offers = extractNpcShopOffers(neuClient.loadAllItemJsons());
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new IllegalStateException("Failed to load NPC shop offers from NEU data.", e);
        }

        String normalizedItemId = normalize(itemId);
        if (!normalizedItemId.isEmpty()) {
            offers = offers.stream()
                    .filter(offer -> normalize(offer.itemId()).equals(normalizedItemId))
                    .toList();
        }

        offers = offers.stream()
                .sorted(Comparator.comparing(NpcShopOfferDto::itemId)
                        .thenComparing(NpcShopOfferDto::npcId))
                .toList();

        int start = (int) Math.min(pageable.getOffset(), offers.size());
        int end = Math.min(start + pageable.getPageSize(), offers.size());
        return new PageImpl<>(offers.subList(start, end), pageable, offers.size());
    }

    private List<NpcShopOfferDto> extractNpcShopOffers(List<JsonNode> nodes) {
        List<NpcShopOfferDto> offers = new ArrayList<>();
        for (JsonNode node : nodes) {
            if (node == null || !node.isObject()) {
                continue;
            }

            String npcId = firstNonBlank(node, "id", "internalname");
            String npcDisplayName = firstNonBlank(node, "displayname", "display_name", "name");

            JsonNode recipes = node.path("recipes");
            if (!recipes.isArray()) {
                continue;
            }

            for (JsonNode recipe : recipes) {
                if (recipe == null || !NPC_SHOP.equals(normalize(recipe.path("type").asString("")))) {
                    continue;
                }

                ParsedStack result = parseStack(recipe.path("result").asString(""));
                if (result == null) {
                    continue;
                }

                List<NpcShopOfferDto.CostDto> costs = parseCosts(recipe.path("cost"));
                long coinCost = costs.stream()
                        .filter(cost -> SKYBLOCK_COIN.equals(cost.itemId()))
                        .mapToLong(NpcShopOfferDto.CostDto::amount)
                        .sum();
                Long effectiveCoinCost = coinCost > 0 ? coinCost : null;
                Double unitCoinCost = effectiveCoinCost == null
                        ? null
                        : (double) effectiveCoinCost / result.amount();

                offers.add(new NpcShopOfferDto(
                        npcId,
                        npcDisplayName,
                        result.itemId(),
                        result.amount(),
                        List.copyOf(costs),
                        effectiveCoinCost,
                        unitCoinCost
                ));
            }
        }
        return offers;
    }

    private List<NpcShopOfferDto.CostDto> parseCosts(JsonNode costNode) {
        if (costNode == null || !costNode.isArray()) {
            return List.of();
        }
        List<NpcShopOfferDto.CostDto> costs = new ArrayList<>();
        for (JsonNode cost : costNode) {
            ParsedStack parsed = parseStack(cost.asString(""));
            if (parsed != null) {
                costs.add(new NpcShopOfferDto.CostDto(parsed.itemId(), parsed.amount()));
            }
        }
        return costs;
    }

    private ParsedStack parseStack(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }

        String[] parts = value.trim().split(":");
        if (parts.length == 0 || parts[0].isBlank()) {
            return null;
        }

        String itemId = parts[0].trim();
        int amount = 1;
        if (parts.length > 1) {
            try {
                amount = Integer.parseInt(parts[1].trim());
            } catch (NumberFormatException ignored) {
            }
        }
        return new ParsedStack(itemId, Math.max(1, amount));
    }

    private String firstNonBlank(JsonNode node, String... fieldNames) {
        for (String fieldName : fieldNames) {
            String value = node.path(fieldName).asString("");
            if (!value.isBlank()) {
                return value;
            }
        }
        return "";
    }

    private String normalize(String value) {
        return value == null ? "" : value.trim().toUpperCase(Locale.ROOT);
    }

    private record ParsedStack(String itemId, int amount) {
    }
}
