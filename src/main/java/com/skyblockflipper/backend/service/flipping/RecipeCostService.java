package com.skyblockflipper.backend.service.flipping;

import com.skyblockflipper.backend.NEU.repository.ItemRepository;
import com.skyblockflipper.backend.api.RecipeCostBreakdownDto;
import com.skyblockflipper.backend.model.Flipping.Recipe.Recipe;
import com.skyblockflipper.backend.model.Flipping.Recipe.RecipeIngredient;
import com.skyblockflipper.backend.model.market.AuctionMarketRecord;
import com.skyblockflipper.backend.model.market.BazaarMarketRecord;
import com.skyblockflipper.backend.model.market.MarketSnapshot;
import com.skyblockflipper.backend.repository.RecipeRepository;
import com.skyblockflipper.backend.service.market.MarketSnapshotPersistenceService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

@Service
public class RecipeCostService {

    private final RecipeRepository recipeRepository;
    private final MarketSnapshotPersistenceService marketSnapshotPersistenceService;
    private final ItemRepository itemRepository;

    public RecipeCostService(RecipeRepository recipeRepository,
                             MarketSnapshotPersistenceService marketSnapshotPersistenceService,
                             ItemRepository itemRepository) {
        this.recipeRepository = recipeRepository;
        this.marketSnapshotPersistenceService = marketSnapshotPersistenceService;
        this.itemRepository = itemRepository;
    }

    @Transactional(readOnly = true)
    public Optional<RecipeCostBreakdownDto> costBreakdown(String recipeId) {
        Optional<Recipe> recipeOpt = recipeRepository.findById(recipeId);
        Optional<MarketSnapshot> snapshotOpt = marketSnapshotPersistenceService.latest();
        if (recipeOpt.isEmpty() || snapshotOpt.isEmpty()) {
            return Optional.empty();
        }

        Recipe recipe = recipeOpt.get();
        MarketSnapshot snapshot = snapshotOpt.get();
        List<RecipeCostBreakdownDto.IngredientCostDto> ingredientCosts = new ArrayList<>();

        long totalCraftCost = 0L;
        for (RecipeIngredient ingredient : recipe.getIngredients()) {
            long unitPrice = resolveBuyPrice(snapshot, ingredient.getItemId());
            long total = unitPrice * ingredient.getAmount();
            totalCraftCost += total;
            ingredientCosts.add(new RecipeCostBreakdownDto.IngredientCostDto(
                    ingredient.getItemId(),
                    ingredient.getAmount(),
                    unitPrice,
                    total
            ));
        }

        String outputItemId = recipe.getOutputItem().getId();
        long outputSellPrice = resolveSellPrice(snapshot, outputItemId);
        long profit = outputSellPrice - totalCraftCost;
        double profitPct = totalCraftCost <= 0 ? 0D : round2((profit * 100D) / totalCraftCost);

        return Optional.of(new RecipeCostBreakdownDto(
                recipeId,
                outputItemId,
                ingredientCosts,
                totalCraftCost,
                outputSellPrice,
                profit,
                profitPct
        ));
    }

    private long resolveBuyPrice(MarketSnapshot snapshot, String itemId) {
        String normalized = normalize(itemId);
        BazaarMarketRecord bazaar = snapshot.bazaarProducts().get(normalized);
        if (bazaar != null && bazaar.buyPrice() > 0) {
            return Math.round(bazaar.buyPrice());
        }
        return resolveAuctionPrice(snapshot, normalized, true);
    }

    private long resolveSellPrice(MarketSnapshot snapshot, String itemId) {
        String normalized = normalize(itemId);
        BazaarMarketRecord bazaar = snapshot.bazaarProducts().get(normalized);
        if (bazaar != null && bazaar.sellPrice() > 0) {
            return Math.round(bazaar.sellPrice());
        }
        return resolveAuctionPrice(snapshot, normalized, false);
    }

    private long resolveAuctionPrice(MarketSnapshot snapshot, String itemId, boolean buyPrice) {
        Set<String> aliases = aliasesFor(itemId);
        List<AuctionMarketRecord> matches = snapshot.auctions().stream()
                .filter(auction -> matches(auction, aliases))
                .toList();
        if (matches.isEmpty()) {
            return 0L;
        }
        if (buyPrice) {
            return matches.stream().map(AuctionMarketRecord::startingBid).min(Long::compareTo).orElse(0L);
        }
        return matches.stream()
                .map(auction -> auction.highestBidAmount() > 0 ? auction.highestBidAmount() : auction.startingBid())
                .mapToLong(Long::longValue)
                .sum() / matches.size();
    }

    private Set<String> aliasesFor(String itemId) {
        Set<String> aliases = new HashSet<>();
        addAlias(aliases, itemId);
        itemRepository.findById(itemId).ifPresent(item -> {
            addAlias(aliases, item.getId());
            addAlias(aliases, item.getDisplayName());
            addAlias(aliases, item.getMinecraftId());
        });
        return aliases;
    }

    private void addAlias(Set<String> aliases, String value) {
        String normalized = normalize(value);
        if (normalized.isEmpty()) {
            return;
        }
        aliases.add(normalized);
        aliases.add(normalized.replace("_", "").replace(" ", ""));
    }

    private boolean matches(AuctionMarketRecord auction, Set<String> aliases) {
        if (auction == null || auction.itemName() == null) {
            return false;
        }
        String normalized = normalize(auction.itemName());
        String compact = normalized.replace("_", "").replace(" ", "");
        return aliases.stream().anyMatch(alias ->
                normalized.equals(alias)
                        || compact.equals(alias)
                        || normalized.contains(alias)
                        || compact.contains(alias)
        );
    }

    private String normalize(String value) {
        if (value == null || value.isBlank()) {
            return "";
        }
        return value.trim().toUpperCase(Locale.ROOT);
    }

    private double round2(double value) {
        return Math.round(value * 100D) / 100D;
    }
}
