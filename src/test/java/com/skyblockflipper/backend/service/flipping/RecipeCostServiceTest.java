package com.skyblockflipper.backend.service.flipping;

import com.skyblockflipper.backend.NEU.model.Item;
import com.skyblockflipper.backend.NEU.repository.ItemRepository;
import com.skyblockflipper.backend.api.RecipeCostBreakdownDto;
import com.skyblockflipper.backend.model.Flipping.Recipe.Recipe;
import com.skyblockflipper.backend.model.Flipping.Recipe.RecipeIngredient;
import com.skyblockflipper.backend.model.Flipping.Recipe.RecipeProcessType;
import com.skyblockflipper.backend.model.market.AuctionMarketRecord;
import com.skyblockflipper.backend.model.market.BazaarMarketRecord;
import com.skyblockflipper.backend.model.market.MarketSnapshot;
import com.skyblockflipper.backend.repository.RecipeRepository;
import com.skyblockflipper.backend.service.market.MarketSnapshotPersistenceService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RecipeCostServiceTest {

    @Mock
    private RecipeRepository recipeRepository;

    @Mock
    private MarketSnapshotPersistenceService snapshotService;

    @Mock
    private ItemRepository itemRepository;

    private RecipeCostService service;

    @BeforeEach
    void setUp() {
        service = new RecipeCostService(recipeRepository, snapshotService, itemRepository);
    }

    @Test
    void costBreakdownReturnsEmptyWhenRecipeMissing() {
        when(recipeRepository.findById("missing")).thenReturn(Optional.empty());
        when(snapshotService.latest()).thenReturn(Optional.of(new MarketSnapshot(
                Instant.parse("2026-02-21T12:00:00Z"),
                List.of(),
                Map.of()
        )));

        assertFalse(service.costBreakdown("missing").isPresent());
    }

    @Test
    void costBreakdownReturnsEmptyWhenSnapshotMissing() {
        Recipe recipe = new Recipe(
                "missing_snapshot",
                Item.builder().id("GOLDEN_PLATE").build(),
                RecipeProcessType.CRAFT,
                0L,
                List.of(new RecipeIngredient("ENCHANTED_GOLD_BLOCK", 1))
        );
        when(recipeRepository.findById("missing_snapshot")).thenReturn(Optional.of(recipe));
        when(snapshotService.latest()).thenReturn(Optional.empty());

        assertFalse(service.costBreakdown("missing_snapshot").isPresent());
    }

    @Test
    void costBreakdownUsesBazaarThenAuctionAliasesAndComputesProfit() {
        Recipe recipe = new Recipe(
                "golden_plate_craft",
                Item.builder().id("GOLDEN_PLATE").build(),
                RecipeProcessType.CRAFT,
                0L,
                List.of(
                        new RecipeIngredient("ENCHANTED_GOLD_BLOCK", 32),
                        new RecipeIngredient("ENCHANTED_REDSTONE_BLOCK", 8)
                )
        );
        when(recipeRepository.findById("golden_plate_craft")).thenReturn(Optional.of(recipe));

        MarketSnapshot snapshot = new MarketSnapshot(
                Instant.parse("2026-02-21T12:00:00Z"),
                List.of(
                        new AuctionMarketRecord("a1", "Enchanted Redstone Block", "MATERIAL", "UNCOMMON",
                                75_000L, 0L, 1L, 2L, false)
                ),
                Map.of(
                        "ENCHANTED_GOLD_BLOCK", new BazaarMarketRecord("ENCHANTED_GOLD_BLOCK", 180_000, 175_000, 1, 1, 1, 1, 1, 1),
                        "GOLDEN_PLATE", new BazaarMarketRecord("GOLDEN_PLATE", 9_700_000, 9_500_000, 1, 1, 1, 1, 1, 1)
                )
        );
        when(snapshotService.latest()).thenReturn(Optional.of(snapshot));
        when(itemRepository.findById("ENCHANTED_REDSTONE_BLOCK"))
                .thenReturn(Optional.of(Item.builder()
                        .id("ENCHANTED_REDSTONE_BLOCK")
                        .displayName("Enchanted Redstone Block")
                        .minecraftId("enchanted_redstone_block")
                        .build()));

        Optional<RecipeCostBreakdownDto> result = service.costBreakdown("golden_plate_craft");

        assertTrue(result.isPresent());
        RecipeCostBreakdownDto dto = result.get();
        assertEquals(2, dto.ingredients().size());
        assertEquals(6_360_000L, dto.totalCraftCost());
        assertEquals(9_500_000L, dto.outputSellPrice());
        assertEquals(3_140_000L, dto.profit());
        assertEquals(49.37D, dto.profitPct(), 0.001D);
    }

    @Test
    void costBreakdownUsesAuctionAverageSellPriceWhenBazaarMissing() {
        Recipe recipe = new Recipe(
                "midas_recipe",
                Item.builder().id("MIDAS_SWORD").build(),
                RecipeProcessType.CRAFT,
                0L,
                List.of(new RecipeIngredient("GOLD_INGOT", 2))
        );
        when(recipeRepository.findById("midas_recipe")).thenReturn(Optional.of(recipe));

        MarketSnapshot snapshot = new MarketSnapshot(
                Instant.parse("2026-02-21T12:00:00Z"),
                List.of(
                        new AuctionMarketRecord("a1", "Gold Ingot", "MATERIAL", "COMMON", 100L, 0L, 1L, 2L, false),
                        new AuctionMarketRecord("a2", "Midas Sword", "WEAPON", "LEGENDARY", 1_000L, 1_100L, 1L, 2L, false),
                        new AuctionMarketRecord("a3", "Midas Sword", "WEAPON", "LEGENDARY", 1_200L, 0L, 1L, 2L, false)
                ),
                Map.of()
        );
        when(snapshotService.latest()).thenReturn(Optional.of(snapshot));
        when(itemRepository.findById("GOLD_INGOT"))
                .thenReturn(Optional.of(Item.builder().id("GOLD_INGOT").displayName("Gold Ingot").minecraftId("gold_ingot").build()));
        when(itemRepository.findById("MIDAS_SWORD"))
                .thenReturn(Optional.of(Item.builder().id("MIDAS_SWORD").displayName("Midas Sword").minecraftId("midas_sword").build()));

        RecipeCostBreakdownDto dto = service.costBreakdown("midas_recipe").orElseThrow();

        assertEquals(200L, dto.totalCraftCost());
        assertEquals(1_150L, dto.outputSellPrice());
        assertEquals(950L, dto.profit());
        assertEquals(475.0D, dto.profitPct(), 0.01D);
    }
}
