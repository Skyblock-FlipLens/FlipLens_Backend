package com.skyblockflipper.backend.service.market;

import com.skyblockflipper.backend.NEU.model.Item;
import com.skyblockflipper.backend.NEU.repository.ItemRepository;
import com.skyblockflipper.backend.api.dto.BazaarOrderBookDto;
import com.skyblockflipper.backend.api.dto.BazaarProductDto;
import com.skyblockflipper.backend.api.dto.BazaarQuickFlipDto;
import com.skyblockflipper.backend.hypixel.HypixelClient;
import com.skyblockflipper.backend.hypixel.model.BazaarProduct;
import com.skyblockflipper.backend.hypixel.model.BazaarResponse;
import com.skyblockflipper.backend.hypixel.model.BazaarSummaryEntry;
import com.skyblockflipper.backend.model.market.BazaarMarketRecord;
import com.skyblockflipper.backend.model.market.MarketSnapshot;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BazaarReadServiceTest {

    private MarketSnapshotPersistenceService snapshotService;
    private HypixelClient hypixelClient;
    private ItemRepository itemRepository;
    private BazaarReadService service;

    @BeforeEach
    void setUp() {
        snapshotService = mock(MarketSnapshotPersistenceService.class);
        hypixelClient = mock(HypixelClient.class);
        itemRepository = mock(ItemRepository.class);
        service = new BazaarReadService(snapshotService, hypixelClient, itemRepository);
    }

    @Test
    void getProductReadsLatestSnapshotAndNormalizesItemId() {
        BazaarMarketRecord record = new BazaarMarketRecord("ENCHANTED_DIAMOND_BLOCK", 205000.4, 198000.1, 100, 90, 200, 180, 5, 4);
        MarketSnapshot snapshot = new MarketSnapshot(
                Instant.parse("2026-02-21T10:00:00Z"),
                List.of(),
                Map.of("ENCHANTED_DIAMOND_BLOCK", record)
        );
        when(snapshotService.latest()).thenReturn(Optional.of(snapshot));

        Optional<BazaarProductDto> result = service.getProduct(" enchanted_diamond_block ");

        assertTrue(result.isPresent());
        // BazaarReadService rounds to nearest long via Math.round for API payload size/stability.
        assertEquals(205000L, result.get().buyPrice());
        assertEquals(198000L, result.get().sellPrice());
        assertEquals(100L, result.get().buyVolume());
        assertEquals(90L, result.get().sellVolume());
    }

    @Test
    void getOrderBookMapsAndLimitsDepth() {
        BazaarProduct product = new BazaarProduct(
                "ENCHANTED_DIAMOND_BLOCK",
                null,
                List.of(
                        new BazaarSummaryEntry(5L, 198000.0, 1),
                        new BazaarSummaryEntry(10L, 197800.0, 2)
                ),
                List.of(
                        new BazaarSummaryEntry(3L, 205100.0, 1),
                        new BazaarSummaryEntry(8L, 205300.0, 2)
                )
        );
        when(hypixelClient.fetchBazaar()).thenReturn(new BazaarResponse(true, 0L, Map.of("ENCHANTED_DIAMOND_BLOCK", product)));

        BazaarOrderBookDto result = service.getOrderBook("ENCHANTED_DIAMOND_BLOCK", 1);

        assertEquals(1, result.sellOrders().size());
        assertEquals(1, result.buyOrders().size());
        assertEquals(205100.0, result.sellOrders().getFirst().pricePerUnit());
        assertEquals(198000.0, result.buyOrders().getFirst().pricePerUnit());
    }

    @Test
    void getOrderBookReturnsEmptyWhenBazaarResponseMissing() {
        when(hypixelClient.fetchBazaar()).thenReturn(null);

        BazaarOrderBookDto result = service.getOrderBook("ANY", 10);

        assertTrue(result.buyOrders().isEmpty());
        assertTrue(result.sellOrders().isEmpty());
    }

    @Test
    void quickFlipsFiltersSortsAndUsesDisplayNames() {
        MarketSnapshot snapshot = new MarketSnapshot(
                Instant.parse("2026-02-21T11:00:00Z"),
                List.of(),
                Map.of(
                        "A", new BazaarMarketRecord("A", 100, 90, 1000, 1000, 0, 0, 1, 1),
                        "B", new BazaarMarketRecord("B", 200, 199, 500, 500, 0, 0, 1, 1),
                        "C", new BazaarMarketRecord("C", 300, 240, 200, 150, 0, 0, 1, 1)
                )
        );
        when(snapshotService.latest()).thenReturn(Optional.of(snapshot));
        when(itemRepository.findAll()).thenReturn(List.of(
                Item.builder().id("A").displayName("Alpha").build(),
                Item.builder().id("C").displayName("Charlie").build()
        ));

        List<BazaarQuickFlipDto> result = service.quickFlips(5.0, 2);

        assertEquals(2, result.size());
        assertEquals("C", result.getFirst().itemId());
        assertEquals("Charlie", result.getFirst().displayName());
        assertEquals(20.0, result.getFirst().spreadPct());
        assertEquals("A", result.get(1).itemId());
        assertFalse(result.stream().anyMatch(dto -> dto.itemId().equals("B")));
    }
}
