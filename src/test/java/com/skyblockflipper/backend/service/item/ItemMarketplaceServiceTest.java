package com.skyblockflipper.backend.service.item;

import com.skyblockflipper.backend.NEU.model.Item;
import com.skyblockflipper.backend.api.dto.MarketplaceType;
import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.model.Flipping.Flip;
import com.skyblockflipper.backend.model.market.AuctionMarketRecord;
import com.skyblockflipper.backend.model.market.BazaarMarketRecord;
import com.skyblockflipper.backend.model.market.MarketSnapshot;
import com.skyblockflipper.backend.repository.FlipRepository;
import com.skyblockflipper.backend.service.market.MarketSnapshotPersistenceService;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ItemMarketplaceServiceTest {

    @Test
    void resolveMarketplacesClassifiesItemsAcrossAllMarketplaceTypes() {
        MarketSnapshotPersistenceService snapshotService = mock(MarketSnapshotPersistenceService.class);
        FlipRepository flipRepository = mock(FlipRepository.class);
        ItemMarketplaceService service = new ItemMarketplaceService(snapshotService, flipRepository);

        MarketSnapshot snapshot = new MarketSnapshot(
                Instant.parse("2026-02-21T12:00:00Z"),
                List.of(new AuctionMarketRecord(
                        "a1", "Hyperion", "WEAPON", "LEGENDARY",
                        500_000_000L, 0L, 1L, 2L, false
                )),
                Map.of(
                        "ENCHANTED_DIAMOND", new BazaarMarketRecord("ENCHANTED_DIAMOND", 100, 95, 20, 20, 0, 0, 1, 1),
                        "HYPERION", new BazaarMarketRecord("HYPERION", 800_000_000, 790_000_000, 10, 10, 0, 0, 1, 1)
                )
        );
        when(snapshotService.latest()).thenReturn(Optional.of(snapshot));
        when(flipRepository.findMaxSnapshotTimestampEpochMillis()).thenReturn(Optional.of(123L));
        when(flipRepository.findByFlipTypeAndSnapshotTimestampEpochMillis(FlipType.AUCTION, 123L))
                .thenReturn(List.of(new Flip(UUID.randomUUID(), FlipType.AUCTION, List.of(), "MIDAS_SWORD", List.of())));

        Item bazaarOnly = item("ENCHANTED_DIAMOND", "Enchanted Diamond", "enchanted_diamond");
        Item auctionOnly = item("MIDAS_SWORD", "Midas Sword", "midas_sword");
        Item both = item("HYPERION", "Hyperion", "hyperion");
        Item none = item("DIRT", "Dirt", "dirt");

        Map<String, MarketplaceType> result = service.resolveMarketplaces(List.of(bazaarOnly, auctionOnly, both, none));

        assertEquals(MarketplaceType.BAZAAR, result.get("ENCHANTED_DIAMOND"));
        assertEquals(MarketplaceType.AUCTION_HOUSE, result.get("MIDAS_SWORD"));
        assertEquals(MarketplaceType.BOTH, result.get("HYPERION"));
        assertEquals(MarketplaceType.NONE, result.get("DIRT"));
    }

    @Test
    void resolveMarketplacesReturnsEmptyForNullOrEmptyInput() {
        MarketSnapshotPersistenceService snapshotService = mock(MarketSnapshotPersistenceService.class);
        FlipRepository flipRepository = mock(FlipRepository.class);
        ItemMarketplaceService service = new ItemMarketplaceService(snapshotService, flipRepository);

        Map<String, MarketplaceType> nullResult = service.resolveMarketplaces(null);
        Map<String, MarketplaceType> emptyResult = service.resolveMarketplaces(List.of());

        assertTrue(nullResult.isEmpty());
        assertTrue(emptyResult.isEmpty());
    }

    private Item item(String id, String displayName, String minecraftId) {
        return Item.builder().id(id).displayName(displayName).minecraftId(minecraftId).build();
    }
}
