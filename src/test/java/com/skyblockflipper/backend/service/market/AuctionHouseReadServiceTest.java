package com.skyblockflipper.backend.service.market;

import com.skyblockflipper.backend.NEU.model.Item;
import com.skyblockflipper.backend.NEU.repository.ItemRepository;
import com.skyblockflipper.backend.api.AhListingDto;
import com.skyblockflipper.backend.api.AhListingSortBy;
import com.skyblockflipper.backend.api.AhRecentSaleDto;
import com.skyblockflipper.backend.model.market.AuctionMarketRecord;
import com.skyblockflipper.backend.model.market.MarketSnapshot;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AuctionHouseReadServiceTest {

    @Test
    void listListingsReturnsEmptyWhenNoSnapshotExists() {
        MarketSnapshotPersistenceService snapshotService = mock(MarketSnapshotPersistenceService.class);
        ItemRepository itemRepository = mock(ItemRepository.class);
        when(snapshotService.latest()).thenReturn(Optional.empty());
        AuctionHouseReadService service = new AuctionHouseReadService(snapshotService, itemRepository);

        Page<AhListingDto> page = service.listListings(
                "HYPERION",
                AhListingSortBy.PRICE,
                Sort.Direction.ASC,
                null,
                null,
                null,
                null,
                Pageable.unpaged()
        );

        assertTrue(page.getContent().isEmpty());
        assertEquals(0, page.getTotalElements());
    }

    @Test
    void listListingsParsesStarsReforgeAndGemSlotsFromLore() {
        MarketSnapshotPersistenceService snapshotService = mock(MarketSnapshotPersistenceService.class);
        ItemRepository itemRepository = mock(ItemRepository.class);
        AuctionHouseReadService service = new AuctionHouseReadService(snapshotService, itemRepository);

        AuctionMarketRecord listing = new AuctionMarketRecord(
                "auction-1",
                "Hyperion ✪✪✪",
                "weapon",
                "LEGENDARY",
                900_000_000L,
                910_000_000L,
                1_000L,
                2_000L,
                false,
                "§7Modifier: §aWithered\n§7Gemstone Slots: Combat Slot, Ruby Slot, Topaz Slot",
                "extra"
        );
        MarketSnapshot snapshot = new MarketSnapshot(
                Instant.parse("2026-02-21T12:00:00Z"),
                List.of(listing),
                Map.of()
        );
        when(snapshotService.latest()).thenReturn(Optional.of(snapshot));
        when(itemRepository.findById("HYPERION")).thenReturn(Optional.empty());

        List<AhListingDto> listings = service.listListings(
                "HYPERION",
                AhListingSortBy.PRICE,
                Sort.Direction.ASC,
                null,
                null,
                null,
                null,
                Pageable.unpaged()
        ).getContent();

        assertEquals(1, listings.size());
        AhListingDto dto = listings.getFirst();
        assertEquals(3, dto.stars());
        assertEquals("Withered", dto.reforge());
        assertTrue(dto.gemSlots().contains("Combat"));
        assertTrue(dto.gemSlots().contains("Ruby"));
        assertTrue(dto.gemSlots().contains("Topaz"));
    }

    @Test
    void listListingsAppliesFiltersSortAndPagination() {
        MarketSnapshotPersistenceService snapshotService = mock(MarketSnapshotPersistenceService.class);
        ItemRepository itemRepository = mock(ItemRepository.class);
        AuctionHouseReadService service = new AuctionHouseReadService(snapshotService, itemRepository);

        AuctionMarketRecord matching = new AuctionMarketRecord(
                "a1",
                "Withered Hyperion \u272A\u272A",
                "weapon",
                "LEGENDARY",
                900_000_000L,
                0L,
                1_000L,
                4_000L,
                false,
                true,
                "line",
                null
        );
        AuctionMarketRecord wrongReforge = new AuctionMarketRecord(
                "a2",
                "Heroic Hyperion \u272A\u272A\u272A",
                "weapon",
                "LEGENDARY",
                800_000_000L,
                0L,
                1_000L,
                3_000L,
                false,
                true,
                "line",
                null
        );
        AuctionMarketRecord wrongType = new AuctionMarketRecord(
                "a3",
                "Withered Hyperion \u272A\u272A",
                "weapon",
                "LEGENDARY",
                700_000_000L,
                0L,
                1_000L,
                2_000L,
                false,
                false,
                "line",
                null
        );
        MarketSnapshot snapshot = new MarketSnapshot(
                Instant.parse("2026-02-21T12:00:00Z"),
                List.of(matching, wrongReforge, wrongType),
                Map.of()
        );
        when(snapshotService.latest()).thenReturn(Optional.of(snapshot));

        Page<AhListingDto> page = service.listListings(
                "HYPERION",
                AhListingSortBy.PRICE,
                Sort.Direction.ASC,
                true,
                2,
                2,
                "withered",
                PageRequest.of(0, 1)
        );

        assertEquals(1, page.getTotalElements());
        assertEquals(1, page.getContent().size());
        assertEquals("a1", page.getContent().getFirst().auctionId());
    }

    @Test
    void breakdownBuildsAggregatesAndLowestBin() {
        MarketSnapshotPersistenceService snapshotService = mock(MarketSnapshotPersistenceService.class);
        ItemRepository itemRepository = mock(ItemRepository.class);
        AuctionHouseReadService service = new AuctionHouseReadService(snapshotService, itemRepository);

        AuctionMarketRecord binOne = new AuctionMarketRecord("b1", "Withered Hyperion \u272A", "weapon", "LEGENDARY", 100L, 0L, 1L, 10L, false, true, null, null);
        AuctionMarketRecord binTwo = new AuctionMarketRecord("b2", "Heroic Hyperion \u272A\u272A", "weapon", "LEGENDARY", 200L, 0L, 1L, 11L, false, true, null, null);
        AuctionMarketRecord auction = new AuctionMarketRecord("a1", "Hyperion", "weapon", "LEGENDARY", 50L, 300L, 1L, 12L, false, false, null, null);
        MarketSnapshot snapshot = new MarketSnapshot(Instant.now(), List.of(binOne, binTwo, auction), Map.of());
        when(snapshotService.latest()).thenReturn(Optional.of(snapshot));

        var breakdown = service.breakdown("HYPERION");

        assertEquals(3, breakdown.totalListings());
        assertEquals(2L, breakdown.byType().get("BIN"));
        assertEquals(1L, breakdown.byType().get("AUCTION"));
        assertEquals(100L, breakdown.lowestBin());
        assertFalse(breakdown.byReforge().isEmpty());
    }

    @Test
    void recentSalesReturnsClaimedSortedByNewestAndHonorsSafeLimit() {
        MarketSnapshotPersistenceService snapshotService = mock(MarketSnapshotPersistenceService.class);
        ItemRepository itemRepository = mock(ItemRepository.class);
        AuctionHouseReadService service = new AuctionHouseReadService(snapshotService, itemRepository);

        AuctionMarketRecord oldest = new AuctionMarketRecord("s1", "Hyperion", "weapon", "LEGENDARY", 100L, 120L, 1L, 10L, true, false, null, null);
        AuctionMarketRecord newest = new AuctionMarketRecord("s2", "Hyperion", "weapon", "LEGENDARY", 100L, 180L, 1L, 20L, true, true, null, null);
        AuctionMarketRecord notClaimed = new AuctionMarketRecord("s3", "Hyperion", "weapon", "LEGENDARY", 100L, 200L, 1L, 30L, false, true, null, null);
        MarketSnapshot snapshot = new MarketSnapshot(Instant.now(), List.of(oldest, newest, notClaimed), Map.of());
        when(snapshotService.latest()).thenReturn(Optional.of(snapshot));

        List<AhRecentSaleDto> sales = service.recentSales("HYPERION", 0);

        assertEquals(1, sales.size());
        assertEquals("s2", sales.getFirst().auctionId());
        assertTrue(sales.getFirst().bin());
    }

    @Test
    void listListingsMatchesAliasesFromItemRepository() {
        MarketSnapshotPersistenceService snapshotService = mock(MarketSnapshotPersistenceService.class);
        ItemRepository itemRepository = mock(ItemRepository.class);
        AuctionHouseReadService service = new AuctionHouseReadService(snapshotService, itemRepository);

        AuctionMarketRecord listing = new AuctionMarketRecord(
                "x1",
                "Terminator Bow \u272A",
                "weapon",
                "LEGENDARY",
                10L,
                0L,
                1L,
                2L,
                false,
                true,
                null,
                null
        );
        MarketSnapshot snapshot = new MarketSnapshot(Instant.now(), List.of(listing), Map.of());
        when(snapshotService.latest()).thenReturn(Optional.of(snapshot));
        when(itemRepository.findById("TERMINATOR_ITEM")).thenReturn(Optional.of(
                Item.builder()
                        .id("TERMINATOR_ITEM")
                        .displayName("Terminator Bow")
                        .minecraftId("minecraft:bow")
                        .build()
        ));

        List<AhListingDto> results = service.listListings(
                "TERMINATOR_ITEM",
                AhListingSortBy.PRICE,
                Sort.Direction.ASC,
                null,
                null,
                null,
                null,
                Pageable.unpaged()
        ).getContent();

        assertEquals(1, results.size());
        assertEquals("x1", results.getFirst().auctionId());
    }
}
