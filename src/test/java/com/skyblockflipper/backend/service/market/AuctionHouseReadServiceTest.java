package com.skyblockflipper.backend.service.market;

import com.skyblockflipper.backend.NEU.repository.ItemRepository;
import com.skyblockflipper.backend.api.AhListingDto;
import com.skyblockflipper.backend.api.AhListingSortBy;
import com.skyblockflipper.backend.model.market.AuctionMarketRecord;
import com.skyblockflipper.backend.model.market.MarketSnapshot;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AuctionHouseReadServiceTest {

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
}
