package com.skyblockflipper.backend.model.market;

import com.skyblockflipper.backend.hypixel.HypixelClient;
import com.skyblockflipper.backend.hypixel.HypixelMarketSnapshotMapper;
import com.skyblockflipper.backend.hypixel.model.Auction;
import com.skyblockflipper.backend.hypixel.model.AuctionResponse;
import com.skyblockflipper.backend.hypixel.model.BazaarProduct;
import com.skyblockflipper.backend.hypixel.model.BazaarQuickStatus;
import com.skyblockflipper.backend.hypixel.model.BazaarResponse;
import com.skyblockflipper.backend.service.flipping.UnifiedFlipInputMapper;
import com.skyblockflipper.backend.service.market.MarketDataProcessingService;
import com.skyblockflipper.backend.service.market.MarketSnapshotPersistenceService;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MarketDataProcessingServiceTest {

    @Test
    void captureCurrentSnapshotAndPrepareInputStoresAndReturnsMappedInput() {
        HypixelClient client = mock(HypixelClient.class);
        HypixelMarketSnapshotMapper snapshotMapper = new HypixelMarketSnapshotMapper();
        MarketSnapshotPersistenceService persistenceService = mock(MarketSnapshotPersistenceService.class);
        UnifiedFlipInputMapper inputMapper = new UnifiedFlipInputMapper();
        MarketDataProcessingService service = new MarketDataProcessingService(client, snapshotMapper, persistenceService, inputMapper);

        Auction auction = new Auction(
                "a-1", "auctioneer", "profile", List.of(), 1L, 2L,
                "ENCHANTED_DIAMOND", "lore", "extra", "misc", "RARE",
                100L, false, List.of(), 120L, List.of()
        );
        AuctionResponse auctionResponse = new AuctionResponse(true, 0, 1, 1, 10_000L, List.of(auction));
        BazaarQuickStatus quickStatus = new BazaarQuickStatus(10.0, 9.0, 100, 90, 1000, 900, 4, 3);
        BazaarProduct bazaarProduct = new BazaarProduct("ENCHANTED_DIAMOND", quickStatus, List.of(), List.of());
        BazaarResponse bazaarResponse = new BazaarResponse(true, 11_000L, Map.of("ENCHANTED_DIAMOND", bazaarProduct));

        when(client.fetchAllAuctionPages()).thenReturn(auctionResponse);
        when(client.fetchBazaar()).thenReturn(bazaarResponse);
        when(persistenceService.save(org.mockito.ArgumentMatchers.any(MarketSnapshot.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        UnifiedFlipInputSnapshot input = service.captureCurrentSnapshotAndPrepareInput().orElseThrow();

        verify(persistenceService).save(org.mockito.ArgumentMatchers.any(MarketSnapshot.class));
        assertEquals(1, input.bazaarQuotes().size());
        assertEquals(1, input.auctionQuotesByItem().size());
        assertEquals(11_000L, input.snapshotTimestamp().toEpochMilli());
    }

    @Test
    void captureCurrentSnapshotAndPrepareInputReturnsEmptyWhenNoData() {
        HypixelClient client = mock(HypixelClient.class);
        HypixelMarketSnapshotMapper snapshotMapper = new HypixelMarketSnapshotMapper();
        MarketSnapshotPersistenceService persistenceService = mock(MarketSnapshotPersistenceService.class);
        UnifiedFlipInputMapper inputMapper = new UnifiedFlipInputMapper();
        MarketDataProcessingService service = new MarketDataProcessingService(client, snapshotMapper, persistenceService, inputMapper);

        when(client.fetchAllAuctionPages()).thenReturn(null);
        when(client.fetchBazaar()).thenReturn(null);

        assertTrue(service.captureCurrentSnapshotAndPrepareInput().isEmpty());
    }
}
