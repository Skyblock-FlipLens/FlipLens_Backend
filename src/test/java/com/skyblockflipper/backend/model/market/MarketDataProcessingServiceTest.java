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
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
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
        auction.setBin(true);
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

    @Test
    void captureCurrentSnapshotAndPrepareInputUsesCachedPayloadWhenWithinCooldown() {
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
        auction.setBin(true);
        AuctionResponse auctionResponse = new AuctionResponse(true, 0, 1, 1, 10_000L, List.of(auction));
        BazaarQuickStatus quickStatus = new BazaarQuickStatus(10.0, 9.0, 100, 90, 1000, 900, 4, 3);
        BazaarProduct bazaarProduct = new BazaarProduct("ENCHANTED_DIAMOND", quickStatus, List.of(), List.of());
        BazaarResponse bazaarResponse = new BazaarResponse(true, 11_000L, Map.of("ENCHANTED_DIAMOND", bazaarProduct));

        when(client.fetchAllAuctionPages()).thenReturn(auctionResponse);
        when(client.fetchBazaar()).thenReturn(bazaarResponse);
        when(persistenceService.save(org.mockito.ArgumentMatchers.any(MarketSnapshot.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        service.captureCurrentSnapshotAndPrepareInput().orElseThrow();
        service.captureCurrentSnapshotAndPrepareInput().orElseThrow();

        verify(client, times(1)).fetchAllAuctionPages();
        verify(client, times(1)).fetchBazaar();
        verify(persistenceService, times(2)).save(org.mockito.ArgumentMatchers.any(MarketSnapshot.class));
    }

    @Test
    void captureCurrentSnapshotAndPrepareInputUsesFreshDecisionTimePerEndpoint() {
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
        auction.setBin(true);
        AuctionResponse auctionResponse = new AuctionResponse(true, 0, 1, 1, 10_000L, List.of(auction));

        BazaarQuickStatus quickStatus = new BazaarQuickStatus(10.0, 9.0, 100, 90, 1000, 900, 4, 3);
        BazaarProduct bazaarProduct = new BazaarProduct("ENCHANTED_DIAMOND", quickStatus, List.of(), List.of());
        BazaarResponse staleCachedBazaar = new BazaarResponse(true, 9_000L, Map.of("ENCHANTED_DIAMOND", bazaarProduct));
        BazaarResponse freshBazaar = new BazaarResponse(true, 11_000L, Map.of("ENCHANTED_DIAMOND", bazaarProduct));

        when(client.fetchAllAuctionPages()).thenAnswer(invocation -> {
            Thread.sleep(40L);
            return auctionResponse;
        });
        when(client.fetchBazaar()).thenReturn(freshBazaar);
        when(persistenceService.save(org.mockito.ArgumentMatchers.any(MarketSnapshot.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        ReflectionTestUtils.setField(service, "cachedBazaarResponse", staleCachedBazaar);
        ReflectionTestUtils.setField(service, "nextBazaarFetchAtMillis", System.currentTimeMillis() + 20L);

        service.captureCurrentSnapshotAndPrepareInput().orElseThrow();

        verify(client, times(1)).fetchBazaar();
    }

    @Test
    void captureCurrentSnapshotAndPrepareInputDoesNotRegressSourceWatermarks() throws InterruptedException {
        HypixelClient client = mock(HypixelClient.class);
        HypixelMarketSnapshotMapper snapshotMapper = new HypixelMarketSnapshotMapper();
        MarketSnapshotPersistenceService persistenceService = mock(MarketSnapshotPersistenceService.class);
        UnifiedFlipInputMapper inputMapper = new UnifiedFlipInputMapper();
        com.skyblockflipper.backend.instrumentation.CycleInstrumentationService instrumentation =
                new com.skyblockflipper.backend.instrumentation.CycleInstrumentationService(
                        new io.micrometer.core.instrument.simple.SimpleMeterRegistry(),
                        new com.skyblockflipper.backend.instrumentation.BlockingTimeTracker(
                                new com.skyblockflipper.backend.instrumentation.InstrumentationProperties()
                        )
                );
        MarketDataProcessingService service = new MarketDataProcessingService(
                client,
                snapshotMapper,
                persistenceService,
                inputMapper,
                instrumentation,
                Duration.ofMillis(1),
                Duration.ofMillis(1),
                1L,
                Duration.ofMillis(1)
        );

        Auction auction = new Auction(
                "a-1", "auctioneer", "profile", List.of(), 1L, 2L,
                "ENCHANTED_DIAMOND", "lore", "extra", "misc", "RARE",
                100L, false, List.of(), 120L, List.of()
        );
        auction.setBin(true);
        AuctionResponse freshAuction = new AuctionResponse(true, 0, 1, 1, 10_000L, List.of(auction));
        AuctionResponse staleAuction = new AuctionResponse(true, 0, 1, 1, 9_000L, List.of(auction));

        BazaarQuickStatus quickStatus = new BazaarQuickStatus(10.0, 9.0, 100, 90, 1000, 900, 4, 3);
        BazaarProduct bazaarProduct = new BazaarProduct("ENCHANTED_DIAMOND", quickStatus, List.of(), List.of());
        BazaarResponse freshBazaar = new BazaarResponse(true, 11_000L, Map.of("ENCHANTED_DIAMOND", bazaarProduct));
        BazaarResponse staleBazaar = new BazaarResponse(true, 10_500L, Map.of("ENCHANTED_DIAMOND", bazaarProduct));

        when(client.fetchAllAuctionPages()).thenReturn(freshAuction, staleAuction);
        when(client.fetchBazaar()).thenReturn(freshBazaar, staleBazaar);
        when(persistenceService.save(org.mockito.ArgumentMatchers.any(MarketSnapshot.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        service.captureCurrentSnapshotAndPrepareInput().orElseThrow();
        Thread.sleep(5L);
        service.captureCurrentSnapshotAndPrepareInput().orElseThrow();

        long auctionWatermark = (long) ReflectionTestUtils.getField(service, "lastAuctionLastUpdated");
        long bazaarWatermark = (long) ReflectionTestUtils.getField(service, "lastBazaarLastUpdated");
        assertEquals(10_000L, auctionWatermark);
        assertEquals(11_000L, bazaarWatermark);
    }
}
