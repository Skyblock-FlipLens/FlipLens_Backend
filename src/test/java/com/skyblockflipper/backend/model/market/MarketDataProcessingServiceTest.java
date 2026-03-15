package com.skyblockflipper.backend.model.market;

import com.skyblockflipper.backend.hypixel.AuctionProbeInfo;
import com.skyblockflipper.backend.hypixel.HypixelClient;
import com.skyblockflipper.backend.hypixel.HypixelMarketSnapshotMapper;
import com.skyblockflipper.backend.hypixel.model.Auction;
import com.skyblockflipper.backend.hypixel.model.AuctionResponse;
import com.skyblockflipper.backend.hypixel.model.BazaarProduct;
import com.skyblockflipper.backend.hypixel.model.BazaarQuickStatus;
import com.skyblockflipper.backend.hypixel.model.BazaarResponse;
import com.skyblockflipper.backend.repository.AhItemSnapshotRepository;
import com.skyblockflipper.backend.repository.BzItemSnapshotRepository;
import com.skyblockflipper.backend.service.flipping.UnifiedFlipInputMapper;
import com.skyblockflipper.backend.service.market.AhSnapshotAggregator;
import com.skyblockflipper.backend.service.market.BzSnapshotAggregator;
import com.skyblockflipper.backend.service.market.MarketDataProcessingService;
import com.skyblockflipper.backend.service.market.MarketSnapshotStorageProperties;
import com.skyblockflipper.backend.service.market.MarketSnapshotPersistenceService;
import com.skyblockflipper.backend.instrumentation.BlockingTimeTracker;
import com.skyblockflipper.backend.instrumentation.CycleInstrumentationService;
import com.skyblockflipper.backend.instrumentation.InstrumentationProperties;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
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
                "ENCHANTED_DIAMOND", "lore", "{\"internalname\":\"ENCHANTED_DIAMOND\"}", "misc", "RARE",
                100L, false, List.of(), 120L, List.of()
        );
        auction.setBin(true);
        AuctionResponse auctionResponse = new AuctionResponse(true, 0, 1, 1, 10_000L, List.of(auction));
        BazaarQuickStatus quickStatus = new BazaarQuickStatus(10.0, 9.0, 100, 90, 1000, 900, 4, 3);
        BazaarProduct bazaarProduct = new BazaarProduct("ENCHANTED_DIAMOND", quickStatus, List.of(), List.of());
        BazaarResponse bazaarResponse = new BazaarResponse(true, 11_000L, Map.of("ENCHANTED_DIAMOND", bazaarProduct));

        mockAuctionStreaming(client, auctionResponse);
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

        when(client.probeAuctionsLastUpdated()).thenThrow(new RuntimeException("probe failed"));
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
                "ENCHANTED_DIAMOND", "lore", "{\"internalname\":\"ENCHANTED_DIAMOND\"}", "misc", "RARE",
                100L, false, List.of(), 120L, List.of()
        );
        auction.setBin(true);
        AuctionResponse auctionResponse = new AuctionResponse(true, 0, 1, 1, 10_000L, List.of(auction));
        BazaarQuickStatus quickStatus = new BazaarQuickStatus(10.0, 9.0, 100, 90, 1000, 900, 4, 3);
        BazaarProduct bazaarProduct = new BazaarProduct("ENCHANTED_DIAMOND", quickStatus, List.of(), List.of());
        BazaarResponse bazaarResponse = new BazaarResponse(true, 11_000L, Map.of("ENCHANTED_DIAMOND", bazaarProduct));

        mockAuctionStreaming(client, auctionResponse);
        when(client.fetchBazaar()).thenReturn(bazaarResponse);
        when(persistenceService.save(org.mockito.ArgumentMatchers.any(MarketSnapshot.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        service.captureCurrentSnapshotAndPrepareInput().orElseThrow();
        service.captureCurrentSnapshotAndPrepareInput().orElseThrow();

        verify(client, times(1)).probeAuctionsLastUpdated();
        verify(client, times(1)).fetchBazaar();
        verify(persistenceService, times(2)).save(org.mockito.ArgumentMatchers.any(MarketSnapshot.class));
    }

    @Test
    void captureCurrentSnapshotAndPrepareInputUsesFreshDecisionTimePerEndpoint() {
        HypixelClient client = mock(HypixelClient.class);
        HypixelMarketSnapshotMapper snapshotMapper = new HypixelMarketSnapshotMapper();
        MarketSnapshotPersistenceService persistenceService = mock(MarketSnapshotPersistenceService.class);
        UnifiedFlipInputMapper inputMapper = new UnifiedFlipInputMapper();
        AtomicLong nowMillis = new AtomicLong(1_000_000L);
        MarketDataProcessingService service = new MarketDataProcessingService(
                client,
                snapshotMapper,
                persistenceService,
                inputMapper,
                nowMillis::get
        );

        Auction auction = new Auction(
                "a-1", "auctioneer", "profile", List.of(), 1L, 2L,
                "ENCHANTED_DIAMOND", "lore", "{\"internalname\":\"ENCHANTED_DIAMOND\"}", "misc", "RARE",
                100L, false, List.of(), 120L, List.of()
        );
        auction.setBin(true);
        AuctionResponse auctionResponse = new AuctionResponse(true, 0, 1, 1, 10_000L, List.of(auction));

        BazaarQuickStatus quickStatus = new BazaarQuickStatus(10.0, 9.0, 100, 90, 1000, 900, 4, 3);
        BazaarProduct bazaarProduct = new BazaarProduct("ENCHANTED_DIAMOND", quickStatus, List.of(), List.of());
        BazaarResponse staleCachedBazaar = new BazaarResponse(true, 9_000L, Map.of("ENCHANTED_DIAMOND", bazaarProduct));
        BazaarResponse freshBazaar = new BazaarResponse(true, 11_000L, Map.of("ENCHANTED_DIAMOND", bazaarProduct));

        when(client.probeAuctionsLastUpdated()).thenAnswer(invocation -> {
            nowMillis.addAndGet(40L);
            return new AuctionProbeInfo(
                    auctionResponse.getLastUpdated(),
                    auctionResponse.getTotalPages(),
                    auctionResponse.getTotalAuctions()
            );
        });
        when(client.fetchAllAuctionPages(org.mockito.ArgumentMatchers.any())).thenAnswer(invocation -> {
            java.util.function.Consumer<Auction> consumer = invocation.getArgument(0);
            auctionResponse.getAuctions().forEach(consumer);
            return new AuctionProbeInfo(
                    auctionResponse.getLastUpdated(),
                    auctionResponse.getTotalPages(),
                    auctionResponse.getTotalAuctions()
            );
        });
        when(client.fetchBazaar()).thenReturn(freshBazaar);
        when(persistenceService.save(org.mockito.ArgumentMatchers.any(MarketSnapshot.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        ReflectionTestUtils.setField(service, "cachedBazaarResponse", staleCachedBazaar);
        ReflectionTestUtils.setField(service, "nextBazaarFetchAtMillis", nowMillis.get() + 20L);

        service.captureCurrentSnapshotAndPrepareInput().orElseThrow();

        verify(client, times(1)).fetchBazaar();
    }

    @Test
    void captureCurrentSnapshotAndPrepareInputDoesNotRegressSourceWatermarks() {
        HypixelClient client = mock(HypixelClient.class);
        HypixelMarketSnapshotMapper snapshotMapper = new HypixelMarketSnapshotMapper();
        MarketSnapshotPersistenceService persistenceService = mock(MarketSnapshotPersistenceService.class);
        UnifiedFlipInputMapper inputMapper = new UnifiedFlipInputMapper();
        AtomicLong nowMillis = new AtomicLong(2_000_000L);
        MarketDataProcessingService service = createService(
                client,
                snapshotMapper,
                persistenceService,
                inputMapper,
                createCycleInstrumentationService(),
                Duration.ofMillis(1),
                Duration.ofMillis(1),
                1L,
                Duration.ofMillis(1),
                nowMillis::get
        );

        Auction auction = new Auction(
                "a-1", "auctioneer", "profile", List.of(), 1L, 2L,
                "ENCHANTED_DIAMOND", "lore", "{\"internalname\":\"ENCHANTED_DIAMOND\"}", "misc", "RARE",
                100L, false, List.of(), 120L, List.of()
        );
        auction.setBin(true);
        AuctionResponse freshAuction = new AuctionResponse(true, 0, 1, 1, 10_000L, List.of(auction));
        AuctionResponse staleAuction = new AuctionResponse(true, 0, 1, 1, 9_000L, List.of(auction));

        BazaarQuickStatus quickStatus = new BazaarQuickStatus(10.0, 9.0, 100, 90, 1000, 900, 4, 3);
        BazaarProduct bazaarProduct = new BazaarProduct("ENCHANTED_DIAMOND", quickStatus, List.of(), List.of());
        BazaarResponse freshBazaar = new BazaarResponse(true, 11_000L, Map.of("ENCHANTED_DIAMOND", bazaarProduct));
        BazaarResponse staleBazaar = new BazaarResponse(true, 10_500L, Map.of("ENCHANTED_DIAMOND", bazaarProduct));

        when(client.probeAuctionsLastUpdated())
                .thenReturn(
                        new AuctionProbeInfo(freshAuction.getLastUpdated(), freshAuction.getTotalPages(), freshAuction.getTotalAuctions()),
                        new AuctionProbeInfo(staleAuction.getLastUpdated(), staleAuction.getTotalPages(), staleAuction.getTotalAuctions())
                );
        when(client.fetchAllAuctionPages(org.mockito.ArgumentMatchers.any())).thenAnswer(invocation -> {
            java.util.function.Consumer<Auction> consumer = invocation.getArgument(0);
            freshAuction.getAuctions().forEach(consumer);
            return new AuctionProbeInfo(
                    freshAuction.getLastUpdated(),
                    freshAuction.getTotalPages(),
                    freshAuction.getTotalAuctions()
            );
        });
        when(client.fetchBazaar()).thenReturn(freshBazaar, staleBazaar);
        when(persistenceService.save(org.mockito.ArgumentMatchers.any(MarketSnapshot.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        service.captureCurrentSnapshotAndPrepareInput().orElseThrow();
        nowMillis.addAndGet(5L);
        service.captureCurrentSnapshotAndPrepareInput().orElseThrow();

        long auctionWatermark = (long) ReflectionTestUtils.getField(service, "lastAuctionLastUpdated");
        long bazaarWatermark = (long) ReflectionTestUtils.getField(service, "lastBazaarLastUpdated");
        AuctionResponse cachedAuction = (AuctionResponse) ReflectionTestUtils.getField(service, "cachedAuctionResponse");
        BazaarResponse cachedBazaar = (BazaarResponse) ReflectionTestUtils.getField(service, "cachedBazaarResponse");
        assertEquals(10_000L, auctionWatermark);
        assertEquals(11_000L, bazaarWatermark);
        assertEquals(10_000L, cachedAuction.getLastUpdated());
        assertEquals(11_000L, cachedBazaar.getLastUpdated());
    }

    @Test
    void captureCurrentSnapshotAndPrepareInputKeepsRawSnapshotWhenAggregateWriteFails() {
        HypixelClient client = mock(HypixelClient.class);
        HypixelMarketSnapshotMapper snapshotMapper = new HypixelMarketSnapshotMapper();
        MarketSnapshotPersistenceService persistenceService = mock(MarketSnapshotPersistenceService.class);
        UnifiedFlipInputMapper inputMapper = new UnifiedFlipInputMapper();
        AhItemSnapshotRepository ahRepo = mock(AhItemSnapshotRepository.class);
        BzItemSnapshotRepository bzRepo = mock(BzItemSnapshotRepository.class);
        AhSnapshotAggregator ahAggregator = mock(AhSnapshotAggregator.class);
        BzSnapshotAggregator bzAggregator = mock(BzSnapshotAggregator.class);
        MarketSnapshotStorageProperties storageProperties = new MarketSnapshotStorageProperties();
        storageProperties.setPersistRawMarketSnapshot(true);
        storageProperties.setPersistAhAggregates(true);
        storageProperties.setPersistBzAggregates(true);
        AtomicLong nowMillis = new AtomicLong(2_100_000L);

        MarketDataProcessingService service = createService(
                client,
                snapshotMapper,
                persistenceService,
                inputMapper,
                ahRepo,
                bzRepo,
                ahAggregator,
                bzAggregator,
                storageProperties,
                Duration.ofSeconds(60),
                Duration.ofSeconds(20),
                2L,
                Duration.ofSeconds(10),
                nowMillis::get
        );

        AuctionResponse auctionResponse = auctionResponse(10_000L);
        BazaarResponse bazaarResponse = bazaarResponse(11_000L);
        mockAuctionStreaming(client, auctionResponse);
        when(client.fetchBazaar()).thenReturn(bazaarResponse);
        when(persistenceService.save(any(MarketSnapshot.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(ahAggregator.aggregateFromAuctions(any(Instant.class), anyList()))
                .thenReturn(List.of(new AhItemSnapshotEntity(11_000L, "ENCHANTED_DIAMOND|T:RARE|C:MISC|P:-|S:0|R:0", 100L, 100L, 100L, 100L, 1, null, 0)));
        when(ahRepo.insertIgnoreBatch(anyList())).thenThrow(new RuntimeException("aggregate failure"));
        when(bzAggregator.aggregate(any(Instant.class), anyMap()))
                .thenReturn(List.of(new BzItemSnapshotEntity(11_000L, "ENCHANTED_DIAMOND", 10.0, 9.0, 100L, 90L)));
        when(bzRepo.insertIgnoreBatch(anyList())).thenReturn(new int[]{1});

        UnifiedFlipInputSnapshot input = service.captureCurrentSnapshotAndPrepareInput().orElseThrow();

        assertNotNull(input);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<AhItemSnapshotEntity>> ahBatchCaptor = ArgumentCaptor.forClass(List.class);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<BzItemSnapshotEntity>> bzBatchCaptor = ArgumentCaptor.forClass(List.class);
        verify(ahRepo, times(1)).insertIgnoreBatch(ahBatchCaptor.capture());
        verify(bzRepo, times(1)).insertIgnoreBatch(bzBatchCaptor.capture());
        assertEquals(1, ahBatchCaptor.getValue().size());
        assertEquals(11_000L, ahBatchCaptor.getValue().getFirst().getSnapshotTs());
        assertEquals("ENCHANTED_DIAMOND|T:RARE|C:MISC|P:-|S:0|R:0", ahBatchCaptor.getValue().getFirst().getItemKey());
        assertEquals(1, bzBatchCaptor.getValue().size());
        assertEquals(11_000L, bzBatchCaptor.getValue().getFirst().getSnapshotTs());
        assertEquals("ENCHANTED_DIAMOND", bzBatchCaptor.getValue().getFirst().getProductId());
        verify(persistenceService, times(1)).save(any(MarketSnapshot.class));
    }

    @Test
    void captureCurrentSnapshotAndPrepareInputPersistsNothingWhenAllStorageTogglesAreDisabled() {
        HypixelClient client = mock(HypixelClient.class);
        HypixelMarketSnapshotMapper snapshotMapper = new HypixelMarketSnapshotMapper();
        MarketSnapshotPersistenceService persistenceService = mock(MarketSnapshotPersistenceService.class);
        UnifiedFlipInputMapper inputMapper = new UnifiedFlipInputMapper();
        AhItemSnapshotRepository ahRepo = mock(AhItemSnapshotRepository.class);
        BzItemSnapshotRepository bzRepo = mock(BzItemSnapshotRepository.class);
        AhSnapshotAggregator ahAggregator = mock(AhSnapshotAggregator.class);
        BzSnapshotAggregator bzAggregator = mock(BzSnapshotAggregator.class);
        MarketSnapshotStorageProperties storageProperties = new MarketSnapshotStorageProperties();
        storageProperties.setPersistRawMarketSnapshot(false);
        storageProperties.setPersistAhAggregates(false);
        storageProperties.setPersistBzAggregates(false);

        MarketDataProcessingService service = createService(
                client,
                snapshotMapper,
                persistenceService,
                inputMapper,
                ahRepo,
                bzRepo,
                ahAggregator,
                bzAggregator,
                storageProperties,
                Duration.ofSeconds(60),
                Duration.ofSeconds(20),
                2L,
                Duration.ofSeconds(10),
                System::currentTimeMillis
        );

        mockAuctionStreaming(client, auctionResponse(10_000L));
        when(client.fetchBazaar()).thenReturn(bazaarResponse(11_000L));

        UnifiedFlipInputSnapshot input = service.captureCurrentSnapshotAndPrepareInput().orElseThrow();

        assertNotNull(input);
        verify(ahRepo, never()).insertIgnoreBatch(anyList());
        verify(bzRepo, never()).insertIgnoreBatch(anyList());
        verify(persistenceService, never()).save(any(MarketSnapshot.class));
        verifyNoInteractions(ahAggregator, bzAggregator);
    }

    @Test
    void ingestBazaarPayloadAndPersistReturnsTimestampWithoutBuildingFlipInput() {
        HypixelClient client = mock(HypixelClient.class);
        HypixelMarketSnapshotMapper snapshotMapper = new HypixelMarketSnapshotMapper();
        MarketSnapshotPersistenceService persistenceService = mock(MarketSnapshotPersistenceService.class);
        UnifiedFlipInputMapper inputMapper = mock(UnifiedFlipInputMapper.class);
        MarketDataProcessingService service = new MarketDataProcessingService(client, snapshotMapper, persistenceService, inputMapper);

        AuctionResponse auctionResponse = auctionResponse(10_000L);
        BazaarResponse bazaarResponse = bazaarResponse(11_000L);
        when(persistenceService.save(any(MarketSnapshot.class))).thenAnswer(invocation -> invocation.getArgument(0));

        service.ingestAuctionPayloadAndPersist(auctionResponse, "adaptive-auctions");
        Instant snapshotTimestamp = service.ingestBazaarPayloadAndPersist(bazaarResponse, "adaptive-bazaar").orElseThrow();

        assertEquals(Instant.ofEpochMilli(11_000L), snapshotTimestamp);
        verify(persistenceService, times(1)).save(any(MarketSnapshot.class));
        verifyNoInteractions(inputMapper);
    }

    private AuctionResponse auctionResponse(long updatedAt) {
        Auction auction = new Auction(
                "a-1", "auctioneer", "profile", List.of(), 1L, 2L,
                "ENCHANTED_DIAMOND", "lore", "{\"internalname\":\"ENCHANTED_DIAMOND\"}", "misc", "RARE",
                100L, false, List.of(), 120L, List.of()
        );
        auction.setBin(true);
        return new AuctionResponse(true, 0, 1, 1, updatedAt, List.of(auction));
    }

    private BazaarResponse bazaarResponse(long updatedAt) {
        BazaarQuickStatus quickStatus = new BazaarQuickStatus(10.0, 9.0, 100, 90, 1000, 900, 4, 3);
        BazaarProduct bazaarProduct = new BazaarProduct("ENCHANTED_DIAMOND", quickStatus, List.of(), List.of());
        return new BazaarResponse(true, updatedAt, Map.of("ENCHANTED_DIAMOND", bazaarProduct));
    }

    private static MarketDataProcessingService createService(
            HypixelClient client,
            HypixelMarketSnapshotMapper snapshotMapper,
            MarketSnapshotPersistenceService persistenceService,
            UnifiedFlipInputMapper inputMapper,
            CycleInstrumentationService instrumentation,
            Duration auctionBaseInterval,
            Duration bazaarBaseInterval,
            long maxIntervalMultiplier,
            Duration retryInterval,
            LongSupplier nowSupplier
    ) {
        return new MarketDataProcessingService(
                client,
                snapshotMapper,
                persistenceService,
                inputMapper,
                instrumentation,
                auctionBaseInterval,
                bazaarBaseInterval,
                maxIntervalMultiplier,
                retryInterval,
                nowSupplier
        );
    }

    private static MarketDataProcessingService createService(
            HypixelClient client,
            HypixelMarketSnapshotMapper snapshotMapper,
            MarketSnapshotPersistenceService persistenceService,
            UnifiedFlipInputMapper inputMapper,
            AhItemSnapshotRepository ahRepo,
            BzItemSnapshotRepository bzRepo,
            AhSnapshotAggregator ahAggregator,
            BzSnapshotAggregator bzAggregator,
            MarketSnapshotStorageProperties storageProperties,
            Duration auctionBaseInterval,
            Duration bazaarBaseInterval,
            long maxIntervalMultiplier,
            Duration retryInterval,
            LongSupplier nowSupplier
    ) {
        return new MarketDataProcessingService(
                client,
                snapshotMapper,
                persistenceService,
                inputMapper,
                ahRepo,
                bzRepo,
                ahAggregator,
                bzAggregator,
                storageProperties,
                createCycleInstrumentationService(),
                auctionBaseInterval,
                bazaarBaseInterval,
                maxIntervalMultiplier,
                retryInterval,
                nowSupplier
        );
    }

    private static CycleInstrumentationService createCycleInstrumentationService() {
        return new CycleInstrumentationService(
                new SimpleMeterRegistry(),
                new BlockingTimeTracker(new InstrumentationProperties())
        );
    }

    private static void mockAuctionStreaming(HypixelClient client, AuctionResponse response) {
        when(client.probeAuctionsLastUpdated()).thenReturn(new AuctionProbeInfo(
                response.getLastUpdated(),
                response.getTotalPages(),
                response.getTotalAuctions()
        ));
        when(client.fetchAllAuctionPages(org.mockito.ArgumentMatchers.any())).thenAnswer(invocation -> {
            java.util.function.Consumer<Auction> consumer = invocation.getArgument(0);
            response.getAuctions().forEach(consumer);
            return new AuctionProbeInfo(
                    response.getLastUpdated(),
                    response.getTotalPages(),
                    response.getTotalAuctions()
            );
        });
    }
}
