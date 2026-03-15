package com.skyblockflipper.backend.service.market.polling;

import com.skyblockflipper.backend.config.properties.AdaptivePollingProperties;
import com.skyblockflipper.backend.hypixel.model.Auction;
import com.skyblockflipper.backend.hypixel.model.AuctionResponse;
import com.skyblockflipper.backend.hypixel.model.BazaarProduct;
import com.skyblockflipper.backend.hypixel.model.BazaarQuickStatus;
import com.skyblockflipper.backend.hypixel.model.BazaarResponse;
import com.skyblockflipper.backend.instrumentation.CycleInstrumentationService;
import com.skyblockflipper.backend.service.flipping.FlipGenerationService;
import com.skyblockflipper.backend.service.item.NeuRepoIngestionService;
import com.skyblockflipper.backend.service.market.MarketDataProcessingService;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.scheduling.TaskScheduler;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AdaptivePollingCoordinatorTest {

    @Test
    void startWhenDisabledDoesNotSchedule() {
        Fixture fixture = fixture(false);

        fixture.coordinator.start();

        verify(fixture.taskScheduler, never()).schedule(any(Runnable.class), any(Instant.class));
    }

    @Test
    void startWhenEnabledSchedulesImmediateAttempt() {
        Fixture fixture = fixture(true);

        fixture.coordinator.start();

        verify(fixture.taskScheduler).schedule(any(Runnable.class), any(Instant.class));
    }

    @Test
    void scheduleStartAttemptSkipsWhenShutdownRequested() throws Exception {
        Fixture fixture = fixture(true);
        setField(fixture.coordinator, "shutdownRequested", true);

        invokePrivate(fixture.coordinator, "scheduleStartAttempt", new Class<?>[]{Instant.class}, Instant.now());

        verify(fixture.taskScheduler, never()).schedule(any(Runnable.class), any(Instant.class));
    }

    @Test
    void scheduleStartAttemptSkipsWhenAlreadyScheduled() throws Exception {
        Fixture fixture = fixture(true);
        AtomicBoolean startScheduled = getAtomicBooleanField(fixture.coordinator, "startScheduledOrRunning");
        startScheduled.set(true);

        invokePrivate(fixture.coordinator, "scheduleStartAttempt", new Class<?>[]{Instant.class}, Instant.now());

        verify(fixture.taskScheduler, never()).schedule(any(Runnable.class), any(Instant.class));
    }

    @Test
    void scheduleStartAttemptResetsGuardWhenSchedulerThrows() throws Exception {
        Fixture fixture = fixture(true);
        when(fixture.taskScheduler.schedule(any(Runnable.class), any(Instant.class)))
                .thenThrow(new RuntimeException("scheduler down"));

        invokePrivate(fixture.coordinator, "scheduleStartAttempt", new Class<?>[]{Instant.class}, Instant.now());

        AtomicBoolean startScheduled = getAtomicBooleanField(fixture.coordinator, "startScheduledOrRunning");
        assertFalse(startScheduled.get());
    }

    @Test
    void attemptStartInterruptedSchedulesRetryAndRestoresInterruptFlag() throws Exception {
        Fixture fixture = fixture(true);
        when(fixture.neuRepoIngestionService.ingestLatestFilteredItems()).thenThrow(new InterruptedException("stop"));

        invokePrivate(fixture.coordinator, "attemptStart");

        ArgumentCaptor<Instant> whenCaptor = ArgumentCaptor.forClass(Instant.class);
        verify(fixture.taskScheduler).schedule(any(Runnable.class), whenCaptor.capture());
        assertTrue(whenCaptor.getValue().isAfter(Instant.now().minusSeconds(2)));
        assertTrue(Thread.currentThread().isInterrupted());
        assertTrue(Thread.interrupted());
    }

    @Test
    void attemptStartFailureSchedulesRetry() throws Exception {
        Fixture fixture = fixture(true);
        when(fixture.neuRepoIngestionService.ingestLatestFilteredItems()).thenThrow(new RuntimeException("boom"));

        invokePrivate(fixture.coordinator, "attemptStart");

        verify(fixture.taskScheduler).schedule(any(Runnable.class), any(Instant.class));
    }

    @Test
    void stopShutsDownPollersAndSetsShutdownFlags() throws Exception {
        Fixture fixture = fixture(true);
        @SuppressWarnings("unchecked")
        AdaptivePoller<AuctionResponse> auctions = mock(AdaptivePoller.class);
        @SuppressWarnings("unchecked")
        AdaptivePoller<BazaarResponse> bazaar = mock(AdaptivePoller.class);
        setField(fixture.coordinator, "auctionsPoller", auctions);
        setField(fixture.coordinator, "bazaarPoller", bazaar);
        getAtomicBooleanField(fixture.coordinator, "auctionsStarted").set(true);

        fixture.coordinator.stop();

        verify(auctions).stop();
        verify(bazaar).stop();
        assertTrue((Boolean) getField(fixture.coordinator, "shutdownRequested"));
        assertFalse(getAtomicBooleanField(fixture.coordinator, "auctionsStarted").get());
    }

    @Test
    void startAuctionsIfBazaarReadySkipsWhenBazaarNotLoaded() throws Exception {
        Fixture fixture = fixture(true);
        @SuppressWarnings("unchecked")
        AdaptivePoller<AuctionResponse> auctions = mock(AdaptivePoller.class);
        setField(fixture.coordinator, "auctionsPoller", auctions);
        when(fixture.marketDataProcessingService.hasBazaarPayload()).thenReturn(false);

        invokePrivate(fixture.coordinator, "startAuctionsIfBazaarReady");

        verify(auctions, never()).start();
    }

    @Test
    void startAuctionsIfBazaarReadyStartsOnlyOnce() throws Exception {
        Fixture fixture = fixture(true);
        @SuppressWarnings("unchecked")
        AdaptivePoller<AuctionResponse> auctions = mock(AdaptivePoller.class);
        setField(fixture.coordinator, "auctionsPoller", auctions);
        when(fixture.marketDataProcessingService.hasBazaarPayload()).thenReturn(true);

        invokePrivate(fixture.coordinator, "startAuctionsIfBazaarReady");
        invokePrivate(fixture.coordinator, "startAuctionsIfBazaarReady");

        verify(auctions, times(1)).start();
        assertTrue(getAtomicBooleanField(fixture.coordinator, "auctionsStarted").get());
    }

    @Test
    void processUpdateSuccessTracksCycleAndCounter() throws Exception {
        Fixture fixture = fixture(true);
        when(fixture.cycleInstrumentationService.startPhase()).thenReturn(7L);

        invokePrivate(
                fixture.coordinator,
                "processUpdate",
                new Class<?>[]{String.class, long.class, Runnable.class},
                "bazaar",
                123L,
                (Runnable) () -> {
                }
        );

        verify(fixture.cycleInstrumentationService).startCycle();
        verify(fixture.cycleInstrumentationService).endPhase(anyString(), any(Long.class), org.mockito.ArgumentMatchers.eq(true), org.mockito.ArgumentMatchers.eq(123L));
        verify(fixture.cycleInstrumentationService).finishCycle(true);
        assertEquals(
                1.0d,
                fixture.meterRegistry.get("skyblock.adaptive.processed_updates")
                        .tag("endpoint", "bazaar")
                        .counter()
                        .count()
        );
    }

    @Test
    void processUpdateFailureStillFinishesCycleAndCounter() throws Exception {
        Fixture fixture = fixture(true);
        when(fixture.cycleInstrumentationService.startPhase()).thenReturn(11L);

        invokePrivate(
                fixture.coordinator,
                "processUpdate",
                new Class<?>[]{String.class, long.class, Runnable.class},
                "auctions",
                321L,
                (Runnable) () -> {
                    throw new RuntimeException("processor failed");
                }
        );

        verify(fixture.cycleInstrumentationService).finishCycle(false);
        assertEquals(
                1.0d,
                fixture.meterRegistry.get("skyblock.adaptive.processed_updates")
                        .tag("endpoint", "auctions")
                        .counter()
                        .count()
        );
    }

    @Test
    void enqueueFlipGenerationWithNullSnapshotSkipsScheduling() throws Exception {
        Fixture fixture = fixture(true);

        invokePrivate(
                fixture.coordinator,
                "enqueueFlipGeneration",
                new Class<?>[]{Instant.class, String.class},
                null,
                "bazaar"
        );

        verify(fixture.taskScheduler, never()).schedule(any(Runnable.class), any(Instant.class));
    }

    @Test
    void enqueueFlipGenerationSchedulesOnlyFirstWorkerAndKeepsLatestSnapshot() throws Exception {
        Fixture fixture = fixture(true);
        Instant first = Instant.parse("2026-02-27T10:00:00Z");
        Instant second = first.plusSeconds(60);

        invokePrivate(
                fixture.coordinator,
                "enqueueFlipGeneration",
                new Class<?>[]{Instant.class, String.class},
                first,
                "bazaar"
        );
        invokePrivate(
                fixture.coordinator,
                "enqueueFlipGeneration",
                new Class<?>[]{Instant.class, String.class},
                second,
                "bazaar"
        );

        verify(fixture.taskScheduler, times(1)).schedule(any(Runnable.class), any(Instant.class));
        AtomicLong latest = getAtomicLongField(fixture.coordinator, "latestSnapshotEpochMillisToGenerate");
        assertEquals(second.toEpochMilli(), latest.get());
        assertEquals(
                2.0d,
                fixture.meterRegistry.get("skyblock.adaptive.flip_generation_enqueued")
                        .tag("endpoint", "bazaar")
                        .counter()
                        .count()
        );
    }

    @Test
    void drainFlipGenerationQueueRunsGenerationAndIncrementsSuccessMetric() throws Exception {
        Fixture fixture = fixture(true);
        Instant snapshotTs = Instant.parse("2026-02-27T11:00:00Z");
        getAtomicLongField(fixture.coordinator, "latestSnapshotEpochMillisToGenerate").set(snapshotTs.toEpochMilli());
        getAtomicBooleanField(fixture.coordinator, "generationWorkerScheduledOrRunning").set(true);

        invokePrivate(fixture.coordinator, "drainFlipGenerationQueue");

        verify(fixture.flipGenerationService).generateIfMissingForSnapshot(snapshotTs);
        assertEquals(
                1.0d,
                fixture.meterRegistry.get("skyblock.adaptive.flip_generation_runs")
                        .tag("status", "success")
                        .counter()
                        .count()
        );
        assertFalse(getAtomicBooleanField(fixture.coordinator, "generationWorkerScheduledOrRunning").get());
    }

    @Test
    void drainFlipGenerationQueueCountsErrorsWhenGenerationThrows() throws Exception {
        Fixture fixture = fixture(true);
        Instant snapshotTs = Instant.parse("2026-02-27T11:10:00Z");
        getAtomicLongField(fixture.coordinator, "latestSnapshotEpochMillisToGenerate").set(snapshotTs.toEpochMilli());
        getAtomicBooleanField(fixture.coordinator, "generationWorkerScheduledOrRunning").set(true);
        doThrow(new RuntimeException("generation error"))
                .when(fixture.flipGenerationService)
                .generateIfMissingForSnapshot(snapshotTs);

        invokePrivate(fixture.coordinator, "drainFlipGenerationQueue");

        assertEquals(
                1.0d,
                fixture.meterRegistry.get("skyblock.adaptive.flip_generation_runs")
                        .tag("status", "error")
                        .counter()
                        .count()
        );
    }

    @Test
    void scheduleGenerationDrainClearsWorkerWhenShutdownRequested() throws Exception {
        Fixture fixture = fixture(true);
        setField(fixture.coordinator, "shutdownRequested", true);
        getAtomicBooleanField(fixture.coordinator, "generationWorkerScheduledOrRunning").set(true);

        invokePrivate(
                fixture.coordinator,
                "scheduleGenerationDrain",
                new Class<?>[]{String.class, int.class},
                "test",
                0
        );

        assertFalse(getAtomicBooleanField(fixture.coordinator, "generationWorkerScheduledOrRunning").get());
        verify(fixture.taskScheduler, never()).schedule(any(Runnable.class), any(Instant.class));
    }

    @Test
    void scheduleGenerationDrainRetriesAfterSchedulerFailure() throws Exception {
        Fixture fixture = fixture(true);
        getAtomicBooleanField(fixture.coordinator, "generationWorkerScheduledOrRunning").set(true);
        when(fixture.taskScheduler.schedule(any(Runnable.class), any(Instant.class)))
                .thenThrow(new RuntimeException("scheduler unavailable"))
                .thenReturn(null);

        invokePrivate(
                fixture.coordinator,
                "scheduleGenerationDrain",
                new Class<?>[]{String.class, int.class},
                "enqueue",
                0
        );

        verify(fixture.taskScheduler, times(2)).schedule(any(Runnable.class), any(Instant.class));
        assertEquals(
                1.0d,
                fixture.meterRegistry.get("skyblock.adaptive.flip_generation_schedule_failures")
                        .counter()
                        .count()
        );
    }

    @Test
    void calculateRetryDelayUsesExponentialBackoff() throws Exception {
        Fixture fixture = fixture(true);

        long delay0 = (long) invokePrivate(
                fixture.coordinator,
                "calculateRetryDelayMillis",
                new Class<?>[]{int.class},
                0
        );
        long delay1 = (long) invokePrivate(
                fixture.coordinator,
                "calculateRetryDelayMillis",
                new Class<?>[]{int.class},
                1
        );
        long delay3 = (long) invokePrivate(
                fixture.coordinator,
                "calculateRetryDelayMillis",
                new Class<?>[]{int.class},
                3
        );

        assertEquals(0L, delay0);
        assertEquals(250L, delay1);
        assertEquals(1_000L, delay3);
    }

    @Test
    void hashAuctionsProbeReturnsNullForNullResponse() throws Exception {
        Fixture fixture = fixture(true);

        Object hash = invokePrivate(
                fixture.coordinator,
                "hashAuctionsProbe",
                new Class<?>[]{AuctionResponse.class},
                new Object[]{null}
        );

        assertNull(hash);
    }

    @Test
    void hashAuctionsProbeIgnoresItemsBeyondFirstForty() throws Exception {
        Fixture fixture = fixture(true);
        AuctionResponse responseA = new AuctionResponse(true, 0, 3, 41, 123L, auctions(41, "base", 10_000L));
        AuctionResponse responseB = new AuctionResponse(true, 0, 3, 41, 123L, auctions(41, "base", 10_000L));
        responseB.getAuctions().get(40).setHighestBidAmount(9_999_999L);

        String hashA = (String) invokePrivate(
                fixture.coordinator,
                "hashAuctionsProbe",
                new Class<?>[]{AuctionResponse.class},
                responseA
        );
        String hashB = (String) invokePrivate(
                fixture.coordinator,
                "hashAuctionsProbe",
                new Class<?>[]{AuctionResponse.class},
                responseB
        );

        assertEquals(hashA, hashB);
    }

    @Test
    void hashBazaarUsesSortedKeysAndSkipsNullQuickStatus() throws Exception {
        Fixture fixture = fixture(true);
        BazaarResponse first = new BazaarResponse(true, 100L, bazaarProducts("A", "B"));
        BazaarResponse second = new BazaarResponse(true, 100L, bazaarProducts("B", "A"));

        String hashFirst = (String) invokePrivate(
                fixture.coordinator,
                "hashBazaar",
                new Class<?>[]{BazaarResponse.class},
                first
        );
        String hashSecond = (String) invokePrivate(
                fixture.coordinator,
                "hashBazaar",
                new Class<?>[]{BazaarResponse.class},
                second
        );

        assertEquals(hashFirst, hashSecond);

        second.getProducts().put("C", new BazaarProduct("C", null, null, null));
        String hashWithNullQuickStatus = (String) invokePrivate(
                fixture.coordinator,
                "hashBazaar",
                new Class<?>[]{BazaarResponse.class},
                second
        );
        assertEquals(hashFirst, hashWithNullQuickStatus);
    }

    @Test
    void estimatePayloadBytesHandlesNullsAndSizes() throws Exception {
        Fixture fixture = fixture(true);
        AuctionResponse auctions = new AuctionResponse(true, 0, 1, 2, 1L, auctions(2, "u", 1_000L));
        BazaarResponse bazaar = new BazaarResponse(true, 1L, bazaarProducts("A", "B", "C"));

        long auctionBytes = (long) invokePrivate(
                fixture.coordinator,
                "estimateAuctionBytes",
                new Class<?>[]{AuctionResponse.class},
                auctions
        );
        long bazaarBytes = (long) invokePrivate(
                fixture.coordinator,
                "estimateBazaarBytes",
                new Class<?>[]{BazaarResponse.class},
                bazaar
        );
        long nullAuctionBytes = (long) invokePrivate(
                fixture.coordinator,
                "estimateAuctionBytes",
                new Class<?>[]{AuctionResponse.class},
                new Object[]{null}
        );
        long nullBazaarBytes = (long) invokePrivate(
                fixture.coordinator,
                "estimateBazaarBytes",
                new Class<?>[]{BazaarResponse.class},
                new Object[]{null}
        );

        assertEquals(640L, auctionBytes);
        assertEquals(660L, bazaarBytes);
        assertEquals(0L, nullAuctionBytes);
        assertEquals(0L, nullBazaarBytes);
    }

    @Test
    void hashAuctionsProbeChangesWhenFirstFortyEntriesChange() throws Exception {
        Fixture fixture = fixture(true);
        AuctionResponse responseA = new AuctionResponse(true, 0, 2, 40, 777L, auctions(40, "id", 50_000L));
        AuctionResponse responseB = new AuctionResponse(true, 0, 2, 40, 777L, auctions(40, "id", 50_000L));
        responseB.getAuctions().get(10).setHighestBidAmount(123_456_789L);

        String hashA = (String) invokePrivate(
                fixture.coordinator,
                "hashAuctionsProbe",
                new Class<?>[]{AuctionResponse.class},
                responseA
        );
        String hashB = (String) invokePrivate(
                fixture.coordinator,
                "hashAuctionsProbe",
                new Class<?>[]{AuctionResponse.class},
                responseB
        );

        assertNotEquals(hashA, hashB);
    }

    @Test
    void startAuctionsAtBootstrapStartsPollerOnlyOnce() throws Exception {
        Fixture fixture = fixture(true);
        @SuppressWarnings("unchecked")
        AdaptivePoller<AuctionResponse> auctions = mock(AdaptivePoller.class);
        setField(fixture.coordinator, "auctionsPoller", auctions);

        invokePrivate(fixture.coordinator, "startAuctionsAtBootstrap");
        invokePrivate(fixture.coordinator, "startAuctionsAtBootstrap");

        verify(auctions, times(1)).start();
        assertTrue(getAtomicBooleanField(fixture.coordinator, "auctionsStarted").get());
    }

    @Test
    void updatePredictionStateResetsBackoffAndAdvancesExpectedTimes() throws Exception {
        Fixture fixture = fixture(true);
        AdaptivePollingProperties.Endpoint endpoint = AdaptivePollingProperties.Endpoint.defaults("auctions", "/auctions", java.time.Duration.ofSeconds(20));
        endpoint.setMinProbeIntervalMs(1_000L);
        endpoint.setJitterMs(0L);

        AuctionPollState auctionPollState = (AuctionPollState) getField(fixture.coordinator, "auctionPollState");
        auctionPollState.setLastSeenLastUpdated(10_000L);
        auctionPollState.setEwmaPeriodMs(4_000L);
        auctionPollState.setProbeBackoffStep(4);
        auctionPollState.setProbeIntervalMs(8_000L);
        invokePrivate(
                fixture.coordinator,
                "updateAuctionPredictionState",
                new Class<?>[]{long.class, long.class, AdaptivePollingProperties.Endpoint.class},
                13_000L,
                20_000L,
                endpoint
        );

        assertEquals(13_000L, auctionPollState.getLastSeenLastUpdated());
        assertEquals(0, auctionPollState.getProbeBackoffStep());
        assertEquals(1_000L, auctionPollState.getProbeIntervalMs());
        assertTrue(auctionPollState.getNextExpectedAtMs() >= 21_000L);

        BazaarPollState bazaarPollState = (BazaarPollState) getField(fixture.coordinator, "bazaarPollState");
        bazaarPollState.setLastSeenLastUpdated(5_000L);
        bazaarPollState.setEwmaPeriodMs(2_000L);
        bazaarPollState.setProbeBackoffStep(3);
        bazaarPollState.setProbeIntervalMs(6_000L);
        invokePrivate(
                fixture.coordinator,
                "updateBazaarPredictionState",
                new Class<?>[]{long.class, long.class, AdaptivePollingProperties.Endpoint.class},
                7_500L,
                10_000L,
                endpoint
        );

        assertEquals(7_500L, bazaarPollState.getLastSeenLastUpdated());
        assertEquals(0, bazaarPollState.getProbeBackoffStep());
        assertEquals(1_000L, bazaarPollState.getProbeIntervalMs());
        assertTrue(bazaarPollState.getNextExpectedAtMs() >= 11_000L);
    }

    @Test
    void updateProbeBackoffResetsNearExpectedWindowAndIncreasesWhenLate() throws Exception {
        Fixture fixture = fixture(true);
        AdaptivePollingProperties.Endpoint endpoint = AdaptivePollingProperties.Endpoint.defaults("bazaar", "/bazaar", java.time.Duration.ofSeconds(60));
        endpoint.setMinProbeIntervalMs(1_000L);
        endpoint.setMaxProbeIntervalMs(8_000L);
        endpoint.setGraceWindowMs(500L);
        endpoint.setJitterMs(0L);

        long resetDelay = (long) invokePrivate(
                fixture.coordinator,
                "updateProbeBackoff",
                new Class<?>[]{long.class, long.class, AdaptivePollingProperties.Endpoint.class, boolean.class, int.class, long.class, boolean.class},
                10_000L,
                10_200L,
                endpoint,
                true,
                3,
                4_000L,
                true
        );
        assertEquals(1_000L, resetDelay);

        long increasedDelay = (long) invokePrivate(
                fixture.coordinator,
                "updateProbeBackoff",
                new Class<?>[]{long.class, long.class, AdaptivePollingProperties.Endpoint.class, boolean.class, int.class, long.class, boolean.class},
                20_000L,
                10_000L,
                endpoint,
                false,
                1,
                2_000L,
                false
        );
        assertEquals(8_000L, increasedDelay);
    }

    @Test
    void sleepDelayAndJitterHelpersHandleEdgeCases() throws Exception {
        Fixture fixture = fixture(true);

        long noExpected = (long) invokePrivate(
                fixture.coordinator,
                "computeSleepDelay",
                new Class<?>[]{long.class, long.class, long.class, long.class},
                10_000L,
                0L,
                500L,
                0L
        );
        long probeNow = (long) invokePrivate(
                fixture.coordinator,
                "computeSleepDelay",
                new Class<?>[]{long.class, long.class, long.class, long.class},
                10_000L,
                10_200L,
                500L,
                0L
        );
        long clamped = (long) invokePrivate(
                fixture.coordinator,
                "applyJitter",
                new Class<?>[]{long.class, long.class},
                -10L,
                100L
        );

        assertEquals(0L, noExpected);
        assertEquals(0L, probeNow);
        assertEquals(0L, clamped);
    }

    @Test
    void toMinimalAuctionKeepsOnlyRelevantFields() throws Exception {
        Fixture fixture = fixture(true);
        Auction source = new Auction(
                "uuid-1",
                "auctioneer",
                "profile",
                List.of("coop"),
                1_000L,
                2_000L,
                "Aspect of the Dragons",
                "lore",
                "extra",
                "weapon",
                "LEGENDARY",
                5_000_000L,
                false,
                true,
                List.of("bidder"),
                5_500_000L,
                List.of()
        );

        Auction minimal = (Auction) invokePrivate(
                fixture.coordinator,
                "toMinimalAuction",
                new Class<?>[]{Auction.class},
                source
        );

        assertEquals("uuid-1", minimal.getUuid());
        assertEquals("Aspect of the Dragons", minimal.getItemName());
        assertEquals("weapon", minimal.getCategory());
        assertEquals("LEGENDARY", minimal.getTier());
        assertEquals(5_000_000L, minimal.getStartingBid());
        assertEquals(5_500_000L, minimal.getHighestBidAmount());
        assertNull(minimal.getItemLore());
        assertTrue(minimal.getCoop() == null || minimal.getCoop().isEmpty());
    }

    private static Fixture fixture(boolean enabled) {
        AdaptivePollingProperties properties = new AdaptivePollingProperties();
        properties.setEnabled(enabled);
        TaskScheduler taskScheduler = mock(TaskScheduler.class);
        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        MarketDataProcessingService marketDataProcessingService = mock(MarketDataProcessingService.class);
        FlipGenerationService flipGenerationService = mock(FlipGenerationService.class);
        CycleInstrumentationService cycleInstrumentationService = mock(CycleInstrumentationService.class);
        NeuRepoIngestionService neuRepoIngestionService = mock(NeuRepoIngestionService.class);
        AdaptivePollingCoordinator coordinator = new AdaptivePollingCoordinator(
                properties,
                taskScheduler,
                meterRegistry,
                marketDataProcessingService,
                flipGenerationService,
                cycleInstrumentationService,
                neuRepoIngestionService,
                "https://api.hypixel.net",
                "test-key"
        );
        return new Fixture(
                coordinator,
                taskScheduler,
                meterRegistry,
                marketDataProcessingService,
                flipGenerationService,
                cycleInstrumentationService,
                neuRepoIngestionService
        );
    }

    private static List<Auction> auctions(int count, String uuidPrefix, long bidBase) {
        List<Auction> auctions = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            auctions.add(new Auction(
                    uuidPrefix + "-" + i,
                    "auctioneer",
                    "profile",
                    List.of(),
                    1_000L + i,
                    2_000L + i,
                    "item-" + i,
                    "lore",
                    null,
                    "misc",
                    "COMMON",
                    1L,
                    false,
                    true,
                    List.of(),
                    bidBase + i,
                    List.of()
            ));
        }
        return auctions;
    }

    private static Map<String, BazaarProduct> bazaarProducts(String... keys) {
        Map<String, BazaarProduct> products = new HashMap<>();
        for (String key : keys) {
            BazaarQuickStatus quickStatus = new BazaarQuickStatus(10.0d, 9.0d, 100L, 120L, 300L, 350L, 4, 5);
            products.put(key, new BazaarProduct(key, quickStatus, List.of(), List.of()));
        }
        return products;
    }

    private static Object invokePrivate(Object target, String methodName) throws Exception {
        return invokePrivate(target, methodName, new Class<?>[0]);
    }

    private static Object invokePrivate(Object target, String methodName, Class<?>[] parameterTypes, Object... args) throws Exception {
        Method method = target.getClass().getDeclaredMethod(methodName, parameterTypes);
        method.setAccessible(true);
        return method.invoke(target, args);
    }

    private static Object getField(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static AtomicBoolean getAtomicBooleanField(Object target, String fieldName) throws Exception {
        return (AtomicBoolean) getField(target, fieldName);
    }

    private static AtomicLong getAtomicLongField(Object target, String fieldName) throws Exception {
        return (AtomicLong) getField(target, fieldName);
    }

    private record Fixture(AdaptivePollingCoordinator coordinator,
                           TaskScheduler taskScheduler,
                           MeterRegistry meterRegistry,
                           MarketDataProcessingService marketDataProcessingService,
                           FlipGenerationService flipGenerationService,
                           CycleInstrumentationService cycleInstrumentationService,
                           NeuRepoIngestionService neuRepoIngestionService) {
    }
}
