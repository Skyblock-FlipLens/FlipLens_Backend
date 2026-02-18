package com.skyblockflipper.backend.service.market;

import com.skyblockflipper.backend.hypixel.HypixelClient;
import com.skyblockflipper.backend.hypixel.HypixelMarketSnapshotMapper;
import com.skyblockflipper.backend.hypixel.model.AuctionResponse;
import com.skyblockflipper.backend.hypixel.model.BazaarResponse;
import com.skyblockflipper.backend.instrumentation.CycleInstrumentationService;
import com.skyblockflipper.backend.model.market.MarketSnapshot;
import com.skyblockflipper.backend.service.flipping.UnifiedFlipInputMapper;
import com.skyblockflipper.backend.model.market.UnifiedFlipInputSnapshot;
import lombok.extern.slf4j.Slf4j;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@Slf4j
public class MarketDataProcessingService {

    private final HypixelClient hypixelClient;
    private final HypixelMarketSnapshotMapper marketSnapshotMapper;
    private final MarketSnapshotPersistenceService marketSnapshotPersistenceService;
    private final UnifiedFlipInputMapper unifiedFlipInputMapper;
    private final CycleInstrumentationService cycleInstrumentationService;

    public MarketDataProcessingService(HypixelClient hypixelClient,
                                       HypixelMarketSnapshotMapper marketSnapshotMapper,
                                       MarketSnapshotPersistenceService marketSnapshotPersistenceService,
                                       UnifiedFlipInputMapper unifiedFlipInputMapper) {
        this(hypixelClient,
                marketSnapshotMapper,
                marketSnapshotPersistenceService,
                unifiedFlipInputMapper,
                new CycleInstrumentationService(
                        new SimpleMeterRegistry(),
                        new com.skyblockflipper.backend.instrumentation.BlockingTimeTracker(
                                new com.skyblockflipper.backend.instrumentation.InstrumentationProperties())));
    }

    @Autowired
    public MarketDataProcessingService(HypixelClient hypixelClient,
                                       HypixelMarketSnapshotMapper marketSnapshotMapper,
                                       MarketSnapshotPersistenceService marketSnapshotPersistenceService,
                                       UnifiedFlipInputMapper unifiedFlipInputMapper,
                                       CycleInstrumentationService cycleInstrumentationService) {
        this.hypixelClient = hypixelClient;
        this.marketSnapshotMapper = marketSnapshotMapper;
        this.marketSnapshotPersistenceService = marketSnapshotPersistenceService;
        this.unifiedFlipInputMapper = unifiedFlipInputMapper;
        this.cycleInstrumentationService = cycleInstrumentationService;
    }

    public Optional<UnifiedFlipInputSnapshot> captureCurrentSnapshotAndPrepareInput() {
        return captureCurrentSnapshotAndPrepareInput("manual");
    }

    public Optional<UnifiedFlipInputSnapshot> captureCurrentSnapshotAndPrepareInput(String cycleId) {
        long pullHttpStart = cycleInstrumentationService.startPhase();
        AuctionResponse auctionResponse = hypixelClient.fetchAllAuctionPages();
        BazaarResponse bazaarResponse = hypixelClient.fetchBazaar();
        long payloadBytes = estimatePayload(auctionResponse, bazaarResponse);
        cycleInstrumentationService.endPhase("pull_http", pullHttpStart, true, payloadBytes);

        long deserializeStart = cycleInstrumentationService.startPhase();
        boolean hasAnyPayload = auctionResponse != null || bazaarResponse != null;
        cycleInstrumentationService.endPhase("validate_response", deserializeStart, hasAnyPayload, payloadBytes);

        if (!hasAnyPayload) {
            log.warn("Both auction and bazaar responses are null, returning empty");
            return Optional.empty();
        }
        if (auctionResponse == null || bazaarResponse == null) {
            log.info("Partial data available: auctions={}, bazaar={} cycleId={}", auctionResponse != null, bazaarResponse != null, cycleId);
        }

        long normalizeStart = cycleInstrumentationService.startPhase();
        MarketSnapshot snapshot = marketSnapshotMapper.map(auctionResponse, bazaarResponse);
        cycleInstrumentationService.endPhase("normalize", normalizeStart, true, payloadBytes);

        long computeStart = cycleInstrumentationService.startPhase();
        UnifiedFlipInputSnapshot inputSnapshot = unifiedFlipInputMapper.map(snapshot);
        cycleInstrumentationService.endPhase("compute_flips", computeStart, true, payloadBytes);

        long persistStart = cycleInstrumentationService.startPhase();
        marketSnapshotPersistenceService.save(snapshot);
        cycleInstrumentationService.endPhase("persist/cache_update", persistStart, true, payloadBytes);

        return Optional.of(inputSnapshot);
    }

    public Optional<MarketSnapshot> latestMarketSnapshot() {
        return marketSnapshotPersistenceService.latest();
    }

    public Optional<MarketSnapshot> marketSnapshotAsOfSecondsAgo(long secondsAgo) {
        long boundedSecondsAgo = Math.max(0L, secondsAgo);
        return marketSnapshotPersistenceService.asOf(java.time.Instant.now().minusSeconds(boundedSecondsAgo));
    }

    public MarketSnapshotPersistenceService.SnapshotCompactionResult compactSnapshots() {
        return marketSnapshotPersistenceService.compactSnapshots();
    }

    private long estimatePayload(AuctionResponse auctionResponse, BazaarResponse bazaarResponse) {
        long auctionCount = auctionResponse == null || auctionResponse.getAuctions() == null ? 0L : auctionResponse.getAuctions().size();
        long bazaarCount = bazaarResponse == null || bazaarResponse.getProducts() == null ? 0L : bazaarResponse.getProducts().size();
        return (auctionCount * 300L) + (bazaarCount * 200L);
    }
}
