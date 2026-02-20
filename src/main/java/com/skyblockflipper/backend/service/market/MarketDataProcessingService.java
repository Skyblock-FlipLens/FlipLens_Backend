package com.skyblockflipper.backend.service.market;

import com.skyblockflipper.backend.hypixel.HypixelClient;
import com.skyblockflipper.backend.hypixel.HypixelMarketSnapshotMapper;
import com.skyblockflipper.backend.hypixel.model.AuctionResponse;
import com.skyblockflipper.backend.hypixel.model.BazaarResponse;
import com.skyblockflipper.backend.instrumentation.CycleInstrumentationService;
import com.skyblockflipper.backend.model.market.MarketSnapshot;
import com.skyblockflipper.backend.service.flipping.UnifiedFlipInputMapper;
import com.skyblockflipper.backend.model.market.UnifiedFlipInputSnapshot;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Optional;

@Service
@Slf4j
public class MarketDataProcessingService {

    private static final Duration DEFAULT_AUCTION_BASE_INTERVAL = Duration.ofSeconds(60);
    private static final Duration DEFAULT_BAZAAR_BASE_INTERVAL = Duration.ofSeconds(20);
    private static final Duration DEFAULT_RETRY_INTERVAL = Duration.ofSeconds(10);
    private static final long DEFAULT_MAX_INTERVAL_MULTIPLIER = 2L;
    private static final long MIN_ADAPTIVE_STEP_MILLIS = 5_000L;

    private final HypixelClient hypixelClient;
    private final HypixelMarketSnapshotMapper marketSnapshotMapper;
    private final MarketSnapshotPersistenceService marketSnapshotPersistenceService;
    private final UnifiedFlipInputMapper unifiedFlipInputMapper;
    private final CycleInstrumentationService cycleInstrumentationService;
    private final long auctionBaseIntervalMillis;
    private final long bazaarBaseIntervalMillis;
    private final long auctionMaxIntervalMillis;
    private final long bazaarMaxIntervalMillis;
    private final long retryIntervalMillis;
    private final Object pollStateLock = new Object();

    private AuctionResponse cachedAuctionResponse;
    private BazaarResponse cachedBazaarResponse;
    private long nextAuctionFetchAtMillis;
    private long nextBazaarFetchAtMillis;
    private long auctionCurrentIntervalMillis;
    private long bazaarCurrentIntervalMillis;
    private long lastAuctionLastUpdated = -1L;
    private long lastBazaarLastUpdated = -1L;

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
                                new com.skyblockflipper.backend.instrumentation.InstrumentationProperties())),
                DEFAULT_AUCTION_BASE_INTERVAL,
                DEFAULT_BAZAAR_BASE_INTERVAL,
                DEFAULT_MAX_INTERVAL_MULTIPLIER,
                DEFAULT_RETRY_INTERVAL);
    }

    @Autowired
    public MarketDataProcessingService(HypixelClient hypixelClient,
                                       HypixelMarketSnapshotMapper marketSnapshotMapper,
                                       MarketSnapshotPersistenceService marketSnapshotPersistenceService,
                                       UnifiedFlipInputMapper unifiedFlipInputMapper,
                                       CycleInstrumentationService cycleInstrumentationService,
                                       @Value("${config.hypixel.polling.auctions-base-interval:PT60S}") Duration auctionBaseInterval,
                                       @Value("${config.hypixel.polling.bazaar-base-interval:PT20S}") Duration bazaarBaseInterval,
                                       @Value("${config.hypixel.polling.max-interval-multiplier:2}") long maxIntervalMultiplier,
                                       @Value("${config.hypixel.polling.retry-interval:PT10S}") Duration retryInterval) {
        this.hypixelClient = hypixelClient;
        this.marketSnapshotMapper = marketSnapshotMapper;
        this.marketSnapshotPersistenceService = marketSnapshotPersistenceService;
        this.unifiedFlipInputMapper = unifiedFlipInputMapper;
        this.cycleInstrumentationService = cycleInstrumentationService;
        this.auctionBaseIntervalMillis = sanitizeDuration(auctionBaseInterval, DEFAULT_AUCTION_BASE_INTERVAL);
        this.bazaarBaseIntervalMillis = sanitizeDuration(bazaarBaseInterval, DEFAULT_BAZAAR_BASE_INTERVAL);
        long safeMultiplier = Math.max(1L, maxIntervalMultiplier);
        this.auctionMaxIntervalMillis = this.auctionBaseIntervalMillis * safeMultiplier;
        this.bazaarMaxIntervalMillis = this.bazaarBaseIntervalMillis * safeMultiplier;
        this.retryIntervalMillis = sanitizeDuration(retryInterval, DEFAULT_RETRY_INTERVAL);
        this.auctionCurrentIntervalMillis = this.auctionBaseIntervalMillis;
        this.bazaarCurrentIntervalMillis = this.bazaarBaseIntervalMillis;
    }

    private static long sanitizeDuration(Duration configured, Duration fallback) {
        Duration safeDuration = configured == null || configured.isNegative() || configured.isZero() ? fallback : configured;
        return safeDuration.toMillis();
    }

    public Optional<UnifiedFlipInputSnapshot> captureCurrentSnapshotAndPrepareInput() {
        return captureCurrentSnapshotAndPrepareInput("manual");
    }

    public Optional<UnifiedFlipInputSnapshot> captureCurrentSnapshotAndPrepareInput(String cycleId) {
        long pullHttpStart = cycleInstrumentationService.startPhase();
        PollPayload payload = pollPayload();
        AuctionResponse auctionResponse = payload.auctionResponse();
        BazaarResponse bazaarResponse = payload.bazaarResponse();
        boolean hasAnyPayload = auctionResponse != null || bazaarResponse != null;
        long payloadBytes = estimatePayload(auctionResponse, bazaarResponse);
        cycleInstrumentationService.endPhase("pull_http", pullHttpStart, hasAnyPayload, payloadBytes);

        return mapAndPersistSnapshot(cycleId, auctionResponse, bazaarResponse, payloadBytes);
    }

    public Optional<UnifiedFlipInputSnapshot> ingestAuctionPayload(AuctionResponse auctionResponse, String cycleId) {
        synchronized (pollStateLock) {
            cachedAuctionResponse = auctionResponse;
            if (auctionResponse != null && auctionResponse.getLastUpdated() > 0L) {
                lastAuctionLastUpdated = Math.max(lastAuctionLastUpdated, auctionResponse.getLastUpdated());
            }
            long payloadBytes = estimatePayload(auctionResponse, cachedBazaarResponse);
            return mapAndPersistSnapshot(cycleId, cachedAuctionResponse, cachedBazaarResponse, payloadBytes);
        }
    }

    public Optional<UnifiedFlipInputSnapshot> ingestBazaarPayload(BazaarResponse bazaarResponse, String cycleId) {
        synchronized (pollStateLock) {
            cachedBazaarResponse = bazaarResponse;
            if (bazaarResponse != null && bazaarResponse.getLastUpdated() > 0L) {
                lastBazaarLastUpdated = Math.max(lastBazaarLastUpdated, bazaarResponse.getLastUpdated());
            }
            long payloadBytes = estimatePayload(cachedAuctionResponse, bazaarResponse);
            return mapAndPersistSnapshot(cycleId, cachedAuctionResponse, cachedBazaarResponse, payloadBytes);
        }
    }

    private Optional<UnifiedFlipInputSnapshot> mapAndPersistSnapshot(String cycleId,
                                                                     AuctionResponse auctionResponse,
                                                                     BazaarResponse bazaarResponse,
                                                                     long payloadBytes) {
        boolean hasAnyPayload = auctionResponse != null || bazaarResponse != null;
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

    private PollPayload pollPayload() {
        synchronized (pollStateLock) {
            long now = System.currentTimeMillis();
            maybeRefreshAuctions(now);
            maybeRefreshBazaar(now);
            return new PollPayload(cachedAuctionResponse, cachedBazaarResponse);
        }
    }

    private void maybeRefreshAuctions(long now) {
        boolean shouldFetch = cachedAuctionResponse == null || now >= nextAuctionFetchAtMillis;
        if (!shouldFetch) {
            return;
        }

        AuctionResponse fetched;
        try {
            fetched = hypixelClient.fetchAllAuctionPages();
        } catch (RuntimeException e) {
            log.warn("Auction refresh failed, keeping cached payload: {}", e.getMessage());
            fetched = null;
        }
        if (fetched == null) {
            auctionCurrentIntervalMillis = growInterval(auctionCurrentIntervalMillis, auctionBaseIntervalMillis, auctionMaxIntervalMillis);
            nextAuctionFetchAtMillis = now + Math.min(retryIntervalMillis, auctionCurrentIntervalMillis);
            return;
        }

        cachedAuctionResponse = fetched;
        long fetchedLastUpdated = fetched.getLastUpdated();
        boolean advanced = fetchedLastUpdated > 0L && fetchedLastUpdated > lastAuctionLastUpdated;
        auctionCurrentIntervalMillis = advanced
                ? auctionBaseIntervalMillis
                : growInterval(auctionCurrentIntervalMillis, auctionBaseIntervalMillis, auctionMaxIntervalMillis);
        if (fetchedLastUpdated > 0L) {
            lastAuctionLastUpdated = fetchedLastUpdated;
        }
        nextAuctionFetchAtMillis = now + auctionCurrentIntervalMillis;
    }

    private void maybeRefreshBazaar(long now) {
        boolean shouldFetch = cachedBazaarResponse == null || now >= nextBazaarFetchAtMillis;
        if (!shouldFetch) {
            return;
        }

        BazaarResponse fetched;
        try {
            fetched = hypixelClient.fetchBazaar();
        } catch (RuntimeException e) {
            log.warn("Bazaar refresh failed, keeping cached payload: {}", e.getMessage());
            fetched = null;
        }
        if (fetched == null) {
            bazaarCurrentIntervalMillis = growInterval(bazaarCurrentIntervalMillis, bazaarBaseIntervalMillis, bazaarMaxIntervalMillis);
            nextBazaarFetchAtMillis = now + Math.min(retryIntervalMillis, bazaarCurrentIntervalMillis);
            return;
        }

        cachedBazaarResponse = fetched;
        long fetchedLastUpdated = fetched.getLastUpdated();
        boolean advanced = fetchedLastUpdated > 0L && fetchedLastUpdated > lastBazaarLastUpdated;
        bazaarCurrentIntervalMillis = advanced
                ? bazaarBaseIntervalMillis
                : growInterval(bazaarCurrentIntervalMillis, bazaarBaseIntervalMillis, bazaarMaxIntervalMillis);
        if (fetchedLastUpdated > 0L) {
            lastBazaarLastUpdated = fetchedLastUpdated;
        }
        nextBazaarFetchAtMillis = now + bazaarCurrentIntervalMillis;
    }

    private long growInterval(long currentIntervalMillis, long baseIntervalMillis, long maxIntervalMillis) {
        long step = Math.max(MIN_ADAPTIVE_STEP_MILLIS, baseIntervalMillis / 2L);
        return Math.min(currentIntervalMillis + step, maxIntervalMillis);
    }

    private long estimatePayload(AuctionResponse auctionResponse, BazaarResponse bazaarResponse) {
        long auctionCount = auctionResponse == null || auctionResponse.getAuctions() == null ? 0L : auctionResponse.getAuctions().size();
        long bazaarCount = bazaarResponse == null || bazaarResponse.getProducts() == null ? 0L : bazaarResponse.getProducts().size();
        return (auctionCount * 300L) + (bazaarCount * 200L);
    }

    private record PollPayload(
            AuctionResponse auctionResponse,
            BazaarResponse bazaarResponse
    ) {
    }
}
