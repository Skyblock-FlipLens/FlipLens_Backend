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
import java.util.function.LongSupplier;

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
    private final LongSupplier nowSupplier;
    private final Object pollStateLock = new Object();

    private AuctionResponse cachedAuctionResponse;
    private BazaarResponse cachedBazaarResponse;
    private long nextAuctionFetchAtMillis;
    private long nextBazaarFetchAtMillis;
    private long auctionCurrentIntervalMillis;
    private long bazaarCurrentIntervalMillis;
    private long lastAuctionLastUpdated = -1L;
    private long lastBazaarLastUpdated = -1L;
    private boolean auctionRefreshInFlight;
    private boolean bazaarRefreshInFlight;

    public MarketDataProcessingService(HypixelClient hypixelClient,
                                       HypixelMarketSnapshotMapper marketSnapshotMapper,
                                       MarketSnapshotPersistenceService marketSnapshotPersistenceService,
                                       UnifiedFlipInputMapper unifiedFlipInputMapper) {
        this(
                hypixelClient,
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
                DEFAULT_RETRY_INTERVAL,
                System::currentTimeMillis
        );
    }

    public MarketDataProcessingService(HypixelClient hypixelClient,
                                       HypixelMarketSnapshotMapper marketSnapshotMapper,
                                       MarketSnapshotPersistenceService marketSnapshotPersistenceService,
                                       UnifiedFlipInputMapper unifiedFlipInputMapper,
                                       LongSupplier nowSupplier) {
        this(
                hypixelClient,
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
                DEFAULT_RETRY_INTERVAL,
                nowSupplier
        );
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
        this(hypixelClient,
                marketSnapshotMapper,
                marketSnapshotPersistenceService,
                unifiedFlipInputMapper,
                cycleInstrumentationService,
                auctionBaseInterval,
                bazaarBaseInterval,
                maxIntervalMultiplier,
                retryInterval,
                System::currentTimeMillis);
    }

    public MarketDataProcessingService(HypixelClient hypixelClient,
                                       HypixelMarketSnapshotMapper marketSnapshotMapper,
                                       MarketSnapshotPersistenceService marketSnapshotPersistenceService,
                                       UnifiedFlipInputMapper unifiedFlipInputMapper,
                                       CycleInstrumentationService cycleInstrumentationService,
                                       Duration auctionBaseInterval,
                                       Duration bazaarBaseInterval,
                                       long maxIntervalMultiplier,
                                       Duration retryInterval,
                                       LongSupplier nowSupplier) {
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
        this.nowSupplier = nowSupplier == null ? System::currentTimeMillis : nowSupplier;
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
        AuctionResponse auctionSnapshot;
        BazaarResponse bazaarSnapshot;
        long payloadBytes;
        synchronized (pollStateLock) {
            cachedAuctionResponse = auctionResponse;
            if (auctionResponse != null && auctionResponse.getLastUpdated() > 0L) {
                lastAuctionLastUpdated = Math.max(lastAuctionLastUpdated, auctionResponse.getLastUpdated());
            }
            auctionSnapshot = cachedAuctionResponse;
            bazaarSnapshot = cachedBazaarResponse;
            payloadBytes = estimatePayload(auctionSnapshot, bazaarSnapshot);
        }
        return mapAndPersistSnapshot(cycleId, auctionSnapshot, bazaarSnapshot, payloadBytes);
    }

    public Optional<UnifiedFlipInputSnapshot> ingestBazaarPayload(BazaarResponse bazaarResponse, String cycleId) {
        AuctionResponse auctionSnapshot;
        BazaarResponse bazaarSnapshot;
        long payloadBytes;
        synchronized (pollStateLock) {
            cachedBazaarResponse = bazaarResponse;
            if (bazaarResponse != null && bazaarResponse.getLastUpdated() > 0L) {
                lastBazaarLastUpdated = Math.max(lastBazaarLastUpdated, bazaarResponse.getLastUpdated());
            }
            auctionSnapshot = cachedAuctionResponse;
            bazaarSnapshot = cachedBazaarResponse;
            payloadBytes = estimatePayload(auctionSnapshot, bazaarSnapshot);
        }
        return mapAndPersistSnapshot(cycleId, auctionSnapshot, bazaarSnapshot, payloadBytes);
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
            log.info("Skipping snapshot build due to partial data: auctions={}, bazaar={} cycleId={}",
                    auctionResponse != null,
                    bazaarResponse != null,
                    cycleId);
            return Optional.empty();
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

    public boolean hasBazaarPayload() {
        synchronized (pollStateLock) {
            return cachedBazaarResponse != null;
        }
    }

    public Optional<MarketSnapshot> marketSnapshotAsOfSecondsAgo(long secondsAgo) {
        long boundedSecondsAgo = Math.max(0L, secondsAgo);
        long nowMillis = nowSupplier.getAsLong();
        return marketSnapshotPersistenceService.asOf(java.time.Instant.ofEpochMilli(nowMillis).minusSeconds(boundedSecondsAgo));
    }

    public MarketSnapshotPersistenceService.SnapshotCompactionResult compactSnapshots() {
        return marketSnapshotPersistenceService.compactSnapshots();
    }

    private PollPayload pollPayload() {
        maybeRefreshAuctions(nowSupplier.getAsLong());
        maybeRefreshBazaar(nowSupplier.getAsLong());
        synchronized (pollStateLock) {
            return new PollPayload(cachedAuctionResponse, cachedBazaarResponse);
        }
    }

    private void maybeRefreshAuctions(long now) {
        boolean shouldFetch;
        synchronized (pollStateLock) {
            shouldFetch = (cachedAuctionResponse == null || now >= nextAuctionFetchAtMillis) && !auctionRefreshInFlight;
            if (shouldFetch) {
                auctionRefreshInFlight = true;
            }
        }
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
        synchronized (pollStateLock) {
            try {
                long decisionNow = nowSupplier.getAsLong();
                if (fetched == null) {
                    auctionCurrentIntervalMillis = growInterval(auctionCurrentIntervalMillis, auctionBaseIntervalMillis, auctionMaxIntervalMillis);
                    nextAuctionFetchAtMillis = decisionNow + Math.min(retryIntervalMillis, auctionCurrentIntervalMillis);
                    return;
                }

                long fetchedLastUpdated = fetched.getLastUpdated();
                boolean accepted = fetchedLastUpdated > 0L && fetchedLastUpdated >= lastAuctionLastUpdated;
                boolean advanced = accepted && fetchedLastUpdated > lastAuctionLastUpdated;
                if (accepted) {
                    cachedAuctionResponse = fetched;
                    if (advanced) {
                        lastAuctionLastUpdated = fetchedLastUpdated;
                        auctionCurrentIntervalMillis = auctionBaseIntervalMillis;
                    } else {
                        auctionCurrentIntervalMillis = growInterval(
                                auctionCurrentIntervalMillis,
                                auctionBaseIntervalMillis,
                                auctionMaxIntervalMillis
                        );
                    }
                } else {
                    auctionCurrentIntervalMillis = growInterval(
                            auctionCurrentIntervalMillis,
                            auctionBaseIntervalMillis,
                            auctionMaxIntervalMillis
                    );
                }
                nextAuctionFetchAtMillis = decisionNow + auctionCurrentIntervalMillis;
            } finally {
                auctionRefreshInFlight = false;
            }
        }
    }

    private void maybeRefreshBazaar(long now) {
        boolean shouldFetch;
        synchronized (pollStateLock) {
            shouldFetch = (cachedBazaarResponse == null || now >= nextBazaarFetchAtMillis) && !bazaarRefreshInFlight;
            if (shouldFetch) {
                bazaarRefreshInFlight = true;
            }
        }
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
        synchronized (pollStateLock) {
            try {
                long decisionNow = nowSupplier.getAsLong();
                if (fetched == null) {
                    bazaarCurrentIntervalMillis = growInterval(bazaarCurrentIntervalMillis, bazaarBaseIntervalMillis, bazaarMaxIntervalMillis);
                    nextBazaarFetchAtMillis = decisionNow + Math.min(retryIntervalMillis, bazaarCurrentIntervalMillis);
                    return;
                }

                long fetchedLastUpdated = fetched.getLastUpdated();
                boolean accepted = fetchedLastUpdated > 0L && fetchedLastUpdated >= lastBazaarLastUpdated;
                boolean advanced = accepted && fetchedLastUpdated > lastBazaarLastUpdated;
                if (accepted) {
                    cachedBazaarResponse = fetched;
                    if (advanced) {
                        lastBazaarLastUpdated = fetchedLastUpdated;
                        bazaarCurrentIntervalMillis = bazaarBaseIntervalMillis;
                    } else {
                        bazaarCurrentIntervalMillis = growInterval(
                                bazaarCurrentIntervalMillis,
                                bazaarBaseIntervalMillis,
                                bazaarMaxIntervalMillis
                        );
                    }
                } else {
                    bazaarCurrentIntervalMillis = growInterval(
                            bazaarCurrentIntervalMillis,
                            bazaarBaseIntervalMillis,
                            bazaarMaxIntervalMillis
                    );
                }
                nextBazaarFetchAtMillis = decisionNow + bazaarCurrentIntervalMillis;
            } finally {
                bazaarRefreshInFlight = false;
            }
        }
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
