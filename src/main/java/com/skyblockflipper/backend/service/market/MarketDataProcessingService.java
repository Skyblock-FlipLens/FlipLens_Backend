package com.skyblockflipper.backend.service.market;

import com.skyblockflipper.backend.hypixel.AuctionProbeInfo;
import com.skyblockflipper.backend.hypixel.HypixelClient;
import com.skyblockflipper.backend.hypixel.HypixelMarketSnapshotMapper;
import com.skyblockflipper.backend.hypixel.model.Auction;
import com.skyblockflipper.backend.hypixel.model.AuctionResponse;
import com.skyblockflipper.backend.hypixel.model.BazaarResponse;
import com.skyblockflipper.backend.instrumentation.CycleInstrumentationService;
import com.skyblockflipper.backend.model.market.AhItemSnapshotEntity;
import com.skyblockflipper.backend.model.market.BzItemSnapshotEntity;
import com.skyblockflipper.backend.model.market.MarketSnapshot;
import com.skyblockflipper.backend.repository.AhItemSnapshotRepository;
import com.skyblockflipper.backend.repository.BzItemSnapshotRepository;
import com.skyblockflipper.backend.service.flipping.UnifiedFlipInputMapper;
import com.skyblockflipper.backend.model.market.UnifiedFlipInputSnapshot;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
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
    private static final int DEFAULT_AGGREGATE_INSERT_BATCH_SIZE = 500;

    private final HypixelClient hypixelClient;
    private final HypixelMarketSnapshotMapper marketSnapshotMapper;
    private final MarketSnapshotPersistenceService marketSnapshotPersistenceService;
    private final UnifiedFlipInputMapper unifiedFlipInputMapper;
    private final AhItemSnapshotRepository ahItemSnapshotRepository;
    private final BzItemSnapshotRepository bzItemSnapshotRepository;
    private final AhSnapshotAggregator ahSnapshotAggregator;
    private final BzSnapshotAggregator bzSnapshotAggregator;
    private final MarketSnapshotStorageProperties snapshotStorageProperties;
    private final CycleInstrumentationService cycleInstrumentationService;
    private final long auctionBaseIntervalMillis;
    private final long bazaarBaseIntervalMillis;
    private final long auctionMaxIntervalMillis;
    private final long bazaarMaxIntervalMillis;
    private final long retryIntervalMillis;
    private final LongSupplier nowSupplier;
    private final int aggregateInsertBatchSize;
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
                null,
                null,
                new AhSnapshotAggregator(new MarketItemKeyService()),
                new BzSnapshotAggregator(new MarketItemKeyService()),
                MarketSnapshotStorageProperties.rawOnlyDefaults(),
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
                null,
                null,
                new AhSnapshotAggregator(new MarketItemKeyService()),
                new BzSnapshotAggregator(new MarketItemKeyService()),
                MarketSnapshotStorageProperties.rawOnlyDefaults(),
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
                                       AhItemSnapshotRepository ahItemSnapshotRepository,
                                       BzItemSnapshotRepository bzItemSnapshotRepository,
                                       AhSnapshotAggregator ahSnapshotAggregator,
                                       BzSnapshotAggregator bzSnapshotAggregator,
                                       MarketSnapshotStorageProperties snapshotStorageProperties,
                                       CycleInstrumentationService cycleInstrumentationService,
                                       @Value("${config.hypixel.polling.auctions-base-interval:PT60S}") Duration auctionBaseInterval,
                                       @Value("${config.hypixel.polling.bazaar-base-interval:PT20S}") Duration bazaarBaseInterval,
                                       @Value("${config.hypixel.polling.max-interval-multiplier:2}") long maxIntervalMultiplier,
                                       @Value("${config.hypixel.polling.retry-interval:PT10S}") Duration retryInterval) {
        this(hypixelClient,
                marketSnapshotMapper,
                marketSnapshotPersistenceService,
                unifiedFlipInputMapper,
                ahItemSnapshotRepository,
                bzItemSnapshotRepository,
                ahSnapshotAggregator,
                bzSnapshotAggregator,
                snapshotStorageProperties,
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
        this(
                hypixelClient,
                marketSnapshotMapper,
                marketSnapshotPersistenceService,
                unifiedFlipInputMapper,
                null,
                null,
                new AhSnapshotAggregator(new MarketItemKeyService()),
                new BzSnapshotAggregator(new MarketItemKeyService()),
                MarketSnapshotStorageProperties.rawOnlyDefaults(),
                cycleInstrumentationService,
                auctionBaseInterval,
                bazaarBaseInterval,
                maxIntervalMultiplier,
                retryInterval,
                nowSupplier
        );
    }

    public MarketDataProcessingService(HypixelClient hypixelClient,
                                       HypixelMarketSnapshotMapper marketSnapshotMapper,
                                       MarketSnapshotPersistenceService marketSnapshotPersistenceService,
                                       UnifiedFlipInputMapper unifiedFlipInputMapper,
                                       AhItemSnapshotRepository ahItemSnapshotRepository,
                                       BzItemSnapshotRepository bzItemSnapshotRepository,
                                       AhSnapshotAggregator ahSnapshotAggregator,
                                       BzSnapshotAggregator bzSnapshotAggregator,
                                       MarketSnapshotStorageProperties snapshotStorageProperties,
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
        this.ahItemSnapshotRepository = ahItemSnapshotRepository;
        this.bzItemSnapshotRepository = bzItemSnapshotRepository;
        this.ahSnapshotAggregator = ahSnapshotAggregator == null
                ? new AhSnapshotAggregator(new MarketItemKeyService())
                : ahSnapshotAggregator;
        this.bzSnapshotAggregator = bzSnapshotAggregator == null
                ? new BzSnapshotAggregator(new MarketItemKeyService())
                : bzSnapshotAggregator;
        this.snapshotStorageProperties = snapshotStorageProperties == null
                ? MarketSnapshotStorageProperties.rawOnlyDefaults()
                : snapshotStorageProperties;
        this.cycleInstrumentationService = cycleInstrumentationService;
        this.auctionBaseIntervalMillis = sanitizeDuration(auctionBaseInterval, DEFAULT_AUCTION_BASE_INTERVAL);
        this.bazaarBaseIntervalMillis = sanitizeDuration(bazaarBaseInterval, DEFAULT_BAZAAR_BASE_INTERVAL);
        long safeMultiplier = Math.max(1L, maxIntervalMultiplier);
        this.auctionMaxIntervalMillis = this.auctionBaseIntervalMillis * safeMultiplier;
        this.bazaarMaxIntervalMillis = this.bazaarBaseIntervalMillis * safeMultiplier;
        this.retryIntervalMillis = sanitizeDuration(retryInterval, DEFAULT_RETRY_INTERVAL);
        this.nowSupplier = nowSupplier == null ? System::currentTimeMillis : nowSupplier;
        this.aggregateInsertBatchSize = sanitizeBatchSize(this.snapshotStorageProperties.getAggregateBatchSize());
        this.auctionCurrentIntervalMillis = this.auctionBaseIntervalMillis;
        this.bazaarCurrentIntervalMillis = this.bazaarBaseIntervalMillis;
        validateStorageConfiguration(this.snapshotStorageProperties);
    }

    private static long sanitizeDuration(Duration configured, Duration fallback) {
        Duration safeDuration = configured == null || configured.isNegative() || configured.isZero() ? fallback : configured;
        return safeDuration.toMillis();
    }

    private int sanitizeBatchSize(int configuredBatchSize) {
        if (configuredBatchSize <= 0) {
            log.warn("Invalid config.snapshot.storage.aggregate-batch-size='{}'; falling back to {}",
                    configuredBatchSize,
                    DEFAULT_AGGREGATE_INSERT_BATCH_SIZE);
            return DEFAULT_AGGREGATE_INSERT_BATCH_SIZE;
        }
        return configuredBatchSize;
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

        return mapAndPersistSnapshot(cycleId, auctionResponse, bazaarResponse, payloadBytes, true)
                .flatMap(result -> Optional.ofNullable(result.inputSnapshot()));
    }

    public Optional<UnifiedFlipInputSnapshot> ingestAuctionPayload(AuctionResponse auctionResponse, String cycleId) {
        return ingestAuctionPayloadInternal(auctionResponse, cycleId, true)
                .flatMap(result -> Optional.ofNullable(result.inputSnapshot()));
    }

    public Optional<Instant> ingestAuctionPayloadAndPersist(AuctionResponse auctionResponse, String cycleId) {
        return ingestAuctionPayloadInternal(auctionResponse, cycleId, false)
                .map(SnapshotProcessingResult::snapshotTimestamp);
    }

    private Optional<SnapshotProcessingResult> ingestAuctionPayloadInternal(AuctionResponse auctionResponse,
                                                                            String cycleId,
                                                                            boolean prepareFlipInput) {
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
        return mapAndPersistSnapshot(cycleId, auctionSnapshot, bazaarSnapshot, payloadBytes, prepareFlipInput);
    }

    public Optional<UnifiedFlipInputSnapshot> ingestBazaarPayload(BazaarResponse bazaarResponse, String cycleId) {
        return ingestBazaarPayloadInternal(bazaarResponse, cycleId, true)
                .flatMap(result -> Optional.ofNullable(result.inputSnapshot()));
    }

    public Optional<Instant> ingestBazaarPayloadAndPersist(BazaarResponse bazaarResponse, String cycleId) {
        return ingestBazaarPayloadInternal(bazaarResponse, cycleId, false)
                .map(SnapshotProcessingResult::snapshotTimestamp);
    }

    private Optional<SnapshotProcessingResult> ingestBazaarPayloadInternal(BazaarResponse bazaarResponse,
                                                                           String cycleId,
                                                                           boolean prepareFlipInput) {
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
        return mapAndPersistSnapshot(cycleId, auctionSnapshot, bazaarSnapshot, payloadBytes, prepareFlipInput);
    }

    private Optional<SnapshotProcessingResult> mapAndPersistSnapshot(String cycleId,
                                                                     AuctionResponse auctionResponse,
                                                                     BazaarResponse bazaarResponse,
                                                                     long payloadBytes,
                                                                     boolean prepareFlipInput) {
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

        long persistStart = cycleInstrumentationService.startPhase();
        try {
            persistAggregateSnapshots(snapshot, auctionResponse);
        } catch (RuntimeException e) {
            log.warn("Aggregate snapshot persistence failed but raw snapshot path will continue.", e);
        }
        if (snapshotStorageProperties.isPersistRawMarketSnapshot()) {
            marketSnapshotPersistenceService.save(snapshot);
        }
        cycleInstrumentationService.endPhase("persist/cache_update", persistStart, true, payloadBytes);

        if (!prepareFlipInput) {
            return Optional.of(new SnapshotProcessingResult(snapshot.snapshotTimestamp(), null));
        }

        long computeStart = cycleInstrumentationService.startPhase();
        UnifiedFlipInputSnapshot inputSnapshot = unifiedFlipInputMapper.map(snapshot);
        cycleInstrumentationService.endPhase("compute_flips", computeStart, true, payloadBytes);

        return Optional.of(new SnapshotProcessingResult(snapshot.snapshotTimestamp(), inputSnapshot));
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

    private void persistAggregateSnapshots(MarketSnapshot snapshot, AuctionResponse auctionResponse) {
        if (snapshot == null || snapshot.snapshotTimestamp() == null) {
            return;
        }
        if (snapshotStorageProperties.isPersistAhAggregates() && ahItemSnapshotRepository != null) {
            try {
                List<AhItemSnapshotEntity> ahAggregates;
                if (auctionResponse != null && auctionResponse.getAuctions() != null) {
                    ahAggregates = ahSnapshotAggregator.aggregateFromAuctions(snapshot.snapshotTimestamp(), auctionResponse.getAuctions());
                } else {
                    ahAggregates = ahSnapshotAggregator.aggregate(snapshot.snapshotTimestamp(), snapshot.auctions());
                }
                for (int fromIndex = 0; fromIndex < ahAggregates.size(); fromIndex += aggregateInsertBatchSize) {
                    int toIndex = Math.min(fromIndex + aggregateInsertBatchSize, ahAggregates.size());
                    ahItemSnapshotRepository.insertIgnoreBatch(ahAggregates.subList(fromIndex, toIndex));
                }
            } catch (RuntimeException e) {
                log.warn("Failed to persist AH aggregate snapshots for {}", snapshot.snapshotTimestamp(), e);
            }
        }
        if (snapshotStorageProperties.isPersistBzAggregates() && bzItemSnapshotRepository != null) {
            try {
                List<BzItemSnapshotEntity> bzAggregates = bzSnapshotAggregator.aggregate(snapshot.snapshotTimestamp(), snapshot.bazaarProducts());
                for (int fromIndex = 0; fromIndex < bzAggregates.size(); fromIndex += aggregateInsertBatchSize) {
                    int toIndex = Math.min(fromIndex + aggregateInsertBatchSize, bzAggregates.size());
                    bzItemSnapshotRepository.insertIgnoreBatch(bzAggregates.subList(fromIndex, toIndex));
                }
            } catch (RuntimeException e) {
                log.warn("Failed to persist Bazaar aggregate snapshots for {}", snapshot.snapshotTimestamp(), e);
            }
        }
    }

    private void validateStorageConfiguration(MarketSnapshotStorageProperties storageProperties) {
        MarketSnapshotStorageProperties safe = Objects.requireNonNull(storageProperties, "storageProperties must not be null");
        boolean rawEnabled = safe.isPersistRawMarketSnapshot();
        boolean ahEnabled = safe.isPersistAhAggregates();
        boolean bzEnabled = safe.isPersistBzAggregates();
        if (!rawEnabled && !ahEnabled && !bzEnabled) {
            log.warn(
                    "All snapshot persistence outputs are disabled (config.snapshot.storage.persist-raw-market-snapshot, "
                            + "config.snapshot.storage.persist-ah-aggregates, config.snapshot.storage.persist-bz-aggregates). "
                            + "No market snapshots will be persisted."
            );
        }
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

        AuctionProbeInfo probe;
        try {
            probe = hypixelClient.probeAuctionsLastUpdated();
        } catch (RuntimeException e) {
            log.warn("Auction probe failed, keeping cached payload: {}", e.getMessage());
            probe = null;
        }
        synchronized (pollStateLock) {
            try {
                long decisionNow = nowSupplier.getAsLong();
                if (probe == null) {
                    auctionCurrentIntervalMillis = growInterval(auctionCurrentIntervalMillis, auctionBaseIntervalMillis, auctionMaxIntervalMillis);
                    nextAuctionFetchAtMillis = decisionNow + Math.min(retryIntervalMillis, auctionCurrentIntervalMillis);
                    return;
                }

                long fetchedLastUpdated = probe.lastUpdated();
                boolean accepted = fetchedLastUpdated > 0L && fetchedLastUpdated >= lastAuctionLastUpdated;
                boolean advanced = accepted && fetchedLastUpdated > lastAuctionLastUpdated;
                if (accepted) {
                    if (advanced) {
                        AuctionResponse streamedSnapshot = fetchFilteredAuctionSnapshotStreaming(probe);
                        if (streamedSnapshot != null) {
                            cachedAuctionResponse = streamedSnapshot;
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

    private AuctionResponse fetchFilteredAuctionSnapshotStreaming(AuctionProbeInfo probe) {
        List<Auction> filteredAuctions = new ArrayList<>();
        try {
            AuctionProbeInfo scanned = hypixelClient.fetchAllAuctionPages(auction -> {
                if (auction == null || (!auction.isBin() && !auction.isClaimed())) {
                    return;
                }
                filteredAuctions.add(toMinimalAuction(auction));
            });
            long lastUpdated = scanned.lastUpdated() > 0L ? scanned.lastUpdated() : probe.lastUpdated();
            return new AuctionResponse(
                    true,
                    0,
                    scanned.totalPages(),
                    scanned.totalAuctions(),
                    lastUpdated,
                    filteredAuctions
            );
        } catch (RuntimeException e) {
            log.warn("Auction commit scan failed, keeping cached payload: {}", e.getMessage());
            return null;
        }
    }

    private Auction toMinimalAuction(Auction auction) {
        return new Auction(
                auction.getUuid(),
                null,
                null,
                List.of(),
                auction.getStart(),
                auction.getEnd(),
                auction.getItemName(),
                auction.getItemLore(),
                auction.getExtra(),
                auction.getCategory(),
                auction.getTier(),
                auction.getStartingBid(),
                auction.isClaimed(),
                auction.isBin(),
                List.of(),
                auction.getHighestBidAmount(),
                List.of()
        );
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

    private record SnapshotProcessingResult(
            Instant snapshotTimestamp,
            UnifiedFlipInputSnapshot inputSnapshot
    ) {
    }
}
