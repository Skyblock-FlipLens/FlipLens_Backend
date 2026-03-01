package com.skyblockflipper.backend.service.market.polling;

import com.skyblockflipper.backend.config.properties.AdaptivePollingProperties;
import com.skyblockflipper.backend.hypixel.HypixelConditionalClient;
import com.skyblockflipper.backend.hypixel.HypixelHttpResult;
import com.skyblockflipper.backend.hypixel.model.Auction;
import com.skyblockflipper.backend.hypixel.model.AuctionResponse;
import com.skyblockflipper.backend.hypixel.model.Auction.Bid;
import com.skyblockflipper.backend.hypixel.model.BazaarProduct;
import com.skyblockflipper.backend.hypixel.model.BazaarQuickStatus;
import com.skyblockflipper.backend.hypixel.model.BazaarResponse;
import com.skyblockflipper.backend.instrumentation.CycleInstrumentationService;
import com.skyblockflipper.backend.service.item.NeuRepoIngestionService;
import com.skyblockflipper.backend.service.flipping.FlipGenerationService;
import com.skyblockflipper.backend.service.market.MarketDataProcessingService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Component
@Profile("!compactor")
@Slf4j
public class AdaptivePollingCoordinator {
    private static final int MAX_GENERATION_SCHEDULE_RETRY_ATTEMPTS = 3;
    private static final Duration GENERATION_SCHEDULE_RETRY_BASE_DELAY = Duration.ofMillis(250);
    private static final Duration START_SCHEDULE_FAILURE_RETRY_DELAY = Duration.ofSeconds(1);
    private static final long AUCTIONS_GRACE_WINDOW_MS = 8_000L;
    private static final long AUCTIONS_MIN_PROBE_INTERVAL_MS = 1_000L;
    private static final long AUCTIONS_MAX_PROBE_INTERVAL_MS = 15_000L;
    private static final double AUCTIONS_EWMA_ALPHA = 0.25d;

    private final AdaptivePollingProperties adaptivePollingProperties;
    private final TaskScheduler taskScheduler;
    private final MeterRegistry meterRegistry;
    private final MarketDataProcessingService marketDataProcessingService;
    private final FlipGenerationService flipGenerationService;
    private final CycleInstrumentationService cycleInstrumentationService;
    private final NeuRepoIngestionService neuRepoIngestionService;
    private final String apiUrl;
    private final String apiKey;
    private final Counter flipGenerationScheduleFailureCounter;
    private final ExecutorService generationFallbackExecutor;
    private final AtomicBoolean startScheduledOrRunning = new AtomicBoolean(false);
    private final AtomicBoolean auctionsStarted = new AtomicBoolean(false);
    private final AtomicLong lastCommittedAuctionLastUpdated = new AtomicLong(-1L);
    private final AuctionPollState auctionPollState = new AuctionPollState();
    private final AtomicLong latestSnapshotEpochMillisToGenerate = new AtomicLong(-1L);
    private final AtomicBoolean generationWorkerScheduledOrRunning = new AtomicBoolean(false);
    private volatile boolean shutdownRequested = false;

    private AdaptivePoller<AuctionResponse> auctionsPoller;
    private AdaptivePoller<BazaarResponse> bazaarPoller;

    public AdaptivePollingCoordinator(AdaptivePollingProperties adaptivePollingProperties,
                                      TaskScheduler taskScheduler,
                                      MeterRegistry meterRegistry,
                                      MarketDataProcessingService marketDataProcessingService,
                                      FlipGenerationService flipGenerationService,
                                      CycleInstrumentationService cycleInstrumentationService,
                                      NeuRepoIngestionService neuRepoIngestionService,
                                      @Value("${config.hypixel.api-url}") String apiUrl,
                                      @Value("${config.hypixel.api-key:}") String apiKey) {
        this.adaptivePollingProperties = adaptivePollingProperties;
        this.taskScheduler = taskScheduler;
        this.meterRegistry = meterRegistry;
        this.marketDataProcessingService = marketDataProcessingService;
        this.flipGenerationService = flipGenerationService;
        this.cycleInstrumentationService = cycleInstrumentationService;
        this.neuRepoIngestionService = neuRepoIngestionService;
        this.apiUrl = apiUrl;
        this.apiKey = apiKey;
        this.flipGenerationScheduleFailureCounter = meterRegistry.counter("skyblock.adaptive.flip_generation_schedule_failures");
        this.generationFallbackExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread thread = new Thread(r, "adaptive-flip-generation-fallback");
            thread.setDaemon(true);
            return thread;
        });
    }

    @PostConstruct
    public void start() {
        if (!adaptivePollingProperties.isEnabled()) {
            log.info("Adaptive polling disabled via config.hypixel.adaptive.enabled=false");
            return;
        }
        scheduleStartAttemptNow();
    }

    private void scheduleStartAttemptNow() {
        scheduleStartAttempt(Instant.now());
    }

    private void scheduleStartAttempt(Instant when) {
        if (shutdownRequested) {
            return;
        }
        if (!startScheduledOrRunning.compareAndSet(false, true)) {
            return;
        }
        try {
            taskScheduler.schedule(this::attemptStart, when);
        } catch (RuntimeException e) {
            startScheduledOrRunning.compareAndSet(true, false);
            log.error("Failed to schedule adaptive polling start attempt at {}: {}", when, ExceptionUtils.getStackTrace(e));
            scheduleStartAttemptFallback(when);
        }
    }

    private void scheduleStartAttemptFallback(Instant requestedWhen) {
        generationFallbackExecutor.execute(() -> {
            if (shutdownRequested || auctionsPoller != null || bazaarPoller != null) {
                return;
            }
            try {
                Thread.sleep(START_SCHEDULE_FAILURE_RETRY_DELAY.toMillis());
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
                return;
            }
            Instant fallbackWhen = requestedWhen == null
                    ? Instant.now().plus(START_SCHEDULE_FAILURE_RETRY_DELAY)
                    : requestedWhen.isAfter(Instant.now()) ? requestedWhen : Instant.now().plus(START_SCHEDULE_FAILURE_RETRY_DELAY);
            scheduleStartAttempt(fallbackWhen);
        });
    }

    private void attemptStart() {
        Instant retryAt = null;
        try {
            if (shutdownRequested || auctionsPoller != null || bazaarPoller != null) {
                return;
            }
            int savedItems = neuRepoIngestionService.ingestLatestFilteredItems();
            log.info("Initial NEU ingestion complete (saved {} items). Starting bazaar poller first.", savedItems);
            GlobalRequestLimiter globalLimiter = new GlobalRequestLimiter(adaptivePollingProperties.getGlobalMaxRequestsPerSecond());
            auctionsPoller = buildAuctionsPoller(globalLimiter);
            bazaarPoller = buildBazaarPoller(globalLimiter);
            bazaarPoller.start();
            startAuctionsIfBazaarReady();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Adaptive polling start deferred: NEU ingestion interrupted. Retrying in 30s.");
            retryAt = Instant.now().plusSeconds(30);
        } catch (Exception e) {
            log.warn("Adaptive polling start deferred: NEU ingestion failed ({}). Retrying in 30s.", e.getMessage());
            retryAt = Instant.now().plusSeconds(30);
        } finally {
            startScheduledOrRunning.set(false);
        }
        if (retryAt != null) {
            scheduleStartAttempt(retryAt);
        }
    }

    @PreDestroy
    public void stop() {
        shutdownRequested = true;
        auctionsStarted.set(false);
        if (auctionsPoller != null) {
            auctionsPoller.stop();
        }
        if (bazaarPoller != null) {
            bazaarPoller.stop();
        }
        generationFallbackExecutor.shutdownNow();
    }

    private AdaptivePoller<AuctionResponse> buildAuctionsPoller(GlobalRequestLimiter globalLimiter) {
        AdaptivePollingProperties.Endpoint endpointCfg = adaptivePollingProperties.getAuctions();
        HypixelConditionalClient client = new HypixelConditionalClient(
                apiUrl,
                apiKey,
                endpointCfg.getConnectTimeout(),
                endpointCfg.getRequestTimeout()
        );
        ProcessingPipeline<AuctionResponse> processingPipeline = new ProcessingPipeline<>(
                endpointCfg.getName(),
                meterRegistry,
                adaptivePollingProperties.getPipeline().getQueueCapacity(),
                adaptivePollingProperties.getPipeline().isCoalesceEnabled(),
                this::processAuctionsUpdate
        );
        AdaptivePoller.PollExecutor<AuctionResponse> pollExecutor = detector -> {
            long nowMillis = System.currentTimeMillis();
            auctionPollState.setPhase(AuctionPollState.Phase.PROBE);
            ChangeDetector.ConditionalHeaders conditionalHeaders = detector.conditionalHeaders();
            HypixelHttpResult<AuctionResponse> probe = client.fetchAuctionPage(
                    endpointCfg.getPath(),
                    0,
                    conditionalHeaders.ifNoneMatch(),
                    conditionalHeaders.ifModifiedSince()
            );
            AuctionResponse probeBody = probe.body();
            if (adaptivePollingProperties.getDebug().isLogPhaseTransitions()) {
                log.info("Adaptive auctions phase=PROBE status={} transportError={}",
                        probe.statusCode(),
                        probe.transportError());
            }
            String probeHash = hashAuctionsProbe(probeBody);
            ChangeDetector.ChangeDecision decision = detector.evaluate(probe, probeHash);
            if (decision.isChanged() && probeBody != null && probeBody.isSuccess()) {
                long probeLastUpdated = probeBody.getLastUpdated();
                long lastCommitted = lastCommittedAuctionLastUpdated.get();
                if (adaptivePollingProperties.getDebug().isLogLastUpdated()) {
                    log.info("Adaptive auctions probe lastUpdated={} lastCommitted={} ewmaPeriodMs={} nextExpectedAtMs={}",
                            probeLastUpdated,
                            lastCommitted,
                            auctionPollState.getEwmaPeriodMs(),
                            auctionPollState.getNextExpectedAtMs());
                }
                if (probeLastUpdated > 0L && probeLastUpdated <= lastCommitted) {
                    updateAuctionProbeBackoff(nowMillis);
                    auctionPollState.setPhase(AuctionPollState.Phase.SLEEP);
                    return new AdaptivePoller.PollExecution<>(ChangeDetector.ChangeDecision.noChange(), null, probeLastUpdated, probe);
                }
            }
            if (!decision.isChanged()) {
                long changeTs = probeBody == null ? 0L : probeBody.getLastUpdated();
                updateAuctionProbeBackoff(nowMillis);
                auctionPollState.setPhase(AuctionPollState.Phase.SLEEP);
                return new AdaptivePoller.PollExecution<>(decision, null, changeTs, probe);
            }
            if (probeBody == null || !probeBody.isSuccess()) {
                auctionPollState.setPhase(AuctionPollState.Phase.SLEEP);
                return new AdaptivePoller.PollExecution<>(ChangeDetector.ChangeDecision.error(), null, 0L, probe);
            }
            auctionPollState.setPhase(AuctionPollState.Phase.COMMIT_IN_FLIGHT);
            long commitStartNanos = System.nanoTime();
            List<Auction> filteredAuctions = new ArrayList<>();
            HypixelHttpResult<HypixelConditionalClient.AuctionScanSummary> full = client.fetchAllAuctionPages(
                    endpointCfg.getPath(),
                    probeBody,
                    auction -> {
                        if (auction == null || !auction.isBin() || auction.isClaimed()) {
                            return;
                        }
                        filteredAuctions.add(toMinimalAuction(auction));
                    }
            );
            if (!full.isSuccessful() || full.body() == null) {
                auctionPollState.setPhase(AuctionPollState.Phase.SLEEP);
                return new AdaptivePoller.PollExecution<>(ChangeDetector.ChangeDecision.error(), null, 0L, full);
            }
            HypixelConditionalClient.AuctionScanSummary scanSummary = full.body();
            updateAuctionPredictionState(scanSummary.lastUpdated(), nowMillis);
            if (adaptivePollingProperties.getDebug().isLogCommitStats()) {
                long durationMillis = (System.nanoTime() - commitStartNanos) / 1_000_000L;
                log.info("Adaptive auctions commit lastUpdated={} totalPages={} pagesFetched={} auctionsSeen={} auctionsKept={} durationMs={}",
                        scanSummary.lastUpdated(),
                        scanSummary.totalPages(),
                        scanSummary.pagesFetched(),
                        scanSummary.auctionsSeen(),
                        filteredAuctions.size(),
                        durationMillis);
            }
            meterRegistry.summary("skyblock.adaptive.auctions.pages_fetched").record(scanSummary.pagesFetched());
            meterRegistry.summary("skyblock.adaptive.auctions.seen_count").record(scanSummary.auctionsSeen());
            meterRegistry.summary("skyblock.adaptive.auctions.kept_count").record(filteredAuctions.size());
            AuctionResponse payload = new AuctionResponse(
                    true,
                    0,
                    scanSummary.totalPages(),
                    scanSummary.totalAuctions(),
                    scanSummary.lastUpdated(),
                    filteredAuctions
            );
            auctionPollState.setPhase(AuctionPollState.Phase.SLEEP);
            return new AdaptivePoller.PollExecution<>(decision, payload, payload.getLastUpdated(), full);
        };

        return new AdaptivePoller<>(
                endpointCfg.getName(),
                endpointCfg,
                taskScheduler,
                meterRegistry,
                pollExecutor,
                processingPipeline,
                globalLimiter
        );
    }

    private AdaptivePoller<BazaarResponse> buildBazaarPoller(GlobalRequestLimiter globalLimiter) {
        AdaptivePollingProperties.Endpoint endpointCfg = adaptivePollingProperties.getBazaar();
        HypixelConditionalClient client = new HypixelConditionalClient(
                apiUrl,
                apiKey,
                endpointCfg.getConnectTimeout(),
                endpointCfg.getRequestTimeout()
        );
        AtomicBoolean initialBazaarPayloadDelivered = new AtomicBoolean(false);
        ProcessingPipeline<BazaarResponse> processingPipeline = new ProcessingPipeline<>(
                endpointCfg.getName(),
                meterRegistry,
                adaptivePollingProperties.getPipeline().getQueueCapacity(),
                adaptivePollingProperties.getPipeline().isCoalesceEnabled(),
                this::processBazaarUpdate
        );
        AdaptivePoller.PollExecutor<BazaarResponse> pollExecutor = detector -> {
            ChangeDetector.ConditionalHeaders conditionalHeaders = detector.conditionalHeaders();
            HypixelHttpResult<BazaarResponse> response = client.fetchBazaar(
                    endpointCfg.getPath(),
                    conditionalHeaders.ifNoneMatch(),
                    conditionalHeaders.ifModifiedSince()
            );
            String responseHash = hashBazaar(response.body());
            ChangeDetector.ChangeDecision decision = detector.evaluate(response, responseHash);
            BazaarResponse body = response.body();
            if (body != null && body.isSuccess() && initialBazaarPayloadDelivered.compareAndSet(false, true) && !decision.isChanged()) {
                decision = ChangeDetector.ChangeDecision.changed();
            }
            if (decision.isChanged() && body != null && body.isSuccess()) {
                return new AdaptivePoller.PollExecution<>(decision, body, body.getLastUpdated(), response);
            }
            long changeTs = body == null ? 0L : body.getLastUpdated();
            return new AdaptivePoller.PollExecution<>(decision, null, changeTs, response);
        };

        return new AdaptivePoller<>(
                endpointCfg.getName(),
                endpointCfg,
                taskScheduler,
                meterRegistry,
                pollExecutor,
                processingPipeline,
                globalLimiter
        );
    }

    private void processAuctionsUpdate(AuctionResponse response) {
        processUpdate("auctions", estimateAuctionBytes(response), () -> marketDataProcessingService
                .ingestAuctionPayload(response, "adaptive-auctions")
                .ifPresent(snapshot -> {
                    if (response != null && response.getLastUpdated() > 0L) {
                        lastCommittedAuctionLastUpdated.accumulateAndGet(response.getLastUpdated(), Math::max);
                    }
                    enqueueFlipGeneration(snapshot.snapshotTimestamp(), "auctions");
                }));
    }

    private void processBazaarUpdate(BazaarResponse response) {
        processUpdate("bazaar", estimateBazaarBytes(response), () -> {
            marketDataProcessingService
                    .ingestBazaarPayload(response, "adaptive-bazaar")
                    .ifPresent(snapshot -> enqueueFlipGeneration(snapshot.snapshotTimestamp(), "bazaar"));
            startAuctionsIfBazaarReady();
        });
    }

    private void startAuctionsIfBazaarReady() {
        if (shutdownRequested || auctionsPoller == null || auctionsStarted.get()) {
            return;
        }
        if (!marketDataProcessingService.hasBazaarPayload()) {
            return;
        }
        if (!auctionsStarted.compareAndSet(false, true)) {
            return;
        }
        log.info("Bazaar cache primed. Starting auctions poller.");
        auctionsPoller.start();
    }

    private void processUpdate(String endpoint, long payloadBytes, Runnable processor) {
        cycleInstrumentationService.startCycle();
        boolean success = false;
        long totalStart = cycleInstrumentationService.startPhase();
        try {
            processor.run();
            success = true;
        } catch (RuntimeException e) {
            log.warn("Adaptive processing failed for {}: {}", endpoint, ExceptionUtils.getStackTrace(e));
        } finally {
            cycleInstrumentationService.endPhase("total_cycle", totalStart, success, payloadBytes);
            cycleInstrumentationService.finishCycle(success);
            meterRegistry.counter("skyblock.adaptive.processed_updates", "endpoint", endpoint).increment();
        }
    }

    private void enqueueFlipGeneration(Instant snapshotTimestamp, String endpoint) {
        if (snapshotTimestamp == null) {
            return;
        }
        long snapshotEpochMillis = snapshotTimestamp.toEpochMilli();
        latestSnapshotEpochMillisToGenerate.accumulateAndGet(snapshotEpochMillis, Math::max);
        meterRegistry.counter("skyblock.adaptive.flip_generation_enqueued", "endpoint", endpoint).increment();
        if (generationWorkerScheduledOrRunning.compareAndSet(false, true)) {
            scheduleGenerationDrain("enqueue");
        }
    }

    private void drainFlipGenerationQueue() {
        try {
            while (!shutdownRequested) {
                long snapshotEpochMillis = latestSnapshotEpochMillisToGenerate.getAndSet(-1L);
                if (snapshotEpochMillis < 0L) {
                    break;
                }
                try {
                    flipGenerationService.generateIfMissingForSnapshot(Instant.ofEpochMilli(snapshotEpochMillis));
                    meterRegistry.counter("skyblock.adaptive.flip_generation_runs", "status", "success").increment();
                } catch (RuntimeException e) {
                    meterRegistry.counter("skyblock.adaptive.flip_generation_runs", "status", "error").increment();
                    log.warn("Async flip generation failed for snapshot {}: {}",
                            Instant.ofEpochMilli(snapshotEpochMillis),
                            ExceptionUtils.getStackTrace(e));
                }
            }
        } finally {
            // latestSnapshotEpochMillisToGenerate is atomically drained via getAndSet(-1L), but a concurrent
            // enqueuer can still publish new work after this worker decides to exit and before
            // generationWorkerScheduledOrRunning is observed as false. This reschedule CAS closes that gap:
            // if shutdownRequested is false and taskScheduler has pending work in
            // latestSnapshotEpochMillisToGenerate, compareAndSet(false, true) ensures a new worker is scheduled
            // so no snapshot generation request is lost.
            generationWorkerScheduledOrRunning.set(false);
            if (!shutdownRequested
                    && latestSnapshotEpochMillisToGenerate.get() >= 0L
                    && generationWorkerScheduledOrRunning.compareAndSet(false, true)) {
                scheduleGenerationDrain("reschedule");
            }
        }
    }

    private void scheduleGenerationDrain(String reason) {
        scheduleGenerationDrain(reason, 0);
    }

    private void scheduleGenerationDrain(String reason, int attempt) {
        if (shutdownRequested) {
            generationWorkerScheduledOrRunning.compareAndSet(true, false);
            return;
        }
        long delayMillis = calculateRetryDelayMillis(attempt);
        try {
            taskScheduler.schedule(this::drainFlipGenerationQueue, Instant.now().plusMillis(delayMillis));
            if (attempt > 0) {
                log.warn("Scheduled flip generation drain retry (reason={}, attempt={}, delayMs={})",
                        reason,
                        attempt,
                        delayMillis);
            }
        } catch (RuntimeException e) {
            generationWorkerScheduledOrRunning.compareAndSet(true, false);
            flipGenerationScheduleFailureCounter.increment();
            log.error("Failed to schedule flip generation drain (reason={}, attempt={}, delayMs={}): {}",
                    reason,
                    attempt,
                    delayMillis,
                    ExceptionUtils.getStackTrace(e));
            int nextAttempt = attempt + 1;
            if (nextAttempt <= MAX_GENERATION_SCHEDULE_RETRY_ATTEMPTS && generationWorkerScheduledOrRunning.compareAndSet(false, true)) {
                log.warn("Retrying flip generation drain scheduling (reason={}, attempt={}, delayMs={})",
                        reason,
                        nextAttempt,
                        calculateRetryDelayMillis(nextAttempt));
                scheduleGenerationDrain(reason, nextAttempt);
                return;
            }
            if (nextAttempt <= MAX_GENERATION_SCHEDULE_RETRY_ATTEMPTS) {
                log.warn("Skipping flip generation drain retry because a worker is already scheduled (reason={}, attempt={})",
                        reason,
                        nextAttempt);
                return;
            }
            log.error("Exhausted flip generation scheduling retries (reason={}, maxAttempts={}). Falling back to executor.",
                    reason,
                    MAX_GENERATION_SCHEDULE_RETRY_ATTEMPTS);
            if (generationWorkerScheduledOrRunning.compareAndSet(false, true)) {
                try {
                    generationFallbackExecutor.execute(this::drainFlipGenerationQueue);
                    log.warn("Submitted flip generation drain to fallback executor (reason={})", reason);
                } catch (RuntimeException fallbackException) {
                    generationWorkerScheduledOrRunning.compareAndSet(true, false);
                    flipGenerationScheduleFailureCounter.increment();
                    log.error("Failed to submit fallback flip generation drain (reason={}): {}",
                            reason,
                            ExceptionUtils.getStackTrace(fallbackException));
                }
            }
        }
    }

    private long calculateRetryDelayMillis(int attempt) {
        if (attempt <= 0) {
            return 0L;
        }
        long base = GENERATION_SCHEDULE_RETRY_BASE_DELAY.toMillis();
        long multiplier = 1L << Math.min(attempt - 1, 10);
        return base * multiplier;
    }

    private String hashAuctionsProbe(AuctionResponse response) {
        if (response == null) {
            return null;
        }
        StringBuilder builder = new StringBuilder(512);
        builder.append(response.getLastUpdated()).append('|')
                .append(response.getTotalAuctions()).append('|')
                .append(response.getTotalPages()).append('|');
        if (response.getAuctions() != null) {
            int limit = Math.min(40, response.getAuctions().size());
            for (int i = 0; i < limit; i++) {
                Auction auction = response.getAuctions().get(i);
                if (auction == null) {
                    continue;
                }
                builder.append(auction.getUuid()).append(':')
                        .append(auction.getStart()).append(':')
                        .append(auction.getEnd()).append(':')
                        .append(auction.getHighestBidAmount()).append(';');
            }
        }
        return sha256(builder.toString());
    }

    private String hashBazaar(BazaarResponse response) {
        if (response == null) {
            return null;
        }
        StringBuilder builder = new StringBuilder(2048);
        builder.append(response.getLastUpdated()).append('|');
        Map<String, BazaarProduct> products = response.getProducts();
        if (products != null && !products.isEmpty()) {
            List<String> keys = new ArrayList<>(products.keySet());
            keys.sort(Comparator.naturalOrder());
            int limit = Math.min(120, keys.size());
            for (int i = 0; i < limit; i++) {
                String key = keys.get(i);
                BazaarProduct product = products.get(key);
                BazaarQuickStatus quickStatus = product == null ? null : product.getQuickStatus();
                if (quickStatus == null) {
                    continue;
                }
                builder.append(key).append(':')
                        .append(quickStatus.getBuyPrice()).append(':')
                        .append(quickStatus.getSellPrice()).append(':')
                        .append(quickStatus.getBuyVolume()).append(':')
                        .append(quickStatus.getSellVolume()).append(':')
                        .append(quickStatus.getBuyOrders()).append(':')
                        .append(quickStatus.getSellOrders()).append(';');
            }
        }
        return sha256(builder.toString());
    }

    private String sha256(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(input.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    private long estimateAuctionBytes(AuctionResponse response) {
        if (response == null || response.getAuctions() == null) {
            return 0L;
        }
        return response.getAuctions().size() * 320L;
    }

    private long estimateBazaarBytes(BazaarResponse response) {
        if (response == null || response.getProducts() == null) {
            return 0L;
        }
        return response.getProducts().size() * 220L;
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
                null,
                null,
                auction.getCategory(),
                auction.getTier(),
                auction.getStartingBid(),
                auction.isClaimed(),
                auction.isBin(),
                List.of(),
                auction.getHighestBidAmount(),
                List.<Bid>of()
        );
    }

    private void updateAuctionPredictionState(long newLastUpdated, long nowMillis) {
        long previous = auctionPollState.getLastSeenLastUpdated();
        if (previous > 0L && newLastUpdated > previous) {
            long observedPeriod = newLastUpdated - previous;
            long ewma = auctionPollState.getEwmaPeriodMs();
            long updatedEwma = Math.round((AUCTIONS_EWMA_ALPHA * observedPeriod) + ((1.0d - AUCTIONS_EWMA_ALPHA) * ewma));
            auctionPollState.setEwmaPeriodMs(Math.max(AUCTIONS_MIN_PROBE_INTERVAL_MS, updatedEwma));
            meterRegistry.summary("skyblock.adaptive.auctions.observed_period_ms").record(observedPeriod);
        }
        auctionPollState.setLastSeenLastUpdated(newLastUpdated);
        auctionPollState.setNextExpectedAtMs(nowMillis + auctionPollState.getEwmaPeriodMs());
        auctionPollState.setProbeBackoffStep(0);
        auctionPollState.setProbeIntervalMs(AUCTIONS_MIN_PROBE_INTERVAL_MS);
    }

    private void updateAuctionProbeBackoff(long nowMillis) {
        long nextExpected = auctionPollState.getNextExpectedAtMs();
        if (nextExpected <= 0L || nowMillis <= (nextExpected + AUCTIONS_GRACE_WINDOW_MS)) {
            auctionPollState.setProbeBackoffStep(0);
            auctionPollState.setProbeIntervalMs(AUCTIONS_MIN_PROBE_INTERVAL_MS);
            return;
        }
        int nextStep = Math.min(10, auctionPollState.getProbeBackoffStep() + 1);
        long interval = Math.min(AUCTIONS_MAX_PROBE_INTERVAL_MS, AUCTIONS_MIN_PROBE_INTERVAL_MS * (1L << Math.min(nextStep, 4)));
        auctionPollState.setProbeBackoffStep(nextStep);
        auctionPollState.setProbeIntervalMs(interval);
        meterRegistry.summary("skyblock.adaptive.auctions.probe_interval_ms").record(interval);
    }
}
