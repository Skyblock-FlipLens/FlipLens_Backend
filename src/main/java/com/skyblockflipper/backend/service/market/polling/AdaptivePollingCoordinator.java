package com.skyblockflipper.backend.service.market.polling;

import com.skyblockflipper.backend.config.properties.AdaptivePollingProperties;
import com.skyblockflipper.backend.hypixel.HypixelConditionalClient;
import com.skyblockflipper.backend.hypixel.HypixelHttpResult;
import com.skyblockflipper.backend.hypixel.model.Auction;
import com.skyblockflipper.backend.hypixel.model.AuctionResponse;
import com.skyblockflipper.backend.hypixel.model.BazaarProduct;
import com.skyblockflipper.backend.hypixel.model.BazaarQuickStatus;
import com.skyblockflipper.backend.hypixel.model.BazaarResponse;
import com.skyblockflipper.backend.instrumentation.CycleInstrumentationService;
import com.skyblockflipper.backend.service.item.NeuRepoIngestionService;
import com.skyblockflipper.backend.service.flipping.FlipGenerationService;
import com.skyblockflipper.backend.service.market.MarketDataProcessingService;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Component
@Slf4j
public class AdaptivePollingCoordinator {

    private final AdaptivePollingProperties adaptivePollingProperties;
    private final TaskScheduler taskScheduler;
    private final MeterRegistry meterRegistry;
    private final MarketDataProcessingService marketDataProcessingService;
    private final FlipGenerationService flipGenerationService;
    private final CycleInstrumentationService cycleInstrumentationService;
    private final NeuRepoIngestionService neuRepoIngestionService;
    private final String apiUrl;
    private final String apiKey;
    private final AtomicBoolean startScheduledOrRunning = new AtomicBoolean(false);
    private final AtomicBoolean auctionsStarted = new AtomicBoolean(false);
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
        taskScheduler.schedule(this::attemptStart, when);
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
            ChangeDetector.ConditionalHeaders conditionalHeaders = detector.conditionalHeaders();
            HypixelHttpResult<AuctionResponse> probe = client.fetchAuctionPage(
                    endpointCfg.getPath(),
                    0,
                    conditionalHeaders.ifNoneMatch(),
                    conditionalHeaders.ifModifiedSince()
            );
            String probeHash = hashAuctionsProbe(probe.body());
            ChangeDetector.ChangeDecision decision = detector.evaluate(probe, probeHash);
            if (!decision.isChanged()) {
                long changeTs = probe.body() == null ? 0L : probe.body().getLastUpdated();
                return new AdaptivePoller.PollExecution<>(decision, null, changeTs, probe);
            }
            if (probe.body() == null || !probe.body().isSuccess()) {
                return new AdaptivePoller.PollExecution<>(ChangeDetector.ChangeDecision.error(), null, 0L, probe);
            }
            HypixelHttpResult<AuctionResponse> full = client.fetchAllAuctionPages(endpointCfg.getPath(), probe.body());
            if (!full.isSuccessful() || full.body() == null || !full.body().isSuccess()) {
                return new AdaptivePoller.PollExecution<>(ChangeDetector.ChangeDecision.error(), null, 0L, full);
            }
            return new AdaptivePoller.PollExecution<>(decision, full.body(), full.body().getLastUpdated(), full);
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
                .ifPresent(snapshot -> enqueueFlipGeneration(snapshot.snapshotTimestamp(), "auctions")));
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
            taskScheduler.schedule(this::drainFlipGenerationQueue, Instant.now());
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
                taskScheduler.schedule(this::drainFlipGenerationQueue, Instant.now());
            }
        }
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
}
