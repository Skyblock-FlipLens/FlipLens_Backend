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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class AdaptivePollingCoordinator {

    private final AdaptivePollingProperties adaptivePollingProperties;
    private final TaskScheduler taskScheduler;
    private final MeterRegistry meterRegistry;
    private final MarketDataProcessingService marketDataProcessingService;
    private final FlipGenerationService flipGenerationService;
    private final CycleInstrumentationService cycleInstrumentationService;
    private final String apiUrl;
    private final String apiKey;

    private AdaptivePoller<AuctionResponse> auctionsPoller;
    private AdaptivePoller<BazaarResponse> bazaarPoller;

    public AdaptivePollingCoordinator(AdaptivePollingProperties adaptivePollingProperties,
                                      TaskScheduler taskScheduler,
                                      MeterRegistry meterRegistry,
                                      MarketDataProcessingService marketDataProcessingService,
                                      FlipGenerationService flipGenerationService,
                                      CycleInstrumentationService cycleInstrumentationService,
                                      @Value("${config.hypixel.api-url}") String apiUrl,
                                      @Value("${config.hypixel.api-key:}") String apiKey) {
        this.adaptivePollingProperties = adaptivePollingProperties;
        this.taskScheduler = taskScheduler;
        this.meterRegistry = meterRegistry;
        this.marketDataProcessingService = marketDataProcessingService;
        this.flipGenerationService = flipGenerationService;
        this.cycleInstrumentationService = cycleInstrumentationService;
        this.apiUrl = apiUrl;
        this.apiKey = apiKey;
    }

    @PostConstruct
    public void start() {
        if (!adaptivePollingProperties.isEnabled()) {
            log.info("Adaptive polling disabled via config.hypixel.adaptive.enabled=false");
            return;
        }

        GlobalRequestLimiter globalLimiter = new GlobalRequestLimiter(adaptivePollingProperties.getGlobalMaxRequestsPerSecond());
        auctionsPoller = buildAuctionsPoller(globalLimiter);
        bazaarPoller = buildBazaarPoller(globalLimiter);
        auctionsPoller.start();
        bazaarPoller.start();
    }

    @PreDestroy
    public void stop() {
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
            if (decision.isChanged() && response.body() != null && response.body().isSuccess()) {
                return new AdaptivePoller.PollExecution<>(decision, response.body(), response.body().getLastUpdated(), response);
            }
            long changeTs = response.body() == null ? 0L : response.body().getLastUpdated();
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
                .ingestAuctionPayload(response, "adaptive-auctions").ifPresent(snapshot -> flipGenerationService.generateIfMissingForSnapshot(snapshot.snapshotTimestamp())));
    }

    private void processBazaarUpdate(BazaarResponse response) {
        processUpdate("bazaar", estimateBazaarBytes(response), () -> marketDataProcessingService
                .ingestBazaarPayload(response, "adaptive-bazaar").ifPresent(snapshot -> flipGenerationService.generateIfMissingForSnapshot(snapshot.snapshotTimestamp())));
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
