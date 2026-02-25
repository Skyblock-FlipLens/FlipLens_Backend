package com.skyblockflipper.backend.config.Jobs;

import com.skyblockflipper.backend.service.flipping.FlipGenerationService;
import com.skyblockflipper.backend.service.item.NeuRepoIngestionService;
import com.skyblockflipper.backend.service.market.MarketDataProcessingService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
@Slf4j
public class SourceJobs {
    private final NeuRepoIngestionService neuRepoIngestionService;
    private final MarketDataProcessingService marketDataProcessingService;
    private final FlipGenerationService flipGenerationService;

    @Autowired
    public SourceJobs(NeuRepoIngestionService neuRepoIngestionService,
                      MarketDataProcessingService marketDataProcessingService,
                      FlipGenerationService flipGenerationService){
        this.neuRepoIngestionService = neuRepoIngestionService;
        this.marketDataProcessingService = marketDataProcessingService;
        this.flipGenerationService = flipGenerationService;
    }

    @Scheduled(fixedDelayString = "30000")
    public void compactSnapshots() {
        try {
            var result = marketDataProcessingService.compactSnapshots();
            if (result.deletedCount() > 0) {
                log.info(
                        "Compacted market snapshots: scanned={}, kept={}, deleted={}",
                        result.scannedCount(),
                        result.keptCount(),
                        result.deletedCount()
                );
            }
        } catch (Exception e) {
            log.warn("Failed to compact market snapshots: {}", ExceptionUtils.getStackTrace(e));
        }
    }

    @Scheduled(cron = "0 0 23 * * *", zone = "UTC")
    public void copyRepoDaily() {
        runCopyRepoDaily();
    }

    @Async("sourceJobsAsyncExecutor")
    public void copyRepoDailyAsync() {
        runCopyRepoDaily();
    }

    private void runCopyRepoDaily() {
        long startedAtMillis = System.currentTimeMillis();
        try {
            int savedItems = neuRepoIngestionService.ingestLatestFilteredItems();
            marketDataProcessingService.latestMarketSnapshot()
                    .ifPresent(snapshot -> {
                        var result = flipGenerationService.regenerateForSnapshot(snapshot.snapshotTimestamp());
                        log.info("Regenerated flips for latest snapshot {} after NEU refresh: generated={}, skipped={}",
                                snapshot.snapshotTimestamp(),
                                result.generatedCount(),
                                result.skippedCount());
                    });
            log.info("copyRepoDaily completed in {} ms (saved {} NEU items)", System.currentTimeMillis() - startedAtMillis, savedItems);
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new RuntimeException(e);
        }
    }
}
