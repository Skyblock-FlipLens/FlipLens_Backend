package com.skyblockflipper.backend.config.Jobs;

import com.skyblockflipper.backend.service.flipping.FlipGenerationService;
import com.skyblockflipper.backend.service.item.NeuRepoIngestionService;
import com.skyblockflipper.backend.repository.AhItemSnapshotRepository;
import com.skyblockflipper.backend.repository.BzItemSnapshotRepository;
import com.skyblockflipper.backend.service.market.MarketDataProcessingService;
import com.skyblockflipper.backend.service.market.SnapshotRetentionProperties;
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
    private static final long MILLIS_PER_DAY = 86_400_000L;
    private static final long DEFAULT_AGGREGATE_RETENTION_DAYS = 30L;

    private final NeuRepoIngestionService neuRepoIngestionService;
    private final MarketDataProcessingService marketDataProcessingService;
    private final FlipGenerationService flipGenerationService;
    private final AhItemSnapshotRepository ahItemSnapshotRepository;
    private final BzItemSnapshotRepository bzItemSnapshotRepository;
    private final SnapshotRetentionProperties snapshotRetentionProperties;

    @Autowired
    public SourceJobs(NeuRepoIngestionService neuRepoIngestionService,
                      MarketDataProcessingService marketDataProcessingService,
                      FlipGenerationService flipGenerationService,
                      AhItemSnapshotRepository ahItemSnapshotRepository,
                      BzItemSnapshotRepository bzItemSnapshotRepository,
                      SnapshotRetentionProperties snapshotRetentionProperties) {
        this.neuRepoIngestionService = neuRepoIngestionService;
        this.marketDataProcessingService = marketDataProcessingService;
        this.flipGenerationService = flipGenerationService;
        this.ahItemSnapshotRepository = ahItemSnapshotRepository;
        this.bzItemSnapshotRepository = bzItemSnapshotRepository;
        this.snapshotRetentionProperties = snapshotRetentionProperties;
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

    @Scheduled(fixedDelayString = "${config.snapshot.retention.aggregate-cleanup-interval-ms:3600000}")
    public void compactAggregateSnapshots() {
        try {
            long nowMillis = System.currentTimeMillis();
            long ahDays = sanitizeDays(snapshotRetentionProperties.getAhAggregateDays(), DEFAULT_AGGREGATE_RETENTION_DAYS);
            long bzDays = sanitizeDays(snapshotRetentionProperties.getBzAggregateDays(), DEFAULT_AGGREGATE_RETENTION_DAYS);
            int ahDeleted = ahItemSnapshotRepository.deleteOlderThan(nowMillis - (ahDays * MILLIS_PER_DAY));
            int bzDeleted = bzItemSnapshotRepository.deleteOlderThan(nowMillis - (bzDays * MILLIS_PER_DAY));
            if (ahDeleted > 0 || bzDeleted > 0) {
                log.info("Compacted aggregate snapshots: ahDeleted={}, bzDeleted={}", ahDeleted, bzDeleted);
            }
        } catch (Exception e) {
            log.warn("Failed to compact aggregate snapshots: {}", ExceptionUtils.getStackTrace(e));
        }
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

    private long sanitizeDays(long configuredDays, long fallbackDays) {
        return configuredDays > 0L ? configuredDays : fallbackDays;
    }
}
