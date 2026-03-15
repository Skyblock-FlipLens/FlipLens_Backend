package com.skyblockflipper.backend.config.Jobs;

import com.skyblockflipper.backend.service.market.rollup.MarketBucketMaterializationService;
import com.skyblockflipper.backend.service.market.partitioning.PartitionLifecycleService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
@Profile("compactor")
@ConditionalOnProperty(
        name = "config.snapshot.partitioning.enabled",
        havingValue = "true",
        matchIfMissing = false
)
@RequiredArgsConstructor
@Slf4j
public class PartitionMaintenanceJob {

    private final PartitionLifecycleService partitionLifecycleService;
    private volatile MarketBucketMaterializationService marketBucketMaterializationService;

    @Autowired(required = false)
    public void setMarketBucketMaterializationService(MarketBucketMaterializationService marketBucketMaterializationService) {
        this.marketBucketMaterializationService = marketBucketMaterializationService;
    }

    @Scheduled(fixedDelayString = "${config.snapshot.partitioning.maintenance-interval-ms:600000}")
    public void ensureForwardPartitions() {
        try {
            partitionLifecycleService.ensureForwardPartitions(Instant.now());
        } catch (Exception e) {
            log.warn("Failed to ensure forward partitions: {}", ExceptionUtils.getStackTrace(e));
        }
    }

    @Scheduled(fixedDelayString = "${config.snapshot.retention.aggregate-cleanup-interval-ms:3600000}")
    public void compactAggregatePartitions() {
        try {
            Instant now = Instant.now();
            MarketBucketMaterializationService materializationService = this.marketBucketMaterializationService;
            if (materializationService != null && materializationService.isEnabled()) {
                var materializationReport = materializationService.materializeDueBuckets(now);
                if (materializationReport.failedBuckets() > 0) {
                    log.warn("Pre-retention bucket materialization reported failures: processed={} failed={} scopes={}",
                            materializationReport.processedBuckets(),
                            materializationReport.failedBuckets(),
                            materializationReport.processedByScope());
                }
            }

            var report = partitionLifecycleService.executeAggregateRetention(now);
            if (report.dryRun() && report.wouldDropPartitions() > 0) {
                log.info("Aggregate partition retention dry-run: wouldDrop={} scanned={}",
                        report.wouldDropPartitions(),
                        report.scannedPartitions());
            } else if (report.droppedPartitions() > 0) {
                log.info("Aggregate partition retention applied: dropped={} scanned={}",
                        report.droppedPartitions(),
                        report.scannedPartitions());
            }
        } catch (Exception e) {
            log.warn("Failed to compact aggregate partitions: {}", ExceptionUtils.getStackTrace(e));
        }
    }
}
