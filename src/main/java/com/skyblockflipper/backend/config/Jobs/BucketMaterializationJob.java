package com.skyblockflipper.backend.config.Jobs;

import com.skyblockflipper.backend.service.market.rollup.MarketBucketMaterializationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
@Profile("compactor")
@ConditionalOnProperty(
        name = "config.snapshot.rollup.enabled",
        havingValue = "true",
        matchIfMissing = true
)
@RequiredArgsConstructor
@Slf4j
public class BucketMaterializationJob {

    private final MarketBucketMaterializationService marketBucketMaterializationService;

    @Scheduled(
            fixedDelayString = "${config.snapshot.rollup.materialization-interval-ms:60000}",
            initialDelayString = "${config.snapshot.rollup.materialization-initial-delay-ms:15000}"
    )
    public void materializeDueBuckets() {
        try {
            var report = marketBucketMaterializationService.materializeDueBuckets(Instant.now());
            if (report.failedBuckets() > 0) {
                log.warn("Bucket materialization reported failures: processed={} failed={} scopes={}",
                        report.processedBuckets(),
                        report.failedBuckets(),
                        report.processedByScope());
            } else if (report.processedBuckets() > 0) {
                log.info("Bucket materialization applied: processed={} scopes={}",
                        report.processedBuckets(),
                        report.processedByScope());
            }
        } catch (Exception e) {
            log.warn("Failed to materialize due buckets: {}", ExceptionUtils.getStackTrace(e));
        }
    }
}
