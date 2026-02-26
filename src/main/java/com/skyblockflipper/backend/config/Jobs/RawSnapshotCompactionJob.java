package com.skyblockflipper.backend.config.Jobs;

import com.skyblockflipper.backend.service.market.MarketDataProcessingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Profile("!compactor")
@ConditionalOnProperty(
        name = "config.snapshot.retention.raw-compaction-enabled",
        havingValue = "true",
        matchIfMissing = true
)
@RequiredArgsConstructor
@Slf4j
public class RawSnapshotCompactionJob {

    private final MarketDataProcessingService marketDataProcessingService;

    @Scheduled(
            fixedDelayString = "${config.snapshot.retention.compaction-interval-ms:60000}",
            initialDelayString = "${config.snapshot.retention.compaction-initial-delay-ms:60000}"
    )
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
}
