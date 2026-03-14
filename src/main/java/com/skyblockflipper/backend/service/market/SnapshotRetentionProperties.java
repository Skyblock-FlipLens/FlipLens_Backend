package com.skyblockflipper.backend.service.market;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Setter
@Getter
@ConfigurationProperties(prefix = "config.snapshot.retention")
public class SnapshotRetentionProperties {

    private long rawWindowSeconds = 90L;
    private long minuteTierUpperSeconds = 30L * 60L;
    private long twoHourTierUpperSeconds = 12L * 60L * 60L;
    private long minuteIntervalSeconds = 60L;
    private long twoHourIntervalSeconds = 2L * 60L * 60L;
    private long ahAggregateDays = 30L;
    private long bzAggregateDays = 30L;
    private int compactionCandidateBatchSize = 50_000;
    private int flipDeleteBatchSize = 1_000;
    private long flipDeleteBatchPauseMillis = 0L;

}
