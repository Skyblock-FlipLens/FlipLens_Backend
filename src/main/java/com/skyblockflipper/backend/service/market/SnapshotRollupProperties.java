package com.skyblockflipper.backend.service.market;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "config.snapshot.rollup")
public class SnapshotRollupProperties {

    private boolean enabled = true;
    private long watermarkSeconds = 30L;
    private long materializationIntervalMs = 60_000L;
    private long materializationInitialDelayMs = 15_000L;
    private long rawRetentionDays = 7L;
    private long testRawRetentionDays = 1L;
    private int maxBucketsPerRun = 128;
    private double minDailyRollupDistinctCoverageRatio = 0.90D;
    private final Anomaly anomaly = new Anomaly();

    @Getter
    @Setter
    public static class Anomaly {

        private boolean enabled = true;
        private int minValidSamples1m = 3;
        private int minValidSamples2h = 4;
        private int minValidSamples1d = 2;
        private double minBucketCoverageRatio = 0.35D;
        private double zThreshold = 6.0D;
        private double bazaarRelativePriceFloor = 0.08D;
        private double ahRelativePriceFloor = 0.10D;
        private double spreadMultiplierThreshold = 2.0D;
        private double liquidityDropThreshold = 0.25D;
        private double ahBinCountDropThreshold = 0.30D;
        private int consecutiveSamplesThreshold = 2;
        private long anomalyDurationThresholdSeconds = 10L;
        private long mergeGapSeconds = 5L;
        private int maxSegmentsPerItemPerBucket = 1;
    }
}
