package com.skyblockflipper.backend.service.market.rollup;

import com.skyblockflipper.backend.model.market.AhItemAnomalySegmentEntity;
import com.skyblockflipper.backend.model.market.AhItemBucketRollupEntity;
import com.skyblockflipper.backend.model.market.AhItemSnapshotEntity;
import com.skyblockflipper.backend.service.market.SnapshotRollupProperties;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AhItemBucketAnalyzerTest {

    @Test
    void analyzeBuildsRollupWithoutAnomalyWhenSeriesIsStable() {
        AhItemBucketAnalyzer analyzer = new AhItemBucketAnalyzer(new SnapshotRollupProperties());
        long bucketStart = Instant.parse("2026-03-14T10:00:00Z").toEpochMilli();
        long bucketEnd = bucketStart + 60_000L;

        List<AhItemSnapshotEntity> samples = List.of(
                sample(bucketStart + 0L, 100L, 100L, 101L, 110L, 30, 99L, 2),
                sample(bucketStart + 5_000L, 101L, 101L, 102L, 111L, 31, 99L, 2),
                sample(bucketStart + 10_000L, 100L, 100L, 101L, 110L, 30, 98L, 3)
        );

        AhItemBucketAnalysisResult result = analyzer.analyze(
                MarketBucketGranularity.ONE_MINUTE,
                bucketStart,
                bucketEnd,
                "HYPERION",
                samples
        );

        assertTrue(result.rollup().isPresent());
        assertTrue(result.anomalySegments().isEmpty());
        AhItemBucketRollupEntity rollup = result.rollup().orElseThrow();
        assertEquals("1m", rollup.getBucketGranularity());
        assertEquals("HYPERION", rollup.getItemKey());
        assertEquals(3, rollup.getSampleCount());
        assertEquals(3, rollup.getValidSampleCount());
        assertEquals(0, rollup.getAnomalySampleCount());
        assertNotNull(rollup.getMedianBinP50());
    }

    @Test
    void analyzeCreatesAnomalySegmentWhenPriceSpikePersists() {
        AhItemBucketAnalyzer analyzer = new AhItemBucketAnalyzer(new SnapshotRollupProperties());
        long bucketStart = Instant.parse("2026-03-14T10:00:00Z").toEpochMilli();
        long bucketEnd = bucketStart + 60_000L;

        List<AhItemSnapshotEntity> samples = List.of(
                sample(bucketStart + 0L, 100L, 100L, 100L, 110L, 30, 99L, 2),
                sample(bucketStart + 5_000L, 101L, 101L, 101L, 111L, 31, 99L, 2),
                sample(bucketStart + 10_000L, 100L, 100L, 100L, 110L, 30, 99L, 2),
                sample(bucketStart + 15_000L, 150L, 150L, 151L, 165L, 31, 140L, 2),
                sample(bucketStart + 20_000L, 152L, 152L, 153L, 166L, 31, 141L, 2),
                sample(bucketStart + 25_000L, 100L, 100L, 100L, 110L, 30, 99L, 2),
                sample(bucketStart + 30_000L, 101L, 101L, 101L, 111L, 31, 99L, 2)
        );

        AhItemBucketAnalysisResult result = analyzer.analyze(
                MarketBucketGranularity.ONE_MINUTE,
                bucketStart,
                bucketEnd,
                "HYPERION",
                samples
        );

        assertTrue(result.rollup().isPresent());
        assertEquals(1, result.anomalySegments().size());
        AhItemAnomalySegmentEntity anomaly = result.anomalySegments().getFirst();
        assertEquals("HYPERION", anomaly.getItemKey());
        assertEquals(2, anomaly.getSampleCount());
        assertEquals("MULTI_SIGNAL", anomaly.getReasonCode());

        AhItemBucketRollupEntity rollup = result.rollup().orElseThrow();
        assertEquals(2, rollup.getAnomalySampleCount());
        assertTrue(rollup.getMedianBinP50() < 120.0D);
    }

    @Test
    void analyzeCreatesInventoryCollapseAnomaly() {
        AhItemBucketAnalyzer analyzer = new AhItemBucketAnalyzer(new SnapshotRollupProperties());
        long bucketStart = Instant.parse("2026-03-14T10:00:00Z").toEpochMilli();
        long bucketEnd = bucketStart + 60_000L;

        List<AhItemSnapshotEntity> samples = List.of(
                sample(bucketStart + 0L, 100L, 100L, 100L, 110L, 30, 99L, 2),
                sample(bucketStart + 5_000L, 100L, 100L, 100L, 111L, 31, 99L, 2),
                sample(bucketStart + 10_000L, 101L, 101L, 101L, 110L, 30, 98L, 2),
                sample(bucketStart + 15_000L, 100L, 100L, 100L, 110L, 3, 99L, 0),
                sample(bucketStart + 20_000L, 100L, 100L, 100L, 110L, 2, 99L, 0),
                sample(bucketStart + 25_000L, 100L, 100L, 100L, 110L, 30, 99L, 2)
        );

        AhItemBucketAnalysisResult result = analyzer.analyze(
                MarketBucketGranularity.ONE_MINUTE,
                bucketStart,
                bucketEnd,
                "HYPERION",
                samples
        );

        assertEquals(1, result.anomalySegments().size());
        assertEquals("INVENTORY_COLLAPSE", result.anomalySegments().getFirst().getReasonCode());
    }

    private AhItemSnapshotEntity sample(long snapshotTs,
                                        long binLowest,
                                        long binLowest5Mean,
                                        long binP50,
                                        long binP95,
                                        int binCount,
                                        long bidP50,
                                        int endingSoonCount) {
        return new AhItemSnapshotEntity(
                snapshotTs,
                "HYPERION",
                binLowest,
                binLowest5Mean,
                binP50,
                binP95,
                binCount,
                bidP50,
                endingSoonCount
        );
    }
}
