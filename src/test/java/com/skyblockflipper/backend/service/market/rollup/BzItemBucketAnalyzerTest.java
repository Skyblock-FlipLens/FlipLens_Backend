package com.skyblockflipper.backend.service.market.rollup;

import com.skyblockflipper.backend.model.market.BzItemAnomalySegmentEntity;
import com.skyblockflipper.backend.model.market.BzItemBucketRollupEntity;
import com.skyblockflipper.backend.model.market.BzItemSnapshotEntity;
import com.skyblockflipper.backend.service.market.SnapshotRollupProperties;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BzItemBucketAnalyzerTest {

    @Test
    void analyzeBuildsRollupWithoutAnomalyWhenSeriesIsStable() {
        BzItemBucketAnalyzer analyzer = new BzItemBucketAnalyzer(new SnapshotRollupProperties());
        long bucketStart = Instant.parse("2026-03-14T10:00:00Z").toEpochMilli();
        long bucketEnd = bucketStart + 60_000L;

        List<BzItemSnapshotEntity> samples = List.of(
                sample(bucketStart + 0L, 100.0D, 99.0D, 1_000L, 1_050L),
                sample(bucketStart + 5_000L, 100.2D, 99.2D, 1_020L, 1_060L),
                sample(bucketStart + 10_000L, 100.1D, 99.1D, 1_010L, 1_030L)
        );

        BzItemBucketAnalysisResult result = analyzer.analyze(
                MarketBucketGranularity.ONE_MINUTE,
                bucketStart,
                bucketEnd,
                "ENCHANTED_DIAMOND",
                samples
        );

        assertTrue(result.rollup().isPresent());
        assertTrue(result.anomalySegments().isEmpty());
        BzItemBucketRollupEntity rollup = result.rollup().orElseThrow();
        assertEquals("1m", rollup.getBucketGranularity());
        assertEquals("ENCHANTED_DIAMOND", rollup.getProductId());
        assertEquals(3, rollup.getSampleCount());
        assertEquals(3, rollup.getValidSampleCount());
        assertEquals(0, rollup.getAnomalySampleCount());
        assertNotNull(rollup.getMedianMidPrice());
    }

    @Test
    void analyzeCreatesAnomalySegmentWhenPriceSpikePersists() {
        BzItemBucketAnalyzer analyzer = new BzItemBucketAnalyzer(new SnapshotRollupProperties());
        long bucketStart = Instant.parse("2026-03-14T10:00:00Z").toEpochMilli();
        long bucketEnd = bucketStart + 60_000L;

        List<BzItemSnapshotEntity> samples = List.of(
                sample(bucketStart + 0L, 100.0D, 99.0D, 1_000L, 1_000L),
                sample(bucketStart + 5_000L, 100.1D, 99.1D, 1_000L, 1_000L),
                sample(bucketStart + 10_000L, 100.2D, 99.2D, 1_000L, 1_000L),
                sample(bucketStart + 15_000L, 150.0D, 149.0D, 1_000L, 1_000L),
                sample(bucketStart + 20_000L, 151.0D, 150.0D, 1_000L, 1_000L),
                sample(bucketStart + 25_000L, 100.0D, 99.0D, 1_000L, 1_000L),
                sample(bucketStart + 30_000L, 100.1D, 99.1D, 1_000L, 1_000L)
        );

        BzItemBucketAnalysisResult result = analyzer.analyze(
                MarketBucketGranularity.ONE_MINUTE,
                bucketStart,
                bucketEnd,
                "ENCHANTED_DIAMOND",
                samples
        );

        assertTrue(result.rollup().isPresent());
        assertEquals(1, result.anomalySegments().size());
        BzItemAnomalySegmentEntity anomaly = result.anomalySegments().getFirst();
        assertEquals("ENCHANTED_DIAMOND", anomaly.getProductId());
        assertEquals(2, anomaly.getSampleCount());
        assertEquals("PRICE_SPIKE", anomaly.getReasonCode());

        BzItemBucketRollupEntity rollup = result.rollup().orElseThrow();
        assertEquals(2, rollup.getAnomalySampleCount());
        assertTrue(rollup.getMedianMidPrice() < 110.0D);
    }

    @Test
    void analyzeDoesNotSplitSingleIsolatedSpike() {
        BzItemBucketAnalyzer analyzer = new BzItemBucketAnalyzer(new SnapshotRollupProperties());
        long bucketStart = Instant.parse("2026-03-14T10:00:00Z").toEpochMilli();
        long bucketEnd = bucketStart + 60_000L;

        List<BzItemSnapshotEntity> samples = List.of(
                sample(bucketStart + 0L, 100.0D, 99.0D, 1_000L, 1_000L),
                sample(bucketStart + 5_000L, 100.1D, 99.1D, 1_000L, 1_000L),
                sample(bucketStart + 10_000L, 150.0D, 149.0D, 1_000L, 1_000L),
                sample(bucketStart + 15_000L, 100.2D, 99.2D, 1_000L, 1_000L),
                sample(bucketStart + 20_000L, 100.0D, 99.0D, 1_000L, 1_000L),
                sample(bucketStart + 25_000L, 100.1D, 99.1D, 1_000L, 1_000L)
        );

        BzItemBucketAnalysisResult result = analyzer.analyze(
                MarketBucketGranularity.ONE_MINUTE,
                bucketStart,
                bucketEnd,
                "ENCHANTED_DIAMOND",
                samples
        );

        assertTrue(result.rollup().isPresent());
        assertTrue(result.anomalySegments().isEmpty());
        assertEquals(0, result.rollup().orElseThrow().getAnomalySampleCount());
    }

    @Test
    void analyzeCreatesLiquidityCollapseAnomaly() {
        BzItemBucketAnalyzer analyzer = new BzItemBucketAnalyzer(new SnapshotRollupProperties());
        long bucketStart = Instant.parse("2026-03-14T10:00:00Z").toEpochMilli();
        long bucketEnd = bucketStart + 60_000L;

        List<BzItemSnapshotEntity> samples = List.of(
                sample(bucketStart + 0L, 100.0D, 99.0D, 1_000L, 1_000L),
                sample(bucketStart + 5_000L, 100.0D, 99.0D, 1_020L, 1_010L),
                sample(bucketStart + 10_000L, 100.1D, 99.1D, 1_010L, 1_030L),
                sample(bucketStart + 15_000L, 100.0D, 99.0D, 50L, 40L),
                sample(bucketStart + 20_000L, 100.0D, 99.0D, 45L, 40L),
                sample(bucketStart + 25_000L, 100.0D, 99.0D, 1_000L, 1_000L)
        );

        BzItemBucketAnalysisResult result = analyzer.analyze(
                MarketBucketGranularity.ONE_MINUTE,
                bucketStart,
                bucketEnd,
                "ENCHANTED_DIAMOND",
                samples
        );

        assertEquals(1, result.anomalySegments().size());
        assertEquals("LIQUIDITY_COLLAPSE", result.anomalySegments().getFirst().getReasonCode());
    }

    @Test
    void analyzeMarksRollupPartialWhenCoverageIsTooLow() {
        BzItemBucketAnalyzer analyzer = new BzItemBucketAnalyzer(new SnapshotRollupProperties());
        long bucketStart = Instant.parse("2026-03-14T10:00:00Z").toEpochMilli();
        long bucketEnd = bucketStart + 7_200_000L;

        List<BzItemSnapshotEntity> samples = List.of(
                sample(bucketStart + 0L, 100.0D, 99.0D, 1_000L, 1_000L),
                sample(bucketStart + 10_000L, 100.1D, 99.1D, 1_010L, 1_000L),
                sample(bucketStart + 20_000L, 100.2D, 99.2D, 1_000L, 1_020L),
                sample(bucketStart + 30_000L, 100.3D, 99.3D, 1_020L, 1_010L)
        );

        BzItemBucketAnalysisResult result = analyzer.analyze(
                MarketBucketGranularity.TWO_HOURS,
                bucketStart,
                bucketEnd,
                "ENCHANTED_DIAMOND",
                samples
        );

        assertTrue(result.rollup().isPresent());
        assertTrue(result.rollup().orElseThrow().isPartial());
        assertFalse(result.rollup().orElseThrow().getValidSampleCount() < 4);
    }

    private BzItemSnapshotEntity sample(long snapshotTs,
                                        double buyPrice,
                                        double sellPrice,
                                        long buyVolume,
                                        long sellVolume) {
        return new BzItemSnapshotEntity(
                snapshotTs,
                "ENCHANTED_DIAMOND",
                buyPrice,
                sellPrice,
                buyVolume,
                sellVolume
        );
    }
}
