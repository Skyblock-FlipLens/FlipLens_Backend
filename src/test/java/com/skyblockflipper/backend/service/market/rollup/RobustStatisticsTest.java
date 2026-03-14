package com.skyblockflipper.backend.service.market.rollup;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class RobustStatisticsTest {

    @Test
    void medianHandlesOddAndEvenSamples() {
        assertEquals(3.0D, RobustStatistics.median(List.of(1.0D, 3.0D, 5.0D)));
        assertEquals(5.0D, RobustStatistics.median(List.of(2.0D, 4.0D, 6.0D, 8.0D)));
    }

    @Test
    void percentileInterpolatesAcrossSortedValues() {
        assertEquals(1.75D, RobustStatistics.percentile(List.of(1.0D, 2.0D, 3.0D, 4.0D), 0.25D), 1e-9);
        assertEquals(3.25D, RobustStatistics.percentile(List.of(1.0D, 2.0D, 3.0D, 4.0D), 0.75D), 1e-9);
    }

    @Test
    void madComputesMedianAbsoluteDeviation() {
        assertEquals(1.0D, RobustStatistics.mad(List.of(10.0D, 11.0D, 12.0D, 20.0D)), 1e-9);
    }

    @Test
    void winsorizedMeanClampsTails() {
        assertEquals(8.5D, RobustStatistics.winsorizedMean(List.of(1.0D, 2.0D, 3.0D, 100.0D), 0.25D, 0.75D), 1e-9);
    }

    @Test
    void emptyCollectionsReturnNull() {
        assertNull(RobustStatistics.median(List.of()));
        assertNull(RobustStatistics.percentile(List.of(), 0.5D));
        assertNull(RobustStatistics.mad(List.of()));
        assertNull(RobustStatistics.winsorizedMean(List.of(), 0.10D, 0.90D));
    }
}
