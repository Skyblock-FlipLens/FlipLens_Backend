package com.skyblockflipper.backend.service.market.polling;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PeriodEstimatorTest {

    @Test
    void usesMedianAndEmaWithOutlierResistance() {
        PeriodEstimator estimator = new PeriodEstimator(7, 0.25d, 20_000L, 10_000L, 40_000L);

        estimator.observeDelta(20_000L);
        estimator.observeDelta(20_300L);
        estimator.observeDelta(19_800L);
        estimator.observeDelta(35_000L);

        long estimate = estimator.estimateMillis();
        assertTrue(estimate >= 19_000L && estimate <= 24_000L);
    }

    @Test
    void returnsFallbackWhenNoSamples() {
        PeriodEstimator estimator = new PeriodEstimator(7, 0.25d, 60_000L, 20_000L, 120_000L);
        assertEquals(60_000L, estimator.estimateMillis());
    }

    @Test
    void computesMadJitter() {
        PeriodEstimator estimator = new PeriodEstimator(7, 0.25d, 20_000L, 10_000L, 40_000L);
        estimator.observeDelta(20_000L);
        estimator.observeDelta(20_100L);
        estimator.observeDelta(20_050L);
        estimator.observeDelta(19_950L);

        assertTrue(estimator.jitterMadMillis() > 0L);
    }
}
