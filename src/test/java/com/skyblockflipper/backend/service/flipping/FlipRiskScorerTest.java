package com.skyblockflipper.backend.service.flipping;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class FlipRiskScorerTest {

    private final FlipRiskScorer scorer = new FlipRiskScorer();

    @Test
    void computeTotalRiskScoreReturnsNullWhenExecutionRiskMissing() {
        Double result = scorer.computeTotalRiskScore(
                List.of(),
                1.0D,
                1.0D,
                1.0D,
                FlipScoreFeatureSet.empty(),
                List.of("ITEM")
        );

        assertNull(result);
    }

    @Test
    void computeTotalRiskScoreFallsBackToExecutionCompositeWithoutTimescaleSignals() {
        Double result = scorer.computeTotalRiskScore(
                List.of(80D),
                3.0D,
                3.0D,
                0.0D,
                null,
                null
        );

        assertNotNull(result);
        assertEquals(86.0D, result, 1e-9);
    }

    @Test
    void computeTotalRiskScoreBlendsExecutionMicroAndMacroWhenConfident() {
        FlipScoreFeatureSet.ItemTimescaleFeatures features = new FlipScoreFeatureSet.ItemTimescaleFeatures(
                0.03D,
                0.05D,
                FlipScoreFeatureSet.ConfidenceLevel.HIGH,
                0.12D,
                0.20D,
                FlipScoreFeatureSet.ConfidenceLevel.HIGH,
                false
        );
        FlipScoreFeatureSet featureSet = new FlipScoreFeatureSet(Map.of("A", features));

        Double result = scorer.computeTotalRiskScore(
                List.of(20D),
                0.0D,
                0.0D,
                0.0D,
                featureSet,
                List.of("A")
        );

        assertNotNull(result);
        assertEquals(61.3D, result, 1e-6);
    }

    @Test
    void computeTotalRiskScoreUsesUniqueItemIdsAndConfidenceDownweightsTimescale() {
        FlipScoreFeatureSet.ItemTimescaleFeatures features = new FlipScoreFeatureSet.ItemTimescaleFeatures(
                0.03D,
                0.00D,
                FlipScoreFeatureSet.ConfidenceLevel.MEDIUM,
                0.12D,
                0.20D,
                FlipScoreFeatureSet.ConfidenceLevel.LOW,
                false
        );
        FlipScoreFeatureSet featureSet = new FlipScoreFeatureSet(Map.of("A", features));

        Double result = scorer.computeTotalRiskScore(
                List.of(50D),
                0.0D,
                0.0D,
                0.0D,
                featureSet,
                List.of("A", "A", "MISSING")
        );

        assertNotNull(result);
        assertEquals(41.125D, result, 1e-6);
    }

    @Test
    void computeTotalRiskScoreClampsExtremeInputsToHundred() {
        Double result = scorer.computeTotalRiskScore(
                List.of(300D),
                50.0D,
                50.0D,
                50.0D,
                FlipScoreFeatureSet.empty(),
                List.of("A")
        );

        assertNotNull(result);
        assertEquals(100.0D, result, 1e-9);
    }
}
