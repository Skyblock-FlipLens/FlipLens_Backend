package com.skyblockflipper.backend.service.flipping;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

@Component
public class FlipRiskScorer {

    private static final double EXPOSURE_TIME_CAP_HOURS = 6.0D;
    private static final double MICRO_VOLATILITY_CAP = 0.03D;
    private static final double MICRO_RETURN_CAP = 0.05D;
    private static final double MACRO_VOLATILITY_CAP = 0.12D;
    private static final double MACRO_RETURN_CAP = 0.20D;
    private static final double TOTAL_RISK_EXECUTION_WEIGHT = 0.45D;
    private static final double TOTAL_RISK_MICRO_WEIGHT = 0.35D;
    private static final double TOTAL_RISK_MACRO_WEIGHT = 0.20D;

    public Double computeTotalRiskScore(List<Double> legExecutionRiskScores,
                                        double inputFillHours,
                                        double outputFillHours,
                                        double craftDelayHours,
                                        FlipScoreFeatureSet featureSet,
                                        List<String> bazaarSignalItemIds) {
        Double executionRisk = maxValue(legExecutionRiskScores);
        if (executionRisk == null) {
            return null;
        }

        double exposureHours = inputFillHours + outputFillHours + craftDelayHours;
        double exposureRisk = clamp01(exposureHours / EXPOSURE_TIME_CAP_HOURS) * 100D;
        double executionComposite = clamp((0.7D * executionRisk) + (0.3D * exposureRisk), 100D);

        RiskAggregation microAggregation = aggregateTimescaleRisk(featureSet, bazaarSignalItemIds, true);
        RiskAggregation macroAggregation = aggregateTimescaleRisk(featureSet, bazaarSignalItemIds, false);

        double microWeight = TOTAL_RISK_MICRO_WEIGHT * microAggregation.confidenceFactor();
        double macroWeight = TOTAL_RISK_MACRO_WEIGHT * macroAggregation.confidenceFactor();
        double executionWeight = TOTAL_RISK_EXECUTION_WEIGHT
                + (TOTAL_RISK_MICRO_WEIGHT - microWeight)
                + (TOTAL_RISK_MACRO_WEIGHT - macroWeight);

        double microRisk = microAggregation.riskScore() == null ? executionComposite : microAggregation.riskScore();
        double macroRisk = macroAggregation.riskScore() == null ? executionComposite : macroAggregation.riskScore();

        return clamp(
                (executionWeight * executionComposite) + (microWeight * microRisk) + (macroWeight * macroRisk),
                100D
        );
    }

    private RiskAggregation aggregateTimescaleRisk(FlipScoreFeatureSet featureSet,
                                                   List<String> bazaarSignalItemIds,
                                                   boolean micro) {
        if (featureSet == null || bazaarSignalItemIds == null || bazaarSignalItemIds.isEmpty()) {
            return RiskAggregation.empty();
        }

        Set<String> uniqueItemIds = new LinkedHashSet<>(bazaarSignalItemIds);
        List<Double> risks = new ArrayList<>();
        double minConfidenceFactor = 1D;

        for (String itemId : uniqueItemIds) {
            FlipScoreFeatureSet.ItemTimescaleFeatures itemFeatures = featureSet.get(itemId);
            if (itemFeatures == null) {
                continue;
            }
            Double risk = micro ? computeMicroRisk(itemFeatures) : computeMacroRisk(itemFeatures);
            if (risk == null) {
                continue;
            }
            risks.add(risk);
            double confidence = micro
                    ? itemFeatures.microConfidence().weightFactor()
                    : itemFeatures.macroConfidence().weightFactor();
            minConfidenceFactor = Math.min(minConfidenceFactor, confidence);
        }

        if (risks.isEmpty()) {
            return RiskAggregation.empty();
        }
        double confidenceFactor = minConfidenceFactor;
        return new RiskAggregation(maxValue(risks), confidenceFactor);
    }

    private Double computeMicroRisk(FlipScoreFeatureSet.ItemTimescaleFeatures features) {
        if (features == null) {
            return null;
        }
        return combineVolAndReturnRisk(
                features.microVolatility1m(),
                features.microReturn1m(),
                MICRO_VOLATILITY_CAP,
                MICRO_RETURN_CAP
        );
    }

    private Double computeMacroRisk(FlipScoreFeatureSet.ItemTimescaleFeatures features) {
        if (features == null) {
            return null;
        }
        return combineVolAndReturnRisk(
                features.macroVolatility1d(),
                features.macroReturn1d(),
                MACRO_VOLATILITY_CAP,
                MACRO_RETURN_CAP
        );
    }

    private Double combineVolAndReturnRisk(Double volatility,
                                           Double ret,
                                           double volatilityCap,
                                           double returnCap) {
        if (volatility == null && ret == null) {
            return null;
        }
        if (volatility != null && ret != null) {
            double volRisk = clamp01(volatility / volatilityCap) * 100D;
            double returnRisk = clamp01(Math.abs(ret) / returnCap) * 100D;
            return clamp((0.7D * volRisk) + (0.3D * returnRisk), 100D);
        }
        if (volatility != null) {
            return clamp01(volatility / volatilityCap) * 100D;
        }
        return clamp01(Math.abs(ret) / returnCap) * 100D;
    }

    private Double maxValue(List<Double> values) {
        if (values == null || values.isEmpty()) {
            return null;
        }
        return values.stream().filter(Objects::nonNull).max(Double::compareTo).orElse(null);
    }

    private double clamp01(double value) {
        return clamp(value, 1D);
    }

    private double clamp(double value, double max) {
        return Math.max(0.0, Math.min(max, value));
    }

    private record RiskAggregation(
            Double riskScore,
            double confidenceFactor
    ) {
        private static RiskAggregation empty() {
            return new RiskAggregation(null, 0D);
        }
    }
}
