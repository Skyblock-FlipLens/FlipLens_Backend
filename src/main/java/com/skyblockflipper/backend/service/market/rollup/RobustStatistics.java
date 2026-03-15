package com.skyblockflipper.backend.service.market.rollup;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public final class RobustStatistics {

    private RobustStatistics() {
    }

    public static Double median(Collection<Double> values) {
        List<Double> sorted = sorted(values);
        if (sorted.isEmpty()) {
            return null;
        }
        int middle = sorted.size() / 2;
        if ((sorted.size() & 1) == 1) {
            return sorted.get(middle);
        }
        return (sorted.get(middle - 1) + sorted.get(middle)) / 2.0D;
    }

    public static Double percentile(Collection<Double> values, double percentile) {
        List<Double> sorted = sorted(values);
        if (sorted.isEmpty()) {
            return null;
        }
        if (sorted.size() == 1) {
            return sorted.getFirst();
        }
        double clamped = Math.max(0.0D, Math.min(1.0D, percentile));
        double index = clamped * (sorted.size() - 1);
        int lower = (int) Math.floor(index);
        int upper = (int) Math.ceil(index);
        if (lower == upper) {
            return sorted.get(lower);
        }
        double weight = index - lower;
        return (sorted.get(lower) * (1.0D - weight)) + (sorted.get(upper) * weight);
    }

    public static Double mad(Collection<Double> values) {
        Double median = median(values);
        if (median == null) {
            return null;
        }
        List<Double> deviations = new ArrayList<>();
        for (Double value : values) {
            if (value == null) {
                continue;
            }
            deviations.add(Math.abs(value - median));
        }
        return median(deviations);
    }

    public static Double winsorizedMean(Collection<Double> values, double lowerPercentile, double upperPercentile) {
        List<Double> sorted = sorted(values);
        if (sorted.isEmpty()) {
            return null;
        }
        Double lower = percentile(sorted, lowerPercentile);
        Double upper = percentile(sorted, upperPercentile);
        if (lower == null || upper == null) {
            return null;
        }
        double sum = 0.0D;
        for (Double value : sorted) {
            sum += Math.min(upper, Math.max(lower, value));
        }
        return sum / sorted.size();
    }

    public static Double min(Collection<Double> values) {
        List<Double> sorted = sorted(values);
        return sorted.isEmpty() ? null : sorted.getFirst();
    }

    public static Double max(Collection<Double> values) {
        List<Double> sorted = sorted(values);
        return sorted.isEmpty() ? null : sorted.getLast();
    }

    private static List<Double> sorted(Collection<Double> values) {
        if (values == null || values.isEmpty()) {
            return List.of();
        }
        List<Double> sorted = new ArrayList<>();
        for (Double value : values) {
            if (value == null || value.isNaN() || value.isInfinite()) {
                continue;
            }
            sorted.add(value);
        }
        Collections.sort(sorted);
        return sorted;
    }
}
