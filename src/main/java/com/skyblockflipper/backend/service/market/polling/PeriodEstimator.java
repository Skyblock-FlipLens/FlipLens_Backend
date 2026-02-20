package com.skyblockflipper.backend.service.market.polling;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;

public class PeriodEstimator {

    private final int windowSize;
    private final double emaAlpha;
    private final long fallbackPeriodMillis;
    private final long minPeriodMillis;
    private final long maxPeriodMillis;
    private final Deque<Long> deltas = new ArrayDeque<>();
    private Long emaMillis;

    public PeriodEstimator(int windowSize,
                           double emaAlpha,
                           long fallbackPeriodMillis,
                           long minPeriodMillis,
                           long maxPeriodMillis) {
        this.windowSize = Math.max(3, windowSize);
        this.emaAlpha = Math.min(0.99d, Math.max(0.01d, emaAlpha));
        this.fallbackPeriodMillis = Math.max(100L, fallbackPeriodMillis);
        this.minPeriodMillis = Math.max(100L, minPeriodMillis);
        this.maxPeriodMillis = Math.max(this.minPeriodMillis, maxPeriodMillis);
    }

    public void observeDelta(long deltaMillis) {
        if (deltaMillis <= 0L) {
            return;
        }
        long clamped = clamp(deltaMillis);
        deltas.addLast(clamped);
        while (deltas.size() > windowSize) {
            deltas.removeFirst();
        }
        if (emaMillis == null) {
            emaMillis = clamped;
            return;
        }
        emaMillis = Math.round((emaAlpha * clamped) + ((1.0d - emaAlpha) * emaMillis));
    }

    public int sampleCount() {
        return deltas.size();
    }

    public long estimateMillis() {
        if (deltas.isEmpty()) {
            return fallbackPeriodMillis;
        }
        long median = median(deltas);
        if (emaMillis == null) {
            return median;
        }
        return clamp(Math.round((median + emaMillis) / 2.0d));
    }

    public long jitterMadMillis() {
        if (deltas.size() < 3) {
            return 0L;
        }
        long center = median(deltas);
        List<Long> absoluteDeviations = new ArrayList<>(deltas.size());
        for (Long delta : deltas) {
            absoluteDeviations.add(Math.abs(delta - center));
        }
        return median(absoluteDeviations);
    }

    private long clamp(long value) {
        return Math.max(minPeriodMillis, Math.min(maxPeriodMillis, value));
    }

    private static long median(Iterable<Long> values) {
        List<Long> sorted = new ArrayList<>();
        for (Long value : values) {
            sorted.add(value);
        }
        Collections.sort(sorted);
        if (sorted.isEmpty()) {
            return 0L;
        }
        int mid = sorted.size() / 2;
        if ((sorted.size() % 2) == 1) {
            return sorted.get(mid);
        }
        return Math.round((sorted.get(mid - 1) + sorted.get(mid)) / 2.0d);
    }
}
