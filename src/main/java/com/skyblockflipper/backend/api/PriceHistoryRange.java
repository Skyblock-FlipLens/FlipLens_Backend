package com.skyblockflipper.backend.api;

import java.time.Duration;

public enum PriceHistoryRange {
    H24("24h", Duration.ofHours(24), Duration.ofHours(1)),
    D7("7d", Duration.ofDays(7), Duration.ofHours(6)),
    D30("30d", Duration.ofDays(30), Duration.ofDays(1)),
    D90("90d", Duration.ofDays(90), Duration.ofDays(1));

    private final String queryValue;
    private final Duration lookback;
    private final Duration bucketSize;

    PriceHistoryRange(String queryValue, Duration lookback, Duration bucketSize) {
        this.queryValue = queryValue;
        this.lookback = lookback;
        this.bucketSize = bucketSize;
    }

    public Duration lookback() {
        return lookback;
    }

    public Duration bucketSize() {
        return bucketSize;
    }

    public static PriceHistoryRange fromQueryValue(String value) {
        if (value == null || value.isBlank()) {
            return D30;
        }
        String normalized = value.trim().toLowerCase();
        for (PriceHistoryRange range : values()) {
            if (range.queryValue.equals(normalized)) {
                return range;
            }
        }
        return D30;
    }
}
