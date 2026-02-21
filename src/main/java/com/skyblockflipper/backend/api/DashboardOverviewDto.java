package com.skyblockflipper.backend.api;

import java.time.Instant;

public record DashboardOverviewDto(
        long totalItems,
        long totalActiveFlips,
        long totalAHListings,
        long bazaarProducts,
        TopFlipDto topFlip,
        String marketTrend,
        Instant lastUpdated
) {
    public record TopFlipDto(
            String id,
            String outputName,
            long expectedProfit
    ) {
    }
}
