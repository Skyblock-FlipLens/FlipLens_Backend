package com.skyblockflipper.backend.api.dto;

import java.util.Map;

public record FlipSummaryStatsDto(
        long totalActiveFlips,
        long avgProfit,
        double avgRoi,
        long bestFlipProfit,
        Map<String, Long> byType
) {
}

