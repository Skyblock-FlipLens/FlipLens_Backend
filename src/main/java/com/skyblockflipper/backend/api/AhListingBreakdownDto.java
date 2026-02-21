package com.skyblockflipper.backend.api;

import java.util.Map;

public record AhListingBreakdownDto(
        long totalListings,
        Map<String, Long> byStars,
        Map<String, Long> byType,
        Map<String, Long> byReforge,
        long avgPrice,
        Long lowestBin
) {
}
