package com.skyblockflipper.backend.model.market;

import java.util.Locale;

public record AuctionComparableKey(
        String baseId,
        String category,
        String tier,
        Integer petLevel,
        Integer stars,
        boolean recombobulated,
        boolean lowConfidence
) {
    public AuctionComparableKey {
        baseId = normalizeOrUnknown(baseId);
        category = normalizeOrUnknown(category);
        tier = normalizeOrUnknown(tier);
        petLevel = petLevel == null || petLevel < 0 ? null : petLevel;
        stars = stars == null || stars < 0 ? null : stars;
    }

    private static String normalizeOrUnknown(String value) {
        if (value == null || value.isBlank()) {
            return "UNKNOWN";
        }
        return value.trim().toUpperCase(Locale.ROOT);
    }
}
