package com.skyblockflipper.backend.api;

public record BazaarQuickFlipDto(
        String itemId,
        String displayName,
        long buyPrice,
        long sellPrice,
        long spread,
        double spreadPct,
        long volume
) {
}
