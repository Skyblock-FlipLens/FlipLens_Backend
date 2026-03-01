package com.skyblockflipper.backend.api.dto;

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

