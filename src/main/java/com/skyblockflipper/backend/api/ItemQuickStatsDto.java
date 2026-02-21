package com.skyblockflipper.backend.api;

public record ItemQuickStatsDto(
        Long buyPrice,
        Long sellPrice,
        Double buyChange24h,
        Double sellChange24h,
        Long spread,
        Double spreadPct,
        Long volume,
        Long avgVolume7d,
        Long high7d,
        Long low7d
) {
}
