package com.skyblockflipper.backend.api;

import java.time.Instant;

public record PricePointDto(
        Instant timestamp,
        Long buyPrice,
        Long sellPrice,
        Long volume
) {
}
