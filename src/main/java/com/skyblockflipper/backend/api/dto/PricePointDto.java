package com.skyblockflipper.backend.api.dto;

import java.time.Instant;

public record PricePointDto(
        Instant timestamp,
        Long buyPrice,
        Long sellPrice,
        Long volume
) {
}

