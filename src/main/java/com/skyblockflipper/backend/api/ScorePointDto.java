package com.skyblockflipper.backend.api;

import java.time.Instant;

public record ScorePointDto(
        Instant timestamp,
        Double liquidityScore,
        Double riskScore
) {
}
