package com.skyblockflipper.backend.api.dto;

import java.time.Instant;

public record ScorePointDto(
        Instant timestamp,
        Double liquidityScore,
        Double riskScore
) {
}

