package com.skyblockflipper.backend.api;

import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;

import java.time.Instant;
import java.util.List;

public record FlipSnapshotStatsDto(
        Instant snapshotTimestamp,
        long totalFlips,
        List<FlipTypeCountDto> byType
) {
    public FlipSnapshotStatsDto {
        byType = byType == null ? List.of() : List.copyOf(byType);
    }

    public record FlipTypeCountDto(
            FlipType flipType,
            long count
    ) {
    }
}
