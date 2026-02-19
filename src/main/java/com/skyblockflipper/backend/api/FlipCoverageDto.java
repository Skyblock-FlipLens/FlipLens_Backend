package com.skyblockflipper.backend.api;

import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;

import java.time.Instant;
import java.util.List;

public record FlipCoverageDto(
        Instant snapshotTimestamp,
        List<String> excludedFlipTypes,
        List<FlipTypeCoverageDto> flipTypes
) {
    public FlipCoverageDto {
        excludedFlipTypes = excludedFlipTypes == null ? List.of() : List.copyOf(excludedFlipTypes);
        flipTypes = flipTypes == null ? List.of() : List.copyOf(flipTypes);
    }

    public enum CoverageStatus {
        SUPPORTED,
        PARTIAL,
        MISSING
    }

    public record FlipTypeCoverageDto(
            FlipType flipType,
            CoverageStatus ingestion,
            CoverageStatus calculation,
            CoverageStatus persistence,
            CoverageStatus api,
            long latestSnapshotCount,
            String notes
    ) {
    }
}
