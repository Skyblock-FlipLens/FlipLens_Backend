package com.skyblockflipper.backend.api.dto;

import java.time.Instant;
import java.util.UUID;

public record MarketSnapshotDto(
        UUID id,
        Instant snapshotTimestamp,
        int auctionCount,
        int bazaarProductCount,
        Instant createdAt
) {
}

