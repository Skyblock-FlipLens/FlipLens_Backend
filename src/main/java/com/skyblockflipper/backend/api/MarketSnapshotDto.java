package com.skyblockflipper.backend.api;

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
