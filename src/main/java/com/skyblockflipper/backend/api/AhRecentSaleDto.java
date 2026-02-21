package com.skyblockflipper.backend.api;

import java.time.Instant;

public record AhRecentSaleDto(
        String auctionId,
        long price,
        int stars,
        String reforge,
        Instant soldAt,
        boolean bin
) {
}
