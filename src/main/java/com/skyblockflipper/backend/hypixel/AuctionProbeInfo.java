package com.skyblockflipper.backend.hypixel;

public record AuctionProbeInfo(
        long lastUpdated,
        int totalPages,
        int totalAuctions
) {
}
