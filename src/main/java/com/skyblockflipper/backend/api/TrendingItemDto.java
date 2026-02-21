package com.skyblockflipper.backend.api;

public record TrendingItemDto(
        String itemId,
        String displayName,
        double priceChange24h,
        double volumeChange24h,
        long currentPrice,
        MarketplaceType marketplace
) {
}
