package com.skyblockflipper.backend.model.market;

public record BazaarMarketRecord(
        String productId,
        double buyPrice,
        double sellPrice,
        long buyVolume,
        long sellVolume,
        long buyMovingWeek,
        long sellMovingWeek,
        int buyOrders,
        int sellOrders
) {
}
