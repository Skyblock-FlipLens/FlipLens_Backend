package com.skyblockflipper.backend.api.dto;

public record BazaarProductDto(
        String itemId,
        long buyPrice,
        long sellPrice,
        long buyVolume,
        long sellVolume,
        int buyOrders,
        int sellOrders,
        long buyMovingWeek,
        long sellMovingWeek
) {
}

