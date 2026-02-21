package com.skyblockflipper.backend.api;

import java.util.List;

public record BazaarOrderBookDto(
        List<OrderLevelDto> sellOrders,
        List<OrderLevelDto> buyOrders
) {
    public BazaarOrderBookDto {
        sellOrders = sellOrders == null ? List.of() : List.copyOf(sellOrders);
        buyOrders = buyOrders == null ? List.of() : List.copyOf(buyOrders);
    }

    public record OrderLevelDto(
            double pricePerUnit,
            long amount,
            int orders
    ) {
    }
}
