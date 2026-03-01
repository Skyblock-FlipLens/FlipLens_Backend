package com.skyblockflipper.backend.api.dto;

import java.util.List;

public record NpcShopOfferDto(
        String npcId,
        String npcDisplayName,
        String itemId,
        int itemAmount,
        List<CostDto> costs,
        Long coinCost,
        Double unitCoinCost
) {
    public record CostDto(
            String itemId,
            int amount
    ) {
    }
}

