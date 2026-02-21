package com.skyblockflipper.backend.api;

import java.util.List;

public record ItemDto(
        String id,
        String displayName,
        String minecraftId,
        String rarity,
        String category,
        MarketplaceType marketplace,
        List<String> infoLinks
) {
    public ItemDto {
        infoLinks = infoLinks == null ? List.of() : List.copyOf(infoLinks);
    }

    public ItemDto(
            String id,
            String displayName,
            String minecraftId,
            String rarity,
            String category,
            List<String> infoLinks
    ) {
        this(id, displayName, minecraftId, rarity, category, null, infoLinks);
    }
}
