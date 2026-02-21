package com.skyblockflipper.backend.api;

import java.time.Instant;
import java.util.List;

public record AhListingDto(
        String auctionId,
        String itemId,
        String displayName,
        long price,
        List<String> enchantments,
        String rarity,
        int stars,
        String reforge,
        Instant endsAt,
        boolean bin,
        long estimatedValue,
        int hotPotatoBooks,
        List<String> gemSlots
) {
    public AhListingDto {
        enchantments = enchantments == null ? List.of() : List.copyOf(enchantments);
        gemSlots = gemSlots == null ? List.of() : List.copyOf(gemSlots);
    }
}
