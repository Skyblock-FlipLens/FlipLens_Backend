package com.skyblockflipper.backend.model.market;

public record AuctionMarketRecord(
        String auctionUuid,
        String itemName,
        String category,
        String tier,
        long startingBid,
        long highestBidAmount,
        long startTimestamp,
        long endTimestamp,
        boolean claimed
) {
}
