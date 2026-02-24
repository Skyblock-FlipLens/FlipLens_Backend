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
        boolean claimed,
        boolean bin,
        String itemLore,
        String extra
) {
    public AuctionMarketRecord(
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
        this(auctionUuid, itemName, category, tier, startingBid, highestBidAmount, startTimestamp, endTimestamp, claimed, false, null, null);
    }

    public AuctionMarketRecord(
            String auctionUuid,
            String itemName,
            String category,
            String tier,
            long startingBid,
            long highestBidAmount,
            long startTimestamp,
            long endTimestamp,
            boolean claimed,
            boolean bin
    ) {
        this(auctionUuid, itemName, category, tier, startingBid, highestBidAmount, startTimestamp, endTimestamp, claimed, bin, null, null);
    }

    public AuctionMarketRecord(
            String auctionUuid,
            String itemName,
            String category,
            String tier,
            long startingBid,
            long highestBidAmount,
            long startTimestamp,
            long endTimestamp,
            boolean claimed,
            String itemLore,
            String extra
    ) {
        this(auctionUuid, itemName, category, tier, startingBid, highestBidAmount, startTimestamp, endTimestamp, claimed, false, itemLore, extra);
    }
}
