package com.skyblockflipper.backend.model.market;

import java.time.Instant;
import java.util.Map;

public record UnifiedFlipInputSnapshot(
        Instant snapshotTimestamp,
        Map<String, BazaarQuote> bazaarQuotes,
        Map<String, AuctionQuote> auctionQuotesByItem
) {
    public UnifiedFlipInputSnapshot {
        if (snapshotTimestamp == null) {
            snapshotTimestamp = Instant.now();
        }
        bazaarQuotes = bazaarQuotes == null ? Map.of() : Map.copyOf(bazaarQuotes);
        auctionQuotesByItem = auctionQuotesByItem == null ? Map.of() : Map.copyOf(auctionQuotesByItem);
    }

    public record BazaarQuote(
            double buyPrice,
            double sellPrice,
            long buyVolume,
            long sellVolume,
            int buyOrders,
            int sellOrders
    ) {
    }

    public record AuctionQuote(
            long lowestStartingBid,
            long highestObservedBid,
            double averageObservedPrice,
            int sampleSize
    ) {
    }
}
