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
            long buyMovingWeek,
            long sellMovingWeek,
            int buyOrders,
            int sellOrders
    ) {
    }

    public record AuctionQuote(
            long lowestStartingBid,
            long secondLowestStartingBid,
            long highestObservedBid,
            double averageObservedPrice,
            double medianObservedPrice,
            double p25ObservedPrice,
            int sampleSize
    ) {
        public AuctionQuote {
            lowestStartingBid = Math.max(0L, lowestStartingBid);
            secondLowestStartingBid = secondLowestStartingBid <= 0L
                    ? lowestStartingBid
                    : Math.max(secondLowestStartingBid, lowestStartingBid);
            highestObservedBid = Math.max(0L, highestObservedBid);
            averageObservedPrice = sanitizeNumber(averageObservedPrice);
            medianObservedPrice = sanitizeNumber(medianObservedPrice);
            p25ObservedPrice = sanitizeNumber(p25ObservedPrice);
            sampleSize = Math.max(0, sampleSize);
        }

        public AuctionQuote(
                long lowestStartingBid,
                long highestObservedBid,
                double averageObservedPrice,
                int sampleSize
        ) {
            this(
                    lowestStartingBid,
                    Math.max(lowestStartingBid, highestObservedBid),
                    highestObservedBid,
                    averageObservedPrice,
                    averageObservedPrice,
                    averageObservedPrice,
                    sampleSize
            );
        }

        private static double sanitizeNumber(double value) {
            if (Double.isNaN(value) || Double.isInfinite(value)) {
                return 0D;
            }
            return Math.max(0D, value);
        }
    }
}
