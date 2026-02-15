package com.skyblockflipper.backend.service.flipping;

import com.skyblockflipper.backend.model.market.AuctionMarketRecord;
import com.skyblockflipper.backend.model.market.BazaarMarketRecord;
import com.skyblockflipper.backend.model.market.MarketSnapshot;
import com.skyblockflipper.backend.model.market.UnifiedFlipInputSnapshot;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component
public class UnifiedFlipInputMapper {

    public UnifiedFlipInputSnapshot map(MarketSnapshot marketSnapshot) {
        if (marketSnapshot == null) {
            return new UnifiedFlipInputSnapshot(null, Map.of(), Map.of());
        }
        return new UnifiedFlipInputSnapshot(
                marketSnapshot.snapshotTimestamp(),
                mapBazaarQuotes(marketSnapshot.bazaarProducts()),
                mapAuctionQuotes(marketSnapshot.auctions())
        );
    }

    private Map<String, UnifiedFlipInputSnapshot.BazaarQuote> mapBazaarQuotes(
            Map<String, BazaarMarketRecord> bazaarProducts
    ) {
        if (bazaarProducts == null || bazaarProducts.isEmpty()) {
            return Map.of();
        }
        Map<String, UnifiedFlipInputSnapshot.BazaarQuote> quotes = new LinkedHashMap<>();
        for (Map.Entry<String, BazaarMarketRecord> entry : bazaarProducts.entrySet()) {
            BazaarMarketRecord record = entry.getValue();
            if (record == null || record.productId() == null || record.productId().isBlank()) {
                continue;
            }
            quotes.put(entry.getKey(), new UnifiedFlipInputSnapshot.BazaarQuote(
                    record.buyPrice(),
                    record.sellPrice(),
                    record.buyVolume(),
                    record.sellVolume(),
                    record.buyOrders(),
                    record.sellOrders()
            ));
        }
        return quotes;
    }

    private Map<String, UnifiedFlipInputSnapshot.AuctionQuote> mapAuctionQuotes(List<AuctionMarketRecord> auctions) {
        if (auctions == null || auctions.isEmpty()) {
            return Map.of();
        }
        Map<String, AuctionAccumulator> byItem = new LinkedHashMap<>();
        for (AuctionMarketRecord record : auctions) {
            if (record == null || record.itemName() == null || record.itemName().isBlank()) {
                continue;
            }
            long observedPrice = record.highestBidAmount() > 0 ? record.highestBidAmount() : record.startingBid();
            byItem.computeIfAbsent(record.itemName(), ignored -> new AuctionAccumulator())
                    .accept(record.startingBid(), observedPrice);
        }

        Map<String, UnifiedFlipInputSnapshot.AuctionQuote> result = new LinkedHashMap<>();
        for (Map.Entry<String, AuctionAccumulator> entry : byItem.entrySet()) {
            AuctionAccumulator acc = entry.getValue();
            result.put(entry.getKey(), new UnifiedFlipInputSnapshot.AuctionQuote(
                    acc.lowestStartingBid,
                    acc.highestObservedBid,
                    acc.averageObservedPrice(),
                    acc.sampleSize
            ));
        }
        return result;
    }

    private static final class AuctionAccumulator {
        private long lowestStartingBid = Long.MAX_VALUE;
        private long highestObservedBid = 0L;
        private long observedPriceSum = 0L;
        private int sampleSize = 0;

        private void accept(long startingBid, long observedPrice) {
            if (startingBid < lowestStartingBid) {
                lowestStartingBid = startingBid;
            }
            if (observedPrice > highestObservedBid) {
                highestObservedBid = observedPrice;
            }
            observedPriceSum += observedPrice;
            sampleSize++;
        }

        private double averageObservedPrice() {
            return sampleSize == 0 ? 0D : (double) observedPriceSum / sampleSize;
        }
    }
}
