package com.skyblockflipper.backend.service.market;

import com.skyblockflipper.backend.model.market.AhItemSnapshotEntity;
import com.skyblockflipper.backend.model.market.AuctionMarketRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class AhSnapshotAggregator {

    private static final long ENDING_SOON_WINDOW_MILLIS = 10L * 60L * 1_000L;
    private final MarketItemKeyService marketItemKeyService;

    public AhSnapshotAggregator() {
        this(new MarketItemKeyService());
    }

    @Autowired
    public AhSnapshotAggregator(MarketItemKeyService marketItemKeyService) {
        this.marketItemKeyService = marketItemKeyService;
    }

    public List<AhItemSnapshotEntity> aggregate(Instant snapshotTimestamp, List<AuctionMarketRecord> auctions) {
        if (snapshotTimestamp == null || auctions == null || auctions.isEmpty()) {
            return List.of();
        }
        long snapshotTs = snapshotTimestamp.toEpochMilli();
        long endingSoonThreshold = snapshotTs + ENDING_SOON_WINDOW_MILLIS;

        Map<String, ItemAccumulator> byItem = new LinkedHashMap<>();
        for (AuctionMarketRecord auction : auctions) {
            if (auction == null || auction.claimed()) {
                continue;
            }
            String itemKey = marketItemKeyService.toAuctionItemKey(auction);
            if (itemKey == null || itemKey.isBlank()) {
                continue;
            }
            ItemAccumulator accumulator = byItem.computeIfAbsent(itemKey, ignored -> new ItemAccumulator());
            if (auction.bin() && auction.startingBid() > 0L) {
                accumulator.binPrices.add(auction.startingBid());
            } else if (!auction.bin() && auction.highestBidAmount() > 0L) {
                accumulator.bidPrices.add(auction.highestBidAmount());
            }
            if (auction.endTimestamp() > snapshotTs && auction.endTimestamp() <= endingSoonThreshold) {
                accumulator.endingSoonCount++;
            }
        }

        if (byItem.isEmpty()) {
            return List.of();
        }

        List<AhItemSnapshotEntity> aggregates = new ArrayList<>(byItem.size());
        byItem.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> {
                    String itemKey = entry.getKey();
                    ItemAccumulator acc = entry.getValue();
                    if (acc.binPrices.isEmpty()) {
                        return;
                    }
                    acc.binPrices.sort(Comparator.naturalOrder());
                    Long bidP50 = null;
                    if (!acc.bidPrices.isEmpty()) {
                        acc.bidPrices.sort(Comparator.naturalOrder());
                        bidP50 = percentile(acc.bidPrices, 0.50D);
                    }
                    aggregates.add(new AhItemSnapshotEntity(
                            snapshotTs,
                            itemKey,
                            acc.binPrices.getFirst(),
                            lowestFiveMean(acc.binPrices),
                            percentile(acc.binPrices, 0.50D),
                            percentile(acc.binPrices, 0.95D),
                            acc.binPrices.size(),
                            bidP50,
                            acc.endingSoonCount
                    ));
                });
        return aggregates;
    }

    private long lowestFiveMean(List<Long> sortedPrices) {
        int sample = Math.min(5, sortedPrices.size());
        long sum = 0L;
        for (int i = 0; i < sample; i++) {
            sum += sortedPrices.get(i);
        }
        return Math.round((double) sum / sample);
    }

    private long percentile(List<Long> sortedPrices, double percentile) {
        if (sortedPrices.isEmpty()) {
            return 0L;
        }
        double clamped = Math.max(0D, Math.min(1D, percentile));
        int index = (int) Math.floor(clamped * (sortedPrices.size() - 1));
        return sortedPrices.get(index);
    }

    private static final class ItemAccumulator {
        private final List<Long> binPrices = new ArrayList<>();
        private final List<Long> bidPrices = new ArrayList<>();
        private int endingSoonCount;
    }
}
