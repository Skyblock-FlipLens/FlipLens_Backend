package com.skyblockflipper.backend.service.market;

import com.skyblockflipper.backend.hypixel.model.Auction;
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
import java.util.Objects;

@Service
public class AhSnapshotAggregator {

    private static final long ENDING_SOON_WINDOW_MILLIS = 10L * 60L * 1_000L;
    private final MarketItemKeyService marketItemKeyService;

    @Autowired
    public AhSnapshotAggregator(MarketItemKeyService marketItemKeyService) {
        this.marketItemKeyService = Objects.requireNonNull(marketItemKeyService, "marketItemKeyService must not be null");
    }

    public List<AhItemSnapshotEntity> aggregateFromAuctions(Instant snapshotTimestamp, List<Auction> auctions) {
        if (snapshotTimestamp == null || auctions == null || auctions.isEmpty()) {
            return List.of();
        }
        List<AuctionMarketRecord> records = new ArrayList<>(auctions.size());
        for (Auction auction : auctions) {
            if (auction == null) {
                continue;
            }
            records.add(new AuctionMarketRecord(
                    auction.getUuid(),
                    auction.getItemName(),
                    auction.getCategory(),
                    auction.getTier(),
                    auction.getStartingBid(),
                    auction.getHighestBidAmount(),
                    auction.getStart(),
                    auction.getEnd(),
                    auction.isClaimed(),
                    auction.isBin(),
                    auction.getItemLore(),
                    auction.getExtra()
            ));
        }
        return aggregate(snapshotTimestamp, records);
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
            String itemKey = marketItemKeyService.toAuctionAggregateItemKey(auction);
            if (itemKey == null || itemKey.isBlank()) {
                continue;
            }
            ItemAccumulator accumulator = byItem.computeIfAbsent(itemKey, ignored -> new ItemAccumulator());
            boolean hasAdditionals = marketItemKeyService.hasAuctionAdditionals(auction);
            if (auction.bin() && auction.startingBid() > 0L) {
                accumulator.totalBinCount++;
                if (!hasAdditionals) {
                    accumulator.pricedBinPrices.add(auction.startingBid());
                }
            } else if (!auction.bin() && auction.highestBidAmount() > 0L && !hasAdditionals) {
                accumulator.pricedBidPrices.add(auction.highestBidAmount());
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
                    if (acc.totalBinCount == 0) {
                        return;
                    }
                    Long binLowest = null;
                    Long binLowestFiveMean = null;
                    Long binP50 = null;
                    Long binP95 = null;
                    if (!acc.pricedBinPrices.isEmpty()) {
                        acc.pricedBinPrices.sort(Comparator.naturalOrder());
                        binLowest = acc.pricedBinPrices.getFirst();
                        binLowestFiveMean = lowestFiveMean(acc.pricedBinPrices);
                        binP50 = percentile(acc.pricedBinPrices, 0.50D);
                        binP95 = percentile(acc.pricedBinPrices, 0.95D);
                    }
                    Long bidP50 = null;
                    if (!acc.pricedBidPrices.isEmpty()) {
                        acc.pricedBidPrices.sort(Comparator.naturalOrder());
                        bidP50 = percentile(acc.pricedBidPrices, 0.50D);
                    }
                    aggregates.add(new AhItemSnapshotEntity(
                            snapshotTs,
                            itemKey,
                            binLowest,
                            binLowestFiveMean,
                            binP50,
                            binP95,
                            acc.totalBinCount,
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
        private final List<Long> pricedBinPrices = new ArrayList<>();
        private final List<Long> pricedBidPrices = new ArrayList<>();
        private int totalBinCount;
        private int endingSoonCount;
    }
}
