package com.skyblockflipper.backend.hypixel;

import com.skyblockflipper.backend.hypixel.model.Auction;
import com.skyblockflipper.backend.hypixel.model.AuctionResponse;
import com.skyblockflipper.backend.hypixel.model.BazaarProduct;
import com.skyblockflipper.backend.hypixel.model.BazaarQuickStatus;
import com.skyblockflipper.backend.hypixel.model.BazaarResponse;
import com.skyblockflipper.backend.model.market.AuctionMarketRecord;
import com.skyblockflipper.backend.model.market.BazaarMarketRecord;
import com.skyblockflipper.backend.model.market.MarketSnapshot;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Component
public class HypixelMarketSnapshotMapper {

    public MarketSnapshot map(AuctionResponse auctionResponse, BazaarResponse bazaarResponse) {
        Instant snapshotTimestamp = resolveSnapshotTimestamp(auctionResponse, bazaarResponse);
        List<AuctionMarketRecord> auctions = mapAuctions(auctionResponse);
        Map<String, BazaarMarketRecord> bazaarProducts = mapBazaarProducts(bazaarResponse);
        return new MarketSnapshot(snapshotTimestamp, auctions, bazaarProducts);
    }

    private Instant resolveSnapshotTimestamp(AuctionResponse auctionResponse, BazaarResponse bazaarResponse) {
        long auctionsUpdated = auctionResponse != null && auctionResponse.isSuccess() ? auctionResponse.getLastUpdated() : 0L;
        long bazaarUpdated = bazaarResponse != null && bazaarResponse.isSuccess() ? bazaarResponse.getLastUpdated() : 0L;
        long latest = Math.max(auctionsUpdated, bazaarUpdated);
        return latest > 0 ? Instant.ofEpochMilli(latest) : Instant.now();
    }

    private List<AuctionMarketRecord> mapAuctions(AuctionResponse auctionResponse) {
        if (auctionResponse == null || !auctionResponse.isSuccess() || auctionResponse.getAuctions() == null) {
            return List.of();
        }
        return auctionResponse.getAuctions().stream()
                .map(this::toAuctionRecord)
                .filter(Objects::nonNull)
                .toList();
    }

    private AuctionMarketRecord toAuctionRecord(Auction auction) {
        if (auction == null) {
            return null;
        }
        return new AuctionMarketRecord(
                auction.getUuid(),
                auction.getItemName(),
                auction.getCategory(),
                auction.getTier(),
                auction.getStartingBid(),
                auction.getHighestBidAmount(),
                auction.getStart(),
                auction.getEnd(),
                auction.isClaimed()
        );
    }

    private Map<String, BazaarMarketRecord> mapBazaarProducts(BazaarResponse bazaarResponse) {
        if (bazaarResponse == null || !bazaarResponse.isSuccess() || bazaarResponse.getProducts() == null) {
            return Map.of();
        }
        Map<String, BazaarMarketRecord> records = new LinkedHashMap<>();
        for (Map.Entry<String, BazaarProduct> entry : bazaarResponse.getProducts().entrySet()) {
            BazaarProduct product = entry.getValue();
            if (product == null) {
                continue;
            }
            String productId = firstNonBlank(product.getProductId(), entry.getKey());
            BazaarQuickStatus quickStatus = product.getQuickStatus();
            if (productId == null || quickStatus == null) {
                continue;
            }
            records.put(productId, new BazaarMarketRecord(
                    productId,
                    quickStatus.getBuyPrice(),
                    quickStatus.getSellPrice(),
                    quickStatus.getBuyVolume(),
                    quickStatus.getSellVolume(),
                    quickStatus.getBuyMovingWeek(),
                    quickStatus.getSellMovingWeek(),
                    quickStatus.getBuyOrders(),
                    quickStatus.getSellOrders()
            ));
        }
        return records;
    }

    private String firstNonBlank(String first, String second) {
        if (first != null && !first.isBlank()) {
            return first;
        }
        if (second != null && !second.isBlank()) {
            return second;
        }
        return null;
    }
}
