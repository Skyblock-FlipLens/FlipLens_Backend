package com.skyblockflipper.backend.model.market;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public record MarketSnapshot(
        Instant snapshotTimestamp,
        List<AuctionMarketRecord> auctions,
        Map<String, BazaarMarketRecord> bazaarProducts
) {
    public MarketSnapshot {
        if (snapshotTimestamp == null) {
            snapshotTimestamp = Instant.now();
        }
        auctions = auctions == null ? List.of() : List.copyOf(auctions);
        bazaarProducts = bazaarProducts == null ? Map.of() : Map.copyOf(bazaarProducts);
    }
}
