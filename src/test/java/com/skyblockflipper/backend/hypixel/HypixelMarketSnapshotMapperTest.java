package com.skyblockflipper.backend.hypixel;

import com.skyblockflipper.backend.hypixel.model.Auction;
import com.skyblockflipper.backend.hypixel.model.AuctionResponse;
import com.skyblockflipper.backend.hypixel.model.BazaarProduct;
import com.skyblockflipper.backend.hypixel.model.BazaarQuickStatus;
import com.skyblockflipper.backend.hypixel.model.BazaarResponse;
import com.skyblockflipper.backend.model.market.BazaarMarketRecord;
import com.skyblockflipper.backend.model.market.MarketSnapshot;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HypixelMarketSnapshotMapperTest {

    private final HypixelMarketSnapshotMapper mapper = new HypixelMarketSnapshotMapper();

    @Test
    void mapBuildsSnapshotFromSuccessfulResponses() {
        Auction auction = new Auction(
                "a-1", "p", "profile", java.util.List.of(), 10L, 20L,
                "ENCHANTED_DIAMOND", "lore", "extra", "misc", "RARE",
                100L, false, java.util.List.of(), 120L, java.util.List.of()
        );
        AuctionResponse auctionResponse = new AuctionResponse(true, 0, 1, 1, 1_000L, Arrays.asList(auction, null));

        BazaarQuickStatus quickStatus = new BazaarQuickStatus(10.0, 9.5, 100, 90, 1000, 900, 4, 3);
        BazaarProduct validProduct = new BazaarProduct(null, quickStatus, java.util.List.of(), java.util.List.of());
        BazaarProduct invalidProduct = new BazaarProduct("INVALID", null, java.util.List.of(), java.util.List.of());
        BazaarResponse bazaarResponse = new BazaarResponse(true, 2_000L, Map.of(
                "ENCHANTED_DIAMOND", validProduct,
                "INVALID", invalidProduct
        ));

        MarketSnapshot snapshot = mapper.map(auctionResponse, bazaarResponse);

        assertEquals(Instant.ofEpochMilli(2_000L), snapshot.snapshotTimestamp());
        assertEquals(1, snapshot.auctions().size());
        assertEquals("ENCHANTED_DIAMOND", snapshot.auctions().getFirst().itemName());
        assertEquals(1, snapshot.bazaarProducts().size());

        BazaarMarketRecord record = snapshot.bazaarProducts().get("ENCHANTED_DIAMOND");
        assertNotNull(record);
        assertEquals(10.0, record.buyPrice());
        assertEquals(9.5, record.sellPrice());
    }

    @Test
    void mapHandlesMissingOrFailedResponses() {
        AuctionResponse failedAuctions = new AuctionResponse(false, 0, 0, 0, 0L, java.util.List.of());
        BazaarResponse failedBazaar = new BazaarResponse(false, 0L, Map.of());

        MarketSnapshot snapshot = mapper.map(failedAuctions, failedBazaar);

        assertNotNull(snapshot.snapshotTimestamp());
        assertTrue(snapshot.auctions().isEmpty());
        assertTrue(snapshot.bazaarProducts().isEmpty());
    }
}
