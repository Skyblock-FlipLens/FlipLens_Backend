package com.skyblockflipper.backend.service.market;

import com.skyblockflipper.backend.model.market.AhItemSnapshotEntity;
import com.skyblockflipper.backend.model.market.AuctionMarketRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AhSnapshotAggregatorTest {

    @Test
    void constructorRejectsNullMarketItemKeyService() {
        assertThrows(NullPointerException.class, () -> new AhSnapshotAggregator(null));
    }

    @Test
    void aggregateReturnsEmptyForNullSnapshotOrInput() {
        AhSnapshotAggregator aggregator = new AhSnapshotAggregator(new MarketItemKeyService());

        assertTrue(aggregator.aggregate(null, List.of()).isEmpty());
        assertTrue(aggregator.aggregate(Instant.now(), null).isEmpty());
        assertTrue(aggregator.aggregate(Instant.now(), List.of()).isEmpty());
    }

    @Test
    void aggregateBuildsSortedEntitiesWithPercentilesAndEndingSoon() {
        AhSnapshotAggregator aggregator = new AhSnapshotAggregator(new MarketItemKeyService());
        Instant snapshot = Instant.ofEpochMilli(1_000L);
        long endingSoon = snapshot.toEpochMilli() + 5_000L;
        long notEndingSoon = snapshot.toEpochMilli() + 700_000L;

        List<AuctionMarketRecord> auctions = new ArrayList<>();
        auctions.add(bin("u1", "Axe", 100, endingSoon));
        auctions.add(bin("u2", "Axe", 200, endingSoon));
        auctions.add(bin("u3", "Axe", 300, notEndingSoon));
        auctions.add(bin("u4", "Axe", 400, notEndingSoon));
        auctions.add(bin("u5", "Axe", 500, notEndingSoon));
        auctions.add(bin("u6", "Axe", 600, notEndingSoon));
        auctions.add(bid("u7", "Axe", 150, endingSoon));
        auctions.add(bid("u8", "Axe", 250, endingSoon));
        auctions.add(bin("u9", "Bow", 90, endingSoon));
        auctions.add(new AuctionMarketRecord("claimed", "Bow", "weapon", "epic", 70, 0, 1, endingSoon, true, true, null, null));
        auctions.add(null);

        List<AhItemSnapshotEntity> entities = aggregator.aggregate(snapshot, auctions);

        assertEquals(2, entities.size());

        AhItemSnapshotEntity axe = entities.get(0);
        AhItemSnapshotEntity bow = entities.get(1);
        assertTrue(axe.getItemKey().compareTo(bow.getItemKey()) < 0);

        assertEquals("AXE|T:EPIC|C:WEAPON|P:-", axe.getItemKey());
        assertEquals(100L, axe.getBinLowest());
        assertEquals(300L, axe.getBinLowest5Mean());
        assertEquals(300L, axe.getBinP50());
        assertEquals(500L, axe.getBinP95());
        assertEquals(6, axe.getBinCount());
        assertEquals(150L, axe.getBidP50());
        assertEquals(4, axe.getEndingSoonCount());

        assertEquals("BOW|T:EPIC|C:WEAPON|P:-", bow.getItemKey());
        assertEquals(90L, bow.getBinLowest());
        assertEquals(1, bow.getBinCount());
    }

    @Test
    void aggregateSkipsGroupsWithoutBinPrices() {
        AhSnapshotAggregator aggregator = new AhSnapshotAggregator(new MarketItemKeyService());
        Instant snapshot = Instant.ofEpochMilli(1_000L);

        List<AuctionMarketRecord> auctions = List.of(
                bid("b1", "OnlyBid", 1_000L, snapshot.toEpochMilli() + 1_000L),
                bid("b2", "OnlyBid", 2_000L, snapshot.toEpochMilli() + 2_000L)
        );

        List<AhItemSnapshotEntity> entities = aggregator.aggregate(snapshot, auctions);

        assertTrue(entities.isEmpty());
    }

    @Test
    void aggregateUsesOnlyNonAdditionalListingsForPricingButCountsAllBins() {
        AhSnapshotAggregator aggregator = new AhSnapshotAggregator(new MarketItemKeyService());
        Instant snapshot = Instant.ofEpochMilli(1_000L);
        long endingSoon = snapshot.toEpochMilli() + 5_000L;

        List<AuctionMarketRecord> auctions = List.of(
                bin("base", "Axe", 100L, endingSoon),
                new AuctionMarketRecord(
                        "mod",
                        "Axe",
                        "weapon",
                        "epic",
                        1_000L,
                        0L,
                        1L,
                        endingSoon,
                        false,
                        true,
                        "Sharpness V",
                        "{\"internalname\":\"AXE\"}"
                )
        );

        List<AhItemSnapshotEntity> entities = aggregator.aggregate(snapshot, auctions);

        assertEquals(1, entities.size());
        AhItemSnapshotEntity axe = entities.getFirst();
        assertEquals("AXE|T:EPIC|C:WEAPON|P:-", axe.getItemKey());
        assertEquals(2, axe.getBinCount());
        assertEquals(100L, axe.getBinLowest());
        assertEquals(100L, axe.getBinLowest5Mean());
        assertEquals(100L, axe.getBinP50());
        assertEquals(100L, axe.getBinP95());
    }

    private static AuctionMarketRecord bin(String uuid, String name, long price, long endTs) {
        return new AuctionMarketRecord(
                uuid,
                name,
                "weapon",
                "epic",
                price,
                0L,
                1L,
                endTs,
                false,
                true,
                null,
                null
        );
    }

    private static AuctionMarketRecord bid(String uuid, String name, long highestBid, long endTs) {
        return new AuctionMarketRecord(
                uuid,
                name,
                "weapon",
                "epic",
                0L,
                highestBid,
                1L,
                endTs,
                false,
                false,
                null,
                null
        );
    }
}
