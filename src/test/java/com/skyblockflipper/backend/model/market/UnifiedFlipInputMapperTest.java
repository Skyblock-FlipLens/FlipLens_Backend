package com.skyblockflipper.backend.model.market;

import com.skyblockflipper.backend.service.flipping.UnifiedFlipInputMapper;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UnifiedFlipInputMapperTest {

    private final UnifiedFlipInputMapper mapper = new UnifiedFlipInputMapper();

    @Test
    void mapAggregatesAuctionAndBazaarQuotes() {
        Instant timestamp = Instant.parse("2026-02-15T12:30:00Z");
        List<AuctionMarketRecord> auctions = List.of(
                new AuctionMarketRecord("a1", "ENCHANTED_DIAMOND", "misc", "RARE", 100L, 0L, 1L, 2L, false, true),
                new AuctionMarketRecord("a2", "ENCHANTED_DIAMOND", "misc", "RARE", 80L, 120L, 1L, 2L, true, true),
                new AuctionMarketRecord("a3", "HOT_POTATO_BOOK", "misc", "EPIC", 50L, 0L, 1L, 2L, false, true),
                new AuctionMarketRecord("a4", "ENCHANTED_DIAMOND", "misc", "RARE", 1L, 500L, 1L, 2L, false, false)
        );
        Map<String, BazaarMarketRecord> bazaar = Map.of(
                "ENCHANTED_DIAMOND", new BazaarMarketRecord("ENCHANTED_DIAMOND", 10.0, 9.0, 100, 90, 1000, 900, 4, 3)
        );
        MarketSnapshot snapshot = new MarketSnapshot(timestamp, auctions, bazaar);

        UnifiedFlipInputSnapshot input = mapper.map(snapshot);

        assertEquals(timestamp, input.snapshotTimestamp());
        assertEquals(1, input.bazaarQuotes().size());
        assertEquals(2, input.auctionQuotesByItem().size());

        UnifiedFlipInputSnapshot.AuctionQuote diamondQuote = input.auctionQuotesByItem().get("ENCHANTED_DIAMOND");
        assertNotNull(diamondQuote);
        assertEquals(80L, diamondQuote.lowestStartingBid());
        assertEquals(100L, diamondQuote.secondLowestStartingBid());
        assertEquals(100L, diamondQuote.highestObservedBid());
        assertEquals(90.0, diamondQuote.averageObservedPrice());
        assertEquals(80.0, diamondQuote.medianObservedPrice());
        assertEquals(80.0, diamondQuote.p25ObservedPrice());
        assertEquals(2, diamondQuote.sampleSize());
    }

    @Test
    void mapHandlesNullSnapshot() {
        UnifiedFlipInputSnapshot input = mapper.map(null);

        assertNotNull(input.snapshotTimestamp());
        assertTrue(input.bazaarQuotes().isEmpty());
        assertTrue(input.auctionQuotesByItem().isEmpty());
    }

    @Test
    void mapGroupsComparableAuctionItemsUsingInternalIdFromExtra() {
        Instant timestamp = Instant.parse("2026-02-15T12:30:00Z");
        List<AuctionMarketRecord> auctions = List.of(
                new AuctionMarketRecord("a1", "[Lvl 1] Squid", "pet", "EPIC", 100_000L, 0L, 1L, 2L, false, true, null, "{\"internalname\":\"SQUID;1\"}"),
                new AuctionMarketRecord("a2", "[Lvl 1] Squid ", "pet", "EPIC", 110_000L, 0L, 1L, 2L, false, true, null, "{\"internalname\":\"SQUID;1\"}")
        );
        MarketSnapshot snapshot = new MarketSnapshot(timestamp, auctions, Map.of());

        UnifiedFlipInputSnapshot input = mapper.map(snapshot);

        assertEquals(1, input.auctionQuotesByItem().size());
        UnifiedFlipInputSnapshot.AuctionQuote quote = input.auctionQuotesByItem().values().stream().findFirst().orElseThrow();
        assertEquals(100_000L, quote.lowestStartingBid());
        assertEquals(110_000L, quote.secondLowestStartingBid());
        assertEquals(2, quote.sampleSize());
    }
}
