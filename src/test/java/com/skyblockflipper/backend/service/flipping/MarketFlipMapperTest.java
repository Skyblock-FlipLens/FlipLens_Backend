package com.skyblockflipper.backend.service.flipping;

import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.model.Flipping.Flip;
import com.skyblockflipper.backend.model.Flipping.Step;
import com.skyblockflipper.backend.model.market.UnifiedFlipInputSnapshot;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MarketFlipMapperTest {

    private final MarketFlipMapper mapper = new MarketFlipMapper(new ObjectMapper());

    @Test
    void generatesAuctionAndBazaarFlipsForViableSpreads() {
        UnifiedFlipInputSnapshot snapshot = new UnifiedFlipInputSnapshot(
                Instant.parse("2026-02-19T20:00:00Z"),
                Map.of(
                        "ENCHANTED_SUGAR", new UnifiedFlipInputSnapshot.BazaarQuote(
                                100D, 103D, 2000L, 2100L, 100_000L, 101_000L, 20, 21
                        )
                ),
                Map.of(
                        "Hyperion", new UnifiedFlipInputSnapshot.AuctionQuote(1_000_000L, 1_300_000L, 1_200_000D, 12)
                )
        );

        List<Flip> flips = mapper.fromMarketSnapshot(snapshot);

        assertEquals(2, flips.size());
        assertEquals(1, flips.stream().filter(flip -> flip.getFlipType() == FlipType.BAZAAR).count());
        assertEquals(1, flips.stream().filter(flip -> flip.getFlipType() == FlipType.AUCTION).count());

        Flip auctionFlip = flips.stream().filter(flip -> flip.getFlipType() == FlipType.AUCTION).findFirst().orElseThrow();
        Step auctionSellStep = auctionFlip.getSteps().getLast();
        assertTrue(auctionSellStep.getParamsJson().contains("\"durationHours\":12"));
        assertTrue(auctionSellStep.getParamsJson().contains("\"market\":\"AUCTION\""));
    }

    @Test
    void filtersWeakSpreadCandidates() {
        UnifiedFlipInputSnapshot snapshot = new UnifiedFlipInputSnapshot(
                Instant.parse("2026-02-19T20:00:00Z"),
                Map.of(
                        "ENCHANTED_SUGAR", new UnifiedFlipInputSnapshot.BazaarQuote(
                                100D, 100.5D, 2000L, 2100L, 100_000L, 101_000L, 20, 21
                        )
                ),
                Map.of(
                        "Hyperion", new UnifiedFlipInputSnapshot.AuctionQuote(1_000_000L, 1_150_000L, 1_020_000D, 12)
                )
        );

        List<Flip> flips = mapper.fromMarketSnapshot(snapshot);
        assertTrue(flips.isEmpty());
    }

    @Test
    void requiresMinimumAuctionSampleSize() {
        UnifiedFlipInputSnapshot snapshot = new UnifiedFlipInputSnapshot(
                Instant.parse("2026-02-19T20:00:00Z"),
                Map.of(),
                Map.of(
                        "Hyperion", new UnifiedFlipInputSnapshot.AuctionQuote(1_000_000L, 2_000_000L, 1_500_000D, 2)
                )
        );

        List<Flip> flips = mapper.fromMarketSnapshot(snapshot);
        assertTrue(flips.isEmpty());
    }
}
