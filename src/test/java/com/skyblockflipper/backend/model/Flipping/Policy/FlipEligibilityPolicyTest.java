package com.skyblockflipper.backend.model.Flipping.Policy;

import com.skyblockflipper.backend.config.properties.FlippingModelProperties;
import com.skyblockflipper.backend.model.market.UnifiedFlipInputSnapshot;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlipEligibilityPolicyTest {

    private final FlipEligibilityPolicy policy = new FlipEligibilityPolicy();

    @Test
    void bazaarEligibilityUsesConfiguredEdgeThreshold() {
        assertTrue(policy.isBazaarFlipEligible(new UnifiedFlipInputSnapshot.BazaarQuote(
                100D, 103D, 0L, 0L, 0L, 0L, 0, 0
        )));
        assertFalse(policy.isBazaarFlipEligible(new UnifiedFlipInputSnapshot.BazaarQuote(
                100D, 101D, 0L, 0L, 0L, 0L, 0, 0
        )));
    }

    @Test
    void auctionEligibilityChecksSampleSizeAndEdge() {
        assertTrue(policy.isAuctionFlipEligible(new UnifiedFlipInputSnapshot.AuctionQuote(
                1_000_000L, 1_250_000L, 1_400_000L, 1_300_000D, 1_260_000D, 1_150_000D, 12
        )));
        assertFalse(policy.isAuctionFlipEligible(new UnifiedFlipInputSnapshot.AuctionQuote(
                1_000_000L, 1_250_000L, 1_400_000L, 1_300_000D, 1_260_000D, 1_150_000D, 9
        )));
        assertFalse(policy.isAuctionFlipEligible(new UnifiedFlipInputSnapshot.AuctionQuote(
                1_000_000L, 1_100_000L, 1_150_000L, 1_090_000D, 1_080_000D, 1_050_000D, 15
        )));
    }

    @Test
    void auctionEligibilityLegacyModeUsesLegacyThresholds() {
        FlippingModelProperties properties = new FlippingModelProperties();
        properties.setAuctionModelV2Enabled(false);
        FlipEligibilityPolicy legacyPolicy = new FlipEligibilityPolicy(properties);

        assertTrue(legacyPolicy.isAuctionFlipEligible(new UnifiedFlipInputSnapshot.AuctionQuote(
                100L, 120L, 140L, 106D, 106D, 106D, 3
        )));
        assertFalse(legacyPolicy.isAuctionFlipEligible(new UnifiedFlipInputSnapshot.AuctionQuote(
                100L, 120L, 140L, 106D, 106D, 106D, 2
        )));
    }
}
