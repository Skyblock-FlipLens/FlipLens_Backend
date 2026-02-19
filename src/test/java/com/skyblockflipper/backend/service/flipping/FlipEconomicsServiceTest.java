package com.skyblockflipper.backend.service.flipping;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class FlipEconomicsServiceTest {

    private final FlipEconomicsService economicsService = new FlipEconomicsService();

    @Test
    void computesRequiredCapitalFromHighestSource() {
        long requiredCapital = economicsService.computeRequiredCapital(1_000L, 4_000L, 3_000L);
        assertEquals(4_000L, requiredCapital);
    }

    @Test
    void computesRoiAndRoiPerHourWithGuards() {
        assertNull(economicsService.computeRoi(0L, 100L));
        assertNull(economicsService.computeRoiPerHour(null, 3600L));
        assertNull(economicsService.computeRoiPerHour(0.1D, 0L));

        Double roi = economicsService.computeRoi(2_000L, 500L);
        Double roiPerHour = economicsService.computeRoiPerHour(roi, 1800L);
        assertEquals(0.25D, roi);
        assertEquals(0.5D, roiPerHour);
    }

    @Test
    void computesBazaarFeesUsingCeil() {
        long fees = economicsService.computeBazaarSellFees(999L, 0.0125D);
        assertEquals(13L, fees);
    }

    @Test
    void computesAuctionFeesWithListingDurationAndClaimRules() {
        FlipEconomicsService.AuctionFeeBreakdown fees = economicsService.computeAuctionFees(20_000_000L, 12, 1.0D);
        assertEquals(400_000L, fees.listingFee());
        assertEquals(100L, fees.durationFee());
        assertEquals(200_000L, fees.claimTax());
        assertEquals(600_100L, fees.totalFee());
    }

    @Test
    void capsAuctionClaimTaxToLeaveAtLeastOneMillionCoins() {
        FlipEconomicsService.AuctionFeeBreakdown fees = economicsService.computeAuctionFees(1_000_100L, 12, 1.0D);
        assertEquals(100L, fees.claimTax());
    }
}
