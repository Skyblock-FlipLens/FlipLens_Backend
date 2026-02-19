package com.skyblockflipper.backend.service.flipping;

import org.springframework.stereotype.Component;

@Component
public class FlipEconomicsService {

    private static final long CLAIM_TAX_MIN_REMAINING_COINS = 1_000_000L;

    public long computeRequiredCapital(long minCapitalConstraint, long currentPriceBaseline, long peakExposure) {
        return Math.max(minCapitalConstraint, Math.max(currentPriceBaseline, peakExposure));
    }

    public long computeExpectedProfit(long grossRevenue, long totalInputCost, long totalFees) {
        return grossRevenue - totalInputCost - totalFees;
    }

    public Double computeRoi(long requiredCapital, long expectedProfit) {
        if (requiredCapital <= 0L) {
            return null;
        }
        return (double) expectedProfit / requiredCapital;
    }

    public Double computeRoiPerHour(Double roi, long durationSeconds) {
        if (roi == null || durationSeconds <= 0L) {
            return null;
        }
        return roi * (3600D / durationSeconds);
    }

    public long computeBazaarSellFees(long grossRevenue, double bazaarTaxRate) {
        return ceilToLong(grossRevenue * bazaarTaxRate);
    }

    public AuctionFeeBreakdown computeAuctionFees(long grossRevenue, int durationHours, double taxMultiplier) {
        double listingRate = resolveAuctionListingRate(grossRevenue);
        long baseListingFee = ceilToLong(grossRevenue * listingRate);
        long listingFee = ceilToLong(baseListingFee * taxMultiplier);

        long durationFee = auctionDurationFee(durationHours);

        long claimTax = computeAuctionClaimTax(grossRevenue, taxMultiplier);

        long totalFee = listingFee + durationFee + claimTax;
        return new AuctionFeeBreakdown(listingFee, durationFee, claimTax, totalFee);
    }

    private long computeAuctionClaimTax(long grossRevenue, double taxMultiplier) {
        if (grossRevenue <= CLAIM_TAX_MIN_REMAINING_COINS) {
            return 0L;
        }
        long baseClaimTax = ceilToLong(grossRevenue * 0.01D);
        long scaledClaimTax = ceilToLong(baseClaimTax * taxMultiplier);
        long cap = Math.max(0L, grossRevenue - CLAIM_TAX_MIN_REMAINING_COINS);
        return Math.min(scaledClaimTax, cap);
    }

    private double resolveAuctionListingRate(long grossRevenue) {
        if (grossRevenue < 10_000_000L) {
            return 0.01D;
        }
        if (grossRevenue < 100_000_000L) {
            return 0.02D;
        }
        return 0.025D;
    }

    private long auctionDurationFee(int durationHours) {
        return switch (durationHours) {
            case 1 -> 20L;
            case 6 -> 45L;
            case 12 -> 100L;
            case 24 -> 350L;
            case 48 -> 1200L;
            default -> 100L;
        };
    }

    private long ceilToLong(double value) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return 0L;
        }
        return (long) Math.ceil(value);
    }

    public record AuctionFeeBreakdown(
            long listingFee,
            long durationFee,
            long claimTax,
            long totalFee
    ) {
    }
}
