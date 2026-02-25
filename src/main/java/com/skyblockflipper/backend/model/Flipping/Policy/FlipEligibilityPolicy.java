package com.skyblockflipper.backend.model.Flipping.Policy;

import com.skyblockflipper.backend.config.properties.FlippingModelProperties;
import com.skyblockflipper.backend.model.market.UnifiedFlipInputSnapshot;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class FlipEligibilityPolicy {

    private static final double MIN_BAZAAR_EDGE_RATIO = 1.015D;
    private static final double MIN_AUCTION_EDGE_RATIO = 1.10D;
    private static final int MIN_AUCTION_SAMPLE_SIZE = 10;
    private static final double LEGACY_MIN_AUCTION_EDGE_RATIO = 1.05D;
    private static final int LEGACY_MIN_AUCTION_SAMPLE_SIZE = 3;
    private final FlippingModelProperties flippingModelProperties;

    public FlipEligibilityPolicy() {
        this(new FlippingModelProperties());
    }

    @Autowired
    public FlipEligibilityPolicy(FlippingModelProperties flippingModelProperties) {
        this.flippingModelProperties = flippingModelProperties == null
                ? new FlippingModelProperties()
                : flippingModelProperties;
    }

    public boolean isBazaarFlipEligible(UnifiedFlipInputSnapshot.BazaarQuote quote) {
        if (quote == null) {
            return false;
        }
        if (quote.buyPrice() <= 0D || quote.sellPrice() <= 0D) {
            return false;
        }
        return (quote.sellPrice() / quote.buyPrice()) >= MIN_BAZAAR_EDGE_RATIO;
    }

    public boolean isAuctionFlipEligible(UnifiedFlipInputSnapshot.AuctionQuote quote) {
        return auctionIneligibilityReason(quote) == null;
    }

    public String auctionIneligibilityReason(UnifiedFlipInputSnapshot.AuctionQuote quote) {
        if (quote == null) {
            return "QUOTE_NULL";
        }
        if (quote.lowestStartingBid() <= 0L) {
            return "NON_POSITIVE_LOWEST_STARTING_BID";
        }
        if (flippingModelProperties.isAuctionModelV2Enabled()) {
            if (quote.sampleSize() < MIN_AUCTION_SAMPLE_SIZE) {
                return "INSUFFICIENT_AUCTION_SAMPLE_SIZE";
            }
            double conservativeSellAnchor = resolveConservativeSellAnchor(quote);
            if (conservativeSellAnchor <= 0D) {
                return "NON_POSITIVE_SELL_ANCHOR";
            }
            return (conservativeSellAnchor / quote.lowestStartingBid()) >= MIN_AUCTION_EDGE_RATIO
                    ? null
                    : "INSUFFICIENT_AUCTION_EDGE";
        }

        if (quote.sampleSize() < LEGACY_MIN_AUCTION_SAMPLE_SIZE) {
            return "INSUFFICIENT_AUCTION_SAMPLE_SIZE";
        }
        double legacyAnchor = quote.averageObservedPrice() > 0D
                ? quote.averageObservedPrice()
                : Math.max(quote.secondLowestStartingBid(), quote.highestObservedBid());
        if (legacyAnchor <= 0D) {
            return "NON_POSITIVE_SELL_ANCHOR";
        }
        return (legacyAnchor / quote.lowestStartingBid()) >= LEGACY_MIN_AUCTION_EDGE_RATIO
                ? null
                : "INSUFFICIENT_AUCTION_EDGE";
    }

    private double resolveConservativeSellAnchor(UnifiedFlipInputSnapshot.AuctionQuote quote) {
        double anchor = Double.MAX_VALUE;
        if (quote.p25ObservedPrice() > 0D) {
            anchor = Math.min(anchor, quote.p25ObservedPrice());
        }
        if (quote.p25ObservedPrice() <= 0D && quote.secondLowestStartingBid() > 0L && quote.medianObservedPrice() > 0D) {
            anchor = Math.min(anchor, Math.min(quote.secondLowestStartingBid(), quote.medianObservedPrice() * 0.97D));
        } else if (quote.secondLowestStartingBid() > 0L) {
            anchor = Math.min(anchor, quote.secondLowestStartingBid());
        }
        if (anchor != Double.MAX_VALUE) {
            return anchor;
        }
        if (quote.medianObservedPrice() > 0D) {
            return quote.medianObservedPrice() * 0.97D;
        }
        if (quote.averageObservedPrice() > 0D) {
            return quote.averageObservedPrice() * 0.95D;
        }
        return 0D;
    }
}
