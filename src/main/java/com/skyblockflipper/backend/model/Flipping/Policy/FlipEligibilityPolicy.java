package com.skyblockflipper.backend.model.Flipping.Policy;

import com.skyblockflipper.backend.model.market.UnifiedFlipInputSnapshot;
import org.springframework.stereotype.Component;

@Component
public class FlipEligibilityPolicy {

    private static final double MIN_BAZAAR_EDGE_RATIO = 1.015D;
    private static final double MIN_AUCTION_EDGE_RATIO = 1.10D;
    private static final int MIN_AUCTION_SAMPLE_SIZE = 10;

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
        if (quote == null) {
            return false;
        }
        if (quote.lowestStartingBid() <= 0L) {
            return false;
        }
        if (quote.sampleSize() < MIN_AUCTION_SAMPLE_SIZE) {
            return false;
        }
        double conservativeSellAnchor = resolveConservativeSellAnchor(quote);
        if (conservativeSellAnchor <= 0D) {
            return false;
        }
        return (conservativeSellAnchor / quote.lowestStartingBid()) >= MIN_AUCTION_EDGE_RATIO;
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
