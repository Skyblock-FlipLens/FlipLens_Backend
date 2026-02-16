package com.skyblockflipper.backend.service.flipping;

import com.skyblockflipper.backend.model.market.UnifiedFlipInputSnapshot;

public record FlipCalculationContext(
        UnifiedFlipInputSnapshot marketSnapshot,
        double bazaarTaxRate,
        double auctionTaxMultiplier,
        boolean electionPartial
) {
    public static FlipCalculationContext standard(UnifiedFlipInputSnapshot snapshot) {
        return new FlipCalculationContext(snapshot, 0.0125D, 1.0D, false);
    }
}
