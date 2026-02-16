package com.skyblockflipper.backend.service.flipping;

import com.skyblockflipper.backend.hypixel.HypixelClient;
import com.skyblockflipper.backend.model.market.UnifiedFlipInputSnapshot;
import com.skyblockflipper.backend.service.market.MarketSnapshotPersistenceService;
import org.springframework.stereotype.Service;
import tools.jackson.databind.JsonNode;

import java.time.Instant;

@Service
public class FlipCalculationContextService {

    private static final double STANDARD_BAZAAR_TAX = 0.0125D;
    private static final double STANDARD_AUCTION_TAX_MULTIPLIER = 1.0D;
    private static final double DERPY_AUCTION_TAX_MULTIPLIER = 4.0D;

    private final MarketSnapshotPersistenceService marketSnapshotPersistenceService;
    private final UnifiedFlipInputMapper unifiedFlipInputMapper;
    private final HypixelClient hypixelClient;

    public FlipCalculationContextService(MarketSnapshotPersistenceService marketSnapshotPersistenceService,
                                         UnifiedFlipInputMapper unifiedFlipInputMapper,
                                         HypixelClient hypixelClient) {
        this.marketSnapshotPersistenceService = marketSnapshotPersistenceService;
        this.unifiedFlipInputMapper = unifiedFlipInputMapper;
        this.hypixelClient = hypixelClient;
    }

    public FlipCalculationContext loadCurrentContext() {
        UnifiedFlipInputSnapshot marketSnapshot = marketSnapshotPersistenceService.latest()
                .map(unifiedFlipInputMapper::map)
                .orElseGet(() -> new UnifiedFlipInputSnapshot(Instant.now(), null, null));

        JsonNode election = hypixelClient.fetchElection();
        if (election == null) {
            return new FlipCalculationContext(
                    marketSnapshot,
                    STANDARD_BAZAAR_TAX,
                    STANDARD_AUCTION_TAX_MULTIPLIER,
                    true
            );
        }

        double auctionTaxMultiplier = hasDerpyQuadTaxes(election)
                ? DERPY_AUCTION_TAX_MULTIPLIER
                : STANDARD_AUCTION_TAX_MULTIPLIER;

        return new FlipCalculationContext(
                marketSnapshot,
                STANDARD_BAZAAR_TAX,
                auctionTaxMultiplier,
                false
        );
    }

    private boolean hasDerpyQuadTaxes(JsonNode election) {
        JsonNode mayor = resolveActiveMayor(election);
        if (mayor == null || mayor.isMissingNode()) {
            return false;
        }

        String mayorName = mayor.path("name").asString("");
        if (!"Derpy".equalsIgnoreCase(mayorName)) {
            return false;
        }

        JsonNode perks = mayor.path("perks");
        if (!perks.isArray()) {
            return false;
        }
        for (JsonNode perk : perks) {
            String perkName = perk.path("name").asString("");
            String description = perk.path("description").asString("");
            String lowerName = perkName.toLowerCase();
            String lowerDescription = description.toLowerCase();
            if (lowerName.contains("quad") && lowerName.contains("tax")) {
                return true;
            }
            if (lowerDescription.contains("quad") && lowerDescription.contains("tax")) {
                return true;
            }
        }
        return false;
    }

    private JsonNode resolveActiveMayor(JsonNode election) {
        JsonNode mayor = election.path("mayor");
        if (mayor.isObject()) {
            return mayor;
        }

        JsonNode currentMayor = election.path("current").path("mayor");
        if (currentMayor.isObject()) {
            return currentMayor;
        }

        JsonNode candidates = election.path("current").path("candidates");
        if (!candidates.isArray()) {
            return null;
        }
        for (JsonNode candidate : candidates) {
            if (candidate.path("elected").asBoolean(false)) {
                return candidate;
            }
        }
        return null;
    }
}
