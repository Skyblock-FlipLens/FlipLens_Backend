package com.skyblockflipper.backend.service.flipping;

import com.skyblockflipper.backend.hypixel.HypixelClient;
import com.skyblockflipper.backend.model.market.MarketSnapshot;
import com.skyblockflipper.backend.service.market.MarketSnapshotPersistenceService;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class FlipCalculationContextServiceTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void marksElectionAsPartialWhenEndpointUnavailable() {
        MarketSnapshotPersistenceService marketSnapshotService = mock(MarketSnapshotPersistenceService.class);
        HypixelClient hypixelClient = mock(HypixelClient.class);
        UnifiedFlipInputMapper inputMapper = new UnifiedFlipInputMapper();

        MarketSnapshot snapshot = new MarketSnapshot(Instant.parse("2026-02-16T10:00:00Z"), null, null);
        when(marketSnapshotService.latest()).thenReturn(Optional.of(snapshot));
        when(hypixelClient.fetchElection()).thenReturn(null);

        FlipCalculationContextService service = new FlipCalculationContextService(marketSnapshotService, inputMapper, hypixelClient);

        FlipCalculationContext context = service.loadCurrentContext();

        assertTrue(context.electionPartial());
        assertEquals(1.0D, context.auctionTaxMultiplier());
        assertEquals(0.0125D, context.bazaarTaxRate());
        assertEquals(Instant.parse("2026-02-16T10:00:00Z"), context.marketSnapshot().snapshotTimestamp());
    }

    @Test
    void appliesDerpyMultiplierWhenQuadTaxesPerkIsPresent() {
        MarketSnapshotPersistenceService marketSnapshotService = mock(MarketSnapshotPersistenceService.class);
        HypixelClient hypixelClient = mock(HypixelClient.class);
        UnifiedFlipInputMapper inputMapper = new UnifiedFlipInputMapper();

        when(marketSnapshotService.latest()).thenReturn(Optional.empty());
        when(hypixelClient.fetchElection()).thenReturn(objectMapper.readTree("""
                {
                  "success": true,
                  "mayor": {
                    "name": "Derpy",
                    "perks": [
                      {
                        "name": "QUAD TAXES!!!",
                        "description": "The Auction House has 4x the listing fee and tax."
                      }
                    ]
                  }
                }
                """));

        FlipCalculationContextService service = new FlipCalculationContextService(marketSnapshotService, inputMapper, hypixelClient);

        FlipCalculationContext context = service.loadCurrentContext();

        assertEquals(4.0D, context.auctionTaxMultiplier());
        assertFalse(context.electionPartial());
        assertNotNull(context.marketSnapshot());
    }
}
