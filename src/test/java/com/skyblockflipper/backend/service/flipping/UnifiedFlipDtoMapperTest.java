package com.skyblockflipper.backend.service.flipping;

import com.skyblockflipper.backend.api.UnifiedFlipDto;
import com.skyblockflipper.backend.model.Flipping.Constraint;
import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.model.Flipping.Flip;
import com.skyblockflipper.backend.model.Flipping.Step;
import com.skyblockflipper.backend.model.market.UnifiedFlipInputSnapshot;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UnifiedFlipDtoMapperTest {

    private final UnifiedFlipDtoMapper mapper = new UnifiedFlipDtoMapper(new ObjectMapper());

    @Test
    void mapsCoreUnifiedFieldsFromFlip() {
        UUID id = UUID.randomUUID();
        Flip flip = new Flip(
                id,
                FlipType.FORGE,
                List.of(
                        Step.forBuyMarketBased(30L, "{\"itemId\":\"ENCHANTED_DIAMOND\",\"amount\":2}"),
                        Step.forForgeFixed(3600L),
                        Step.forSellMarketBased(15L, "{\"itemId\":\"REFINED_DIAMOND\",\"amount\":1}")
                ),
                "REFINED_DIAMOND",
                List.of(Constraint.minCapital(150_000L))
        );

        UnifiedFlipDto dto = mapper.toDto(flip);

        assertEquals(id, dto.id());
        assertEquals(FlipType.FORGE, dto.flipType());
        assertEquals(3_645L, dto.durationSeconds());
        assertEquals(150_000L, dto.requiredCapital());
        assertEquals(0L, dto.expectedProfit());
        assertEquals(0D, dto.roi());
        assertEquals(0D, dto.roiPerHour());
        assertEquals(1, dto.inputItems().size());
        assertEquals("ENCHANTED_DIAMOND", dto.inputItems().getFirst().itemId());
        assertEquals(2, dto.inputItems().getFirst().amount());
        assertEquals(1, dto.outputItems().size());
        assertEquals("REFINED_DIAMOND", dto.outputItems().getFirst().itemId());
        assertEquals(2, dto.outputItems().getFirst().amount());
        assertTrue(dto.partial());
        assertFalse(dto.partialReasons().isEmpty());
        assertNotNull(dto.snapshotTimestamp());
        assertEquals(3, dto.steps().size());
        assertEquals(1, dto.constraints().size());
    }

    @Test
    void returnsNullForNullFlip() {
        assertNull(mapper.toDto(null));
    }

    @Test
    void computesBazaarProfitAndRoiUsingImplicitResultSell() {
        Flip flip = new Flip(
                UUID.randomUUID(),
                FlipType.CRAFTING,
                List.of(
                        Step.forBuyMarketBased(30L, "{\"itemId\":\"ENCHANTED_HAY_BLOCK\",\"amount\":2}"),
                        Step.forCraftInstant(10L)
                ),
                "TIGHTLY_TIED_HAY_BALE",
                List.of()
        );

        UnifiedFlipInputSnapshot snapshot = new UnifiedFlipInputSnapshot(
                Instant.parse("2026-02-16T10:00:00Z"),
                Map.of(
                        "ENCHANTED_HAY_BLOCK", new UnifiedFlipInputSnapshot.BazaarQuote(100D, 95D, 20_000L, 18_000L, 100, 90),
                        "TIGHTLY_TIED_HAY_BALE", new UnifiedFlipInputSnapshot.BazaarQuote(250D, 240D, 8_000L, 7_500L, 70, 65)
                ),
                Map.of()
        );

        UnifiedFlipDto dto = mapper.toDto(flip, FlipCalculationContext.standard(snapshot));

        assertEquals(200L, dto.requiredCapital());
        assertEquals(37L, dto.expectedProfit());
        assertEquals(3L, dto.fees());
        assertEquals(0.185D, dto.roi(), 1e-9);
        assertEquals(16.65D, dto.roiPerHour(), 1e-9);
        assertFalse(dto.partial());
        assertTrue(dto.partialReasons().isEmpty());
        assertEquals(Instant.parse("2026-02-16T10:00:00Z"), dto.snapshotTimestamp());
    }

    @Test
    void appliesAuctionFeesWithDerpyMultiplier() {
        Flip flip = new Flip(
                UUID.randomUUID(),
                FlipType.FORGE,
                List.of(
                        Step.forBuyMarketBased(30L, "{\"itemId\":\"ENCHANTED_DIAMOND_BLOCK\",\"amount\":1}"),
                        Step.forSellMarketBased(15L, "{\"itemId\":\"REFINED_DIAMOND\",\"amount\":1,\"durationHours\":12}")
                ),
                "REFINED_DIAMOND",
                List.of()
        );

        UnifiedFlipInputSnapshot snapshot = new UnifiedFlipInputSnapshot(
                Instant.parse("2026-02-16T10:00:00Z"),
                Map.of(
                        "ENCHANTED_DIAMOND_BLOCK", new UnifiedFlipInputSnapshot.BazaarQuote(1_000_000D, 999_000D, 50_000L, 49_000L, 100, 95)
                ),
                Map.of(
                        "REFINED_DIAMOND", new UnifiedFlipInputSnapshot.AuctionQuote(19_000_000L, 21_000_000L, 20_000_000D, 12)
                )
        );

        UnifiedFlipDto dto = mapper.toDto(
                flip,
                new FlipCalculationContext(snapshot, 0.0125D, 4.0D, false)
        );

        assertEquals(2_600_400L, dto.requiredCapital());
        assertEquals(17_199_600L, dto.expectedProfit());
        assertEquals(1_800_400L, dto.fees());
        assertEquals(6.61421319796954D, dto.roi(), 1e-9);
        assertEquals(529.137055837563D, dto.roiPerHour(), 1e-9);
        assertFalse(dto.partial());
    }

    @Test
    void explicitAuctionSellUsesAuctionPriceEvenWhenBazaarPriceExists() {
        Flip flip = new Flip(
                UUID.randomUUID(),
                FlipType.AUCTION,
                List.of(
                        Step.forBuyMarketBased(30L, "{\"itemId\":\"INPUT_ITEM\",\"amount\":1,\"market\":\"BAZAAR\"}"),
                        Step.forSellMarketBased(15L, "{\"itemId\":\"OUTPUT_ITEM\",\"amount\":1,\"market\":\"AUCTION\",\"durationHours\":12}")
                ),
                "OUTPUT_ITEM",
                List.of()
        );

        UnifiedFlipInputSnapshot snapshot = new UnifiedFlipInputSnapshot(
                Instant.parse("2026-02-16T11:00:00Z"),
                Map.of(
                        "INPUT_ITEM", new UnifiedFlipInputSnapshot.BazaarQuote(50D, 49D, 5000L, 4800L, 40, 35),
                        "OUTPUT_ITEM", new UnifiedFlipInputSnapshot.BazaarQuote(100D, 100D, 5000L, 4800L, 40, 35)
                ),
                Map.of(
                        "OUTPUT_ITEM", new UnifiedFlipInputSnapshot.AuctionQuote(150L, 220L, 200D, 15)
                )
        );

        UnifiedFlipDto dto = mapper.toDto(flip, FlipCalculationContext.standard(snapshot));

        assertEquals(152L, dto.requiredCapital());
        assertEquals(48L, dto.expectedProfit());
        assertEquals(102L, dto.fees());
        assertFalse(dto.partial());
    }

    @Test
    void npcBuySourceUsesNpcUnitPriceInCapitalAndProfit() {
        Flip flip = new Flip(
                UUID.randomUUID(),
                FlipType.CRAFTING,
                List.of(
                        Step.forBuyMarketBased(30L, "{\"itemId\":\"NPC_ITEM\",\"amount\":2,\"market\":\"NPC\",\"npcPrice\":100}"),
                        Step.forSellMarketBased(15L, "{\"itemId\":\"NPC_ITEM\",\"amount\":2,\"market\":\"BAZAAR\"}")
                ),
                "NPC_ITEM",
                List.of()
        );

        UnifiedFlipInputSnapshot snapshot = new UnifiedFlipInputSnapshot(
                Instant.parse("2026-02-16T11:00:00Z"),
                Map.of(
                        "NPC_ITEM", new UnifiedFlipInputSnapshot.BazaarQuote(170D, 160D, 2000L, 2200L, 25, 20)
                ),
                Map.of()
        );

        UnifiedFlipDto dto = mapper.toDto(flip, FlipCalculationContext.standard(snapshot));

        assertEquals(200L, dto.requiredCapital());
        assertEquals(116L, dto.expectedProfit());
        assertEquals(4L, dto.fees());
        assertFalse(dto.partial());
    }

    @Test
    void buyWithoutSourceMarksPartialWhenBothBazaarAndAuctionExist() {
        Flip flip = new Flip(
                UUID.randomUUID(),
                FlipType.CRAFTING,
                List.of(
                        Step.forBuyMarketBased(30L, "{\"itemId\":\"AMBIGUOUS_ITEM\",\"amount\":1}"),
                        Step.forSellMarketBased(15L, "{\"itemId\":\"AMBIGUOUS_ITEM\",\"amount\":1,\"market\":\"BAZAAR\"}")
                ),
                "AMBIGUOUS_ITEM",
                List.of()
        );

        UnifiedFlipInputSnapshot snapshot = new UnifiedFlipInputSnapshot(
                Instant.parse("2026-02-16T11:00:00Z"),
                Map.of(
                        "AMBIGUOUS_ITEM", new UnifiedFlipInputSnapshot.BazaarQuote(100D, 90D, 2000L, 2200L, 25, 20)
                ),
                Map.of(
                        "AMBIGUOUS_ITEM", new UnifiedFlipInputSnapshot.AuctionQuote(80L, 120L, 110D, 10)
                )
        );

        UnifiedFlipDto dto = mapper.toDto(flip, FlipCalculationContext.standard(snapshot));

        assertTrue(dto.partial());
        assertTrue(dto.partialReasons().contains("AMBIGUOUS_INPUT_MARKET_SOURCE:AMBIGUOUS_ITEM"));
    }
}
