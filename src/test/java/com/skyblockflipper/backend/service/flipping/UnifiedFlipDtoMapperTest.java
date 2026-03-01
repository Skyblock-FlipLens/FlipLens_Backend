package com.skyblockflipper.backend.service.flipping;

import com.skyblockflipper.backend.api.dto.UnifiedFlipDto;
import com.skyblockflipper.backend.model.Flipping.Constraint;
import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.model.Flipping.Flip;
import com.skyblockflipper.backend.model.Flipping.Step;
import com.skyblockflipper.backend.model.market.UnifiedFlipInputSnapshot;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.lang.reflect.Method;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class UnifiedFlipDtoMapperTest {

    private final UnifiedFlipDtoMapper mapper = new UnifiedFlipDtoMapper(new ObjectMapper(), new FlipRiskScorer());

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
        assertEquals(1, dto.outputItems().getFirst().amount());
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
                        "ENCHANTED_HAY_BLOCK", new UnifiedFlipInputSnapshot.BazaarQuote(100D, 95D, 20_000L, 18_000L, 1_680_000L, 1_512_000L, 100, 90),
                        "TIGHTLY_TIED_HAY_BALE", new UnifiedFlipInputSnapshot.BazaarQuote(250D, 240D, 8_000L, 7_500L, 672_000L, 630_000L, 70, 65)
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
                        "ENCHANTED_DIAMOND_BLOCK", new UnifiedFlipInputSnapshot.BazaarQuote(1_000_000D, 999_000D, 50_000L, 49_000L, 4_200_000L, 4_116_000L, 100, 95)
                ),
                Map.of(
                        "REFINED_DIAMOND", new UnifiedFlipInputSnapshot.AuctionQuote(19_000_000L, 21_000_000L, 20_000_000D, 12)
                )
        );

        UnifiedFlipDto dto = mapper.toDto(
                flip,
                new FlipCalculationContext(snapshot, 0.0125D, 4.0D, false, FlipScoreFeatureSet.empty())
        );

        assertEquals(2_600_100L, dto.requiredCapital());
        assertEquals(16_599_900L, dto.expectedProfit());
        assertEquals(2_400_100L, dto.fees());
        assertEquals(16_599_900D / 2_600_100D, dto.roi(), 1e-6);
        assertEquals((16_599_900D / 2_600_100D) * 80D, dto.roiPerHour(), 1e-3);
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
                        "INPUT_ITEM", new UnifiedFlipInputSnapshot.BazaarQuote(50D, 49D, 5000L, 4800L, 420_000L, 403_200L, 40, 35),
                        "OUTPUT_ITEM", new UnifiedFlipInputSnapshot.BazaarQuote(100D, 100D, 5000L, 4800L, 420_000L, 403_200L, 40, 35)
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
                        "NPC_ITEM", new UnifiedFlipInputSnapshot.BazaarQuote(170D, 160D, 2000L, 2200L, 168_000L, 184_800L, 25, 20)
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
                        "AMBIGUOUS_ITEM", new UnifiedFlipInputSnapshot.BazaarQuote(100D, 90D, 2000L, 2200L, 168_000L, 184_800L, 25, 20)
                ),
                Map.of(
                        "AMBIGUOUS_ITEM", new UnifiedFlipInputSnapshot.AuctionQuote(80L, 120L, 110D, 10)
                )
        );

        UnifiedFlipDto dto = mapper.toDto(flip, FlipCalculationContext.standard(snapshot));

        assertTrue(dto.partial());
        assertTrue(dto.partialReasons().contains("AMBIGUOUS_INPUT_MARKET_SOURCE:AMBIGUOUS_ITEM"));
    }
    @Test
    void computesPositionAwareLiquidityAndRiskScores() {
        Flip flip = new Flip(
                UUID.randomUUID(),
                FlipType.CRAFTING,
                List.of(
                        Step.forBuyMarketBased(30L, "{\"itemId\":\"FAST_INPUT\",\"amount\":10,\"market\":\"BAZAAR\"}"),
                        Step.forSellMarketBased(15L, "{\"itemId\":\"SLOW_OUTPUT\",\"amount\":5,\"market\":\"BAZAAR\"}")
                ),
                "SLOW_OUTPUT",
                List.of()
        );

        UnifiedFlipInputSnapshot snapshot = new UnifiedFlipInputSnapshot(
                Instant.parse("2026-02-16T12:00:00Z"),
                Map.of(
                        "FAST_INPUT", new UnifiedFlipInputSnapshot.BazaarQuote(100D, 95D, 10_000L, 12_000L, 840_000L, 1_008_000L, 80, 90),
                        "SLOW_OUTPUT", new UnifiedFlipInputSnapshot.BazaarQuote(500D, 460D, 100L, 80L, 840L, 336L, 5, 4)
                ),
                Map.of()
        );

        UnifiedFlipDto dto = mapper.toDto(flip, FlipCalculationContext.standard(snapshot));

        assertNotNull(dto.liquidityScore());
        assertNotNull(dto.riskScore());
        assertTrue(dto.liquidityScore() >= 0D && dto.liquidityScore() <= 100D);
        assertTrue(dto.riskScore() >= 0D && dto.riskScore() <= 100D);
        assertTrue(dto.riskScore() > dto.liquidityScore());
    }

    @Test
    void auctionLegDurationsContributeToExposureRisk() {
        UnifiedFlipInputSnapshot snapshot = new UnifiedFlipInputSnapshot(
                Instant.parse("2026-02-16T12:00:00Z"),
                Map.of(),
                Map.of(
                        "INPUT_ITEM", new UnifiedFlipInputSnapshot.AuctionQuote(1_000L, 1_200L, 1_100D, 10),
                        "OUTPUT_ITEM", new UnifiedFlipInputSnapshot.AuctionQuote(2_000L, 2_200L, 2_100D, 10)
                )
        );

        Flip shortAuctionFlip = new Flip(
                UUID.randomUUID(),
                FlipType.AUCTION,
                List.of(
                        Step.forBuyMarketBased(30L, "{\"itemId\":\"INPUT_ITEM\",\"amount\":1,\"market\":\"AUCTION\"}"),
                        Step.forSellMarketBased(15L, "{\"itemId\":\"OUTPUT_ITEM\",\"amount\":1,\"market\":\"AUCTION\",\"durationHours\":1}")
                ),
                "OUTPUT_ITEM",
                List.of()
        );
        Flip longAuctionFlip = new Flip(
                UUID.randomUUID(),
                FlipType.AUCTION,
                List.of(
                        Step.forBuyMarketBased(30L, "{\"itemId\":\"INPUT_ITEM\",\"amount\":1,\"market\":\"AUCTION\"}"),
                        Step.forSellMarketBased(15L, "{\"itemId\":\"OUTPUT_ITEM\",\"amount\":1,\"market\":\"AUCTION\",\"durationHours\":48}")
                ),
                "OUTPUT_ITEM",
                List.of()
        );

        UnifiedFlipDto shortDurationDto = mapper.toDto(shortAuctionFlip, FlipCalculationContext.standard(snapshot));
        UnifiedFlipDto longDurationDto = mapper.toDto(longAuctionFlip, FlipCalculationContext.standard(snapshot));

        assertNotNull(shortDurationDto.riskScore());
        assertNotNull(longDurationDto.riskScore());
        assertTrue(longDurationDto.riskScore() > shortDurationDto.riskScore());
    }

    @Test
    void marksPartialAndInflatesInputCostWhenBuyDepthIsInsufficient() {
        Flip flip = new Flip(
                UUID.randomUUID(),
                FlipType.CRAFTING,
                List.of(
                        Step.forBuyMarketBased(30L, "{\"itemId\":\"DEPTH_ITEM\",\"amount\":100,\"market\":\"BAZAAR\"}"),
                        Step.forSellMarketBased(15L, "{\"itemId\":\"DEPTH_ITEM\",\"amount\":100,\"market\":\"BAZAAR\"}")
                ),
                "DEPTH_ITEM",
                List.of()
        );

        UnifiedFlipInputSnapshot snapshot = new UnifiedFlipInputSnapshot(
                Instant.parse("2026-02-16T12:00:00Z"),
                Map.of(
                        "DEPTH_ITEM", new UnifiedFlipInputSnapshot.BazaarQuote(
                                100D, 110D, 20_000L, 10L, 1_680_000L, 840L, 100, 5
                        )
                ),
                Map.of()
        );

        UnifiedFlipDto dto = mapper.toDto(flip, FlipCalculationContext.standard(snapshot));

        assertTrue(dto.partial());
        assertTrue(dto.partialReasons().contains("INSUFFICIENT_INPUT_DEPTH:DEPTH_ITEM"));
        assertTrue(dto.requiredCapital() > 10_000L);
    }

    @Test
    void reducesOutputProfitWhenSellDepthIsInsufficient() {
        Flip flip = new Flip(
                UUID.randomUUID(),
                FlipType.CRAFTING,
                List.of(
                        Step.forBuyMarketBased(30L, "{\"itemId\":\"DEPTH_ITEM\",\"amount\":100,\"market\":\"BAZAAR\"}"),
                        Step.forSellMarketBased(15L, "{\"itemId\":\"DEPTH_ITEM\",\"amount\":100,\"market\":\"BAZAAR\"}")
                ),
                "DEPTH_ITEM",
                List.of()
        );

        UnifiedFlipInputSnapshot deepSnapshot = new UnifiedFlipInputSnapshot(
                Instant.parse("2026-02-16T12:00:00Z"),
                Map.of(
                        "DEPTH_ITEM", new UnifiedFlipInputSnapshot.BazaarQuote(
                                100D, 110D, 50_000L, 50_000L, 4_200_000L, 4_200_000L, 200, 200
                        )
                ),
                Map.of()
        );
        UnifiedFlipInputSnapshot shallowSellSnapshot = new UnifiedFlipInputSnapshot(
                Instant.parse("2026-02-16T12:00:00Z"),
                Map.of(
                        "DEPTH_ITEM", new UnifiedFlipInputSnapshot.BazaarQuote(
                                100D, 110D, 10L, 50_000L, 840L, 4_200_000L, 5, 200
                        )
                ),
                Map.of()
        );

        UnifiedFlipDto deepDto = mapper.toDto(flip, FlipCalculationContext.standard(deepSnapshot));
        UnifiedFlipDto shallowDto = mapper.toDto(flip, FlipCalculationContext.standard(shallowSellSnapshot));

        assertTrue(shallowDto.partialReasons().contains("INSUFFICIENT_OUTPUT_DEPTH:DEPTH_ITEM"));
        assertTrue(shallowDto.expectedProfit() < deepDto.expectedProfit());
    }

    @Test
    void parseItemStackHandlesAmountMarketAndNpcPriceVariants() throws Exception {
        Object parsed = invokePrivate(
                mapper,
                "parseItemStack",
                new Class<?>[]{String.class},
                "{\"itemId\":\"NPC_ITEM\",\"amount\":\" 3 \",\"market\":\"npc_shop\",\"coinCost\":\"123.5\"}"
        );

        assertNotNull(parsed);
        assertEquals("NPC_ITEM", invokeAccessor(parsed, "itemId"));
        assertEquals(3, invokeAccessor(parsed, "amount"));
        assertEquals("NPC", String.valueOf(invokeAccessor(parsed, "marketPreference")));
        assertEquals(123.5D, (Double) invokeAccessor(parsed, "npcUnitPrice"), 1e-9);
    }

    @Test
    void parseItemStackFallsBackToDefaultsForUnsupportedAmountAndSourceField() throws Exception {
        Object parsed = invokePrivate(
                mapper,
                "parseItemStack",
                new Class<?>[]{String.class},
                "{\"itemId\":\"A\",\"amount\":\"not-a-number\",\"source\":\"AUCTION\"}"
        );

        assertNotNull(parsed);
        assertEquals("A", invokeAccessor(parsed, "itemId"));
        assertEquals(1, invokeAccessor(parsed, "amount"));
        assertEquals("AUCTION", String.valueOf(invokeAccessor(parsed, "marketPreference")));
        assertEquals(null, invokeAccessor(parsed, "npcUnitPrice"));
    }

    @Test
    void parseItemStackReturnsNullForInvalidJsonOrMissingItemId() throws Exception {
        Object malformed = invokePrivate(
                mapper,
                "parseItemStack",
                new Class<?>[]{String.class},
                "{bad-json"
        );
        Object missingItem = invokePrivate(
                mapper,
                "parseItemStack",
                new Class<?>[]{String.class},
                "{\"amount\":2}"
        );
        Object blankItem = invokePrivate(
                mapper,
                "parseItemStack",
                new Class<?>[]{String.class},
                "{\"itemId\":\"   \"}"
        );

        assertNull(malformed);
        assertNull(missingItem);
        assertNull(blankItem);
    }

    @Test
    void resolveConservativeAuctionSellUnitPriceUsesExpectedFallbackOrder() throws Exception {
        Method method = UnifiedFlipDtoMapper.class.getDeclaredMethod(
                "resolveConservativeAuctionSellUnitPrice",
                UnifiedFlipInputSnapshot.AuctionQuote.class
        );
        method.setAccessible(true);

        double withP25AndSecond = (double) method.invoke(
                mapper,
                new UnifiedFlipInputSnapshot.AuctionQuote(80L, 90L, 150L, 120D, 110D, 100D, 10)
        );
        double withP25Only = (double) method.invoke(
                mapper,
                new UnifiedFlipInputSnapshot.AuctionQuote(0L, 0L, 150L, 120D, 110D, 100D, 10)
        );
        double withSecondAndMedian = (double) method.invoke(
                mapper,
                new UnifiedFlipInputSnapshot.AuctionQuote(1L, 120L, 150L, 120D, 100D, 0D, 10)
        );
        double withSecondOnly = (double) method.invoke(
                mapper,
                new UnifiedFlipInputSnapshot.AuctionQuote(1L, 120L, 150L, 0D, 0D, 0D, 10)
        );
        double withMedianOnly = (double) method.invoke(
                mapper,
                new UnifiedFlipInputSnapshot.AuctionQuote(0L, 0L, 150L, 0D, 100D, 0D, 10)
        );
        double withAverageOnly = (double) method.invoke(
                mapper,
                new UnifiedFlipInputSnapshot.AuctionQuote(0L, 0L, 150L, 200D, 0D, 0D, 10)
        );
        double withHighestOnly = (double) method.invoke(
                mapper,
                new UnifiedFlipInputSnapshot.AuctionQuote(0L, 0L, 150L, 0D, 0D, 0D, 10)
        );

        assertEquals(90D, withP25AndSecond, 1e-9);
        assertEquals(100D, withP25Only, 1e-9);
        assertEquals(97D, withSecondAndMedian, 1e-9);
        assertEquals(120D, withSecondOnly, 1e-9);
        assertEquals(97D, withMedianOnly, 1e-9);
        assertEquals(190D, withAverageOnly, 1e-9);
        assertEquals(150D, withHighestOnly, 1e-9);
    }

    @Test
    void unsupportedAndInvalidAuctionDurationFallBackToDefaultAndMarkPartial() {
        UnifiedFlipInputSnapshot snapshot = new UnifiedFlipInputSnapshot(
                Instant.parse("2026-02-16T14:00:00Z"),
                Map.of(),
                Map.of("ITEM", new UnifiedFlipInputSnapshot.AuctionQuote(10_000L, 12_000L, 11_000D, 12))
        );
        Flip unsupportedDuration = new Flip(
                UUID.randomUUID(),
                FlipType.AUCTION,
                List.of(Step.forSellMarketBased(15L, "{\"itemId\":\"ITEM\",\"amount\":1,\"market\":\"AUCTION\",\"durationHours\":5}")),
                "ITEM",
                List.of()
        );
        Flip invalidDuration = new Flip(
                UUID.randomUUID(),
                FlipType.AUCTION,
                List.of(Step.forSellMarketBased(15L, "{\"itemId\":\"ITEM\",\"amount\":1,\"market\":\"AUCTION\",\"durationHours\":\"x\"}")),
                "ITEM",
                List.of()
        );

        UnifiedFlipDto unsupportedDto = mapper.toDto(unsupportedDuration, FlipCalculationContext.standard(snapshot));
        UnifiedFlipDto invalidDto = mapper.toDto(invalidDuration, FlipCalculationContext.standard(snapshot));

        assertTrue(unsupportedDto.partialReasons().contains("UNSUPPORTED_AUCTION_DURATION_PRESET"));
        assertTrue(invalidDto.partialReasons().contains("INVALID_AUCTION_DURATION"));
    }

    @Test
    void toDtoParsesStepJsonOncePerStepWithinSingleMapping() throws Exception {
        ObjectMapper spyMapper = spy(new ObjectMapper());
        UnifiedFlipDtoMapper localMapper = new UnifiedFlipDtoMapper(spyMapper, new FlipRiskScorer());
        Flip flip = new Flip(
                UUID.randomUUID(),
                FlipType.CRAFTING,
                List.of(
                        Step.forBuyMarketBased(30L, "{\"itemId\":\"INPUT\",\"amount\":2,\"market\":\"BAZAAR\"}"),
                        Step.forSellMarketBased(15L, "{\"itemId\":\"OUTPUT\",\"amount\":1,\"market\":\"AUCTION\",\"durationHours\":12}")
                ),
                "OUTPUT",
                List.of()
        );

        UnifiedFlipInputSnapshot snapshot = new UnifiedFlipInputSnapshot(
                Instant.parse("2026-02-16T13:00:00Z"),
                Map.of("INPUT", new UnifiedFlipInputSnapshot.BazaarQuote(100D, 95D, 10_000L, 10_000L, 840_000L, 840_000L, 80, 80)),
                Map.of("OUTPUT", new UnifiedFlipInputSnapshot.AuctionQuote(10_000L, 12_000L, 11_000D, 12))
        );

        localMapper.toDto(flip, FlipCalculationContext.standard(snapshot));

        verify(spyMapper, times(3)).readTree(anyString());
    }

    private Object invokePrivate(Object target, String methodName, Class<?>[] parameterTypes, Object... args) throws Exception {
        Method method = target.getClass().getDeclaredMethod(methodName, parameterTypes);
        method.setAccessible(true);
        return method.invoke(target, args);
    }

    private Object invokeAccessor(Object target, String accessor) throws Exception {
        Method method = target.getClass().getDeclaredMethod(accessor);
        method.setAccessible(true);
        return method.invoke(target);
    }

}
