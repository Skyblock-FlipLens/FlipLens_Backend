package com.skyblockflipper.backend.service.item;

import com.skyblockflipper.backend.NEU.model.Item;
import com.skyblockflipper.backend.NEU.repository.ItemRepository;
import com.skyblockflipper.backend.api.dto.ItemQuickStatsDto;
import com.skyblockflipper.backend.api.dto.PriceHistoryRange;
import com.skyblockflipper.backend.api.dto.PricePointDto;
import com.skyblockflipper.backend.api.dto.ScorePointDto;
import com.skyblockflipper.backend.api.dto.UnifiedFlipDto;
import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.model.Flipping.Flip;
import com.skyblockflipper.backend.model.market.AuctionMarketRecord;
import com.skyblockflipper.backend.model.market.BazaarMarketRecord;
import com.skyblockflipper.backend.model.market.MarketSnapshot;
import com.skyblockflipper.backend.repository.FlipRepository;
import com.skyblockflipper.backend.service.flipping.FlipCalculationContext;
import com.skyblockflipper.backend.service.flipping.FlipCalculationContextService;
import com.skyblockflipper.backend.service.flipping.FlipScoreFeatureSet;
import com.skyblockflipper.backend.service.flipping.UnifiedFlipDtoMapper;
import com.skyblockflipper.backend.service.market.MarketSnapshotPersistenceService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

class ItemAnalyticsServiceTest {

    private MarketSnapshotPersistenceService snapshotService;
    private FlipRepository flipRepository;
    private UnifiedFlipDtoMapper mapper;
    private FlipCalculationContextService contextService;
    private ItemRepository itemRepository;
    private ItemAnalyticsService service;

    @BeforeEach
    void setUp() {
        snapshotService = mock(MarketSnapshotPersistenceService.class);
        flipRepository = mock(FlipRepository.class);
        mapper = mock(UnifiedFlipDtoMapper.class);
        contextService = mock(FlipCalculationContextService.class);
        itemRepository = mock(ItemRepository.class);
        service = new ItemAnalyticsService(snapshotService, flipRepository, mapper, contextService, itemRepository);
    }

    @Test
    void listPriceHistoryBucketsSnapshotsAndResolvesAuctionAlias() {
        Instant t1 = Instant.parse("2026-02-21T10:05:00Z");
        Instant t1Newer = Instant.parse("2026-02-21T10:45:00Z");
        Instant t2 = Instant.parse("2026-02-21T11:00:00Z");

        MarketSnapshot s1 = new MarketSnapshot(
                t1,
                List.of(new AuctionMarketRecord("a1", "Hyperion", "WEAPON", "LEGENDARY", 100L, 0L, 1L, 2L, false)),
                Map.of()
        );
        MarketSnapshot s2 = new MarketSnapshot(
                t1Newer,
                List.of(new AuctionMarketRecord("a2", "Hyperion ✪✪", "WEAPON", "LEGENDARY", 120L, 0L, 1L, 2L, false)),
                Map.of()
        );
        MarketSnapshot s3 = new MarketSnapshot(
                t2,
                List.of(),
                Map.of("HYPERION", new BazaarMarketRecord("HYPERION", 200D, 190D, 50L, 40L, 0L, 0L, 1, 1))
        );

        when(snapshotService.latest()).thenReturn(Optional.of(s3));
        when(snapshotService.between(t2.minus(PriceHistoryRange.H24.lookback()), t2)).thenReturn(List.of(s1, s2, s3));
        when(itemRepository.findById("HYPERION"))
                .thenReturn(Optional.of(Item.builder().id("HYPERION").displayName("Hyperion").minecraftId("hyperion").build()));

        List<PricePointDto> history = service.listPriceHistory("hyperion", PriceHistoryRange.H24);

        assertEquals(2, history.size());
        assertEquals(t1Newer, history.getFirst().timestamp());
        assertEquals(120L, history.getFirst().buyPrice());
        assertEquals(120L, history.getFirst().sellPrice());
        assertEquals(1L, history.getFirst().volume());
        assertEquals(200L, history.get(1).buyPrice());
        assertEquals(190L, history.get(1).sellPrice());
        assertEquals(50L, history.get(1).volume());
    }

    @Test
    void listPriceHistoryReturnsEmptyForInvalidInputOrMissingSnapshot() {
        when(snapshotService.latest()).thenReturn(Optional.empty());

        assertTrue(service.listPriceHistory(" ", PriceHistoryRange.D30).isEmpty());
        assertTrue(service.listPriceHistory("HYPERION", PriceHistoryRange.D30).isEmpty());
    }

    @Test
    void listScoreHistoryComputesLiquidityAndRiskFromPriceHistory() {
        ItemAnalyticsService serviceSpy = spy(service);

        doReturn(List.of(
                new PricePointDto(Instant.parse("2026-02-20T00:00:00Z"), 100L, 90L, 50L),
                new PricePointDto(Instant.parse("2026-02-21T00:00:00Z"), 110L, 100L, 100L)
        )).when(serviceSpy).listPriceHistory("HYPERION", PriceHistoryRange.D30);

        List<ScorePointDto> result = serviceSpy.listScoreHistory("HYPERION");

        assertEquals(2, result.size());
        assertEquals(50.0, result.getFirst().liquidityScore());
        assertEquals(100.0, result.get(1).liquidityScore());
        assertTrue(result.getFirst().riskScore() > 0D);
        assertTrue(result.get(1).riskScore() > 0D);
    }

    @Test
    void quickStatsAggregatesHistoryValues() {
        ItemAnalyticsService serviceSpy = spy(service);

        doReturn(List.of(
                new PricePointDto(Instant.parse("2026-02-20T00:00:00Z"), 150L, 145L, 80L),
                new PricePointDto(Instant.parse("2026-02-21T00:00:00Z"), 200L, 190L, 100L)
        )).when(serviceSpy).listPriceHistory("HYPERION", PriceHistoryRange.D30);
        doReturn(List.of(
                new PricePointDto(Instant.parse("2026-02-20T12:00:00Z"), 100L, 100L, 20L),
                new PricePointDto(Instant.parse("2026-02-21T12:00:00Z"), 110L, 90L, 30L)
        )).when(serviceSpy).listPriceHistory("HYPERION", PriceHistoryRange.H24);
        doReturn(List.of(
                new PricePointDto(Instant.parse("2026-02-15T00:00:00Z"), 180L, 170L, 50L),
                new PricePointDto(Instant.parse("2026-02-18T00:00:00Z"), 220L, 160L, 70L),
                new PricePointDto(Instant.parse("2026-02-21T00:00:00Z"), 200L, 190L, 100L)
        )).when(serviceSpy).listPriceHistory("HYPERION", PriceHistoryRange.D7);

        Optional<ItemQuickStatsDto> stats = serviceSpy.quickStats("HYPERION");

        assertTrue(stats.isPresent());
        ItemQuickStatsDto dto = stats.get();
        assertEquals(200L, dto.buyPrice());
        assertEquals(190L, dto.sellPrice());
        assertEquals(10.0, dto.buyChange24h());
        assertEquals(-10.0, dto.sellChange24h());
        assertEquals(10L, dto.spread());
        assertEquals(5.0, dto.spreadPct());
        assertEquals(73L, dto.avgVolume7d());
        assertEquals(220L, dto.high7d());
        assertEquals(160L, dto.low7d());
    }

    @Test
    void listFlipsForItemFiltersByInputAndOutputAliasesAndPaginates() {
        long snapshotEpoch = Instant.parse("2026-02-21T12:00:00Z").toEpochMilli();
        when(flipRepository.findMaxSnapshotTimestampEpochMillis()).thenReturn(Optional.of(snapshotEpoch));
        when(itemRepository.findById("HYPERION"))
                .thenReturn(Optional.of(Item.builder().id("HYPERION").displayName("Hyperion").minecraftId("hyperion").build()));

        Flip flipOne = new Flip(UUID.randomUUID(), FlipType.AUCTION, List.of(), "A", List.of());
        Flip flipTwo = new Flip(UUID.randomUUID(), FlipType.BAZAAR, List.of(), "B", List.of());
        Flip flipThree = new Flip(UUID.randomUUID(), FlipType.CRAFTING, List.of(), "C", List.of());
        when(flipRepository.findAllBySnapshotTimestampEpochMillis(snapshotEpoch)).thenReturn(List.of(flipOne, flipTwo, flipThree));

        FlipCalculationContext context = new FlipCalculationContext(null, 0.0125D, 1D, false, FlipScoreFeatureSet.empty());
        when(contextService.loadContextAsOf(Instant.ofEpochMilli(snapshotEpoch))).thenReturn(context);
        when(mapper.toDto(flipOne, context)).thenReturn(flipDto(flipOne.getId(),
                List.of(new UnifiedFlipDto.ItemStackDto("HYPERION", 1)),
                List.of()));
        when(mapper.toDto(flipTwo, context)).thenReturn(flipDto(flipTwo.getId(),
                List.of(),
                List.of(new UnifiedFlipDto.ItemStackDto("SUPER_HYPERION_CORE", 1))));
        when(mapper.toDto(flipThree, context)).thenReturn(flipDto(flipThree.getId(),
                List.of(new UnifiedFlipDto.ItemStackDto("DIAMOND", 1)),
                List.of(new UnifiedFlipDto.ItemStackDto("EMERALD", 1))));

        Page<UnifiedFlipDto> page = service.listFlipsForItem("hyperion", PageRequest.of(0, 1));

        assertEquals(2, page.getTotalElements());
        assertEquals(1, page.getContent().size());
        assertEquals(flipOne.getId(), page.getContent().getFirst().id());
    }

    @Test
    void listFlipsForItemReturnsEmptyWhenItemIdOrSnapshotMissing() {
        when(flipRepository.findMaxSnapshotTimestampEpochMillis()).thenReturn(Optional.empty());

        assertTrue(service.listFlipsForItem(" ", PageRequest.of(0, 10)).isEmpty());
        assertTrue(service.listFlipsForItem("HYPERION", PageRequest.of(0, 10)).isEmpty());
    }

    private UnifiedFlipDto flipDto(UUID id,
                                   List<UnifiedFlipDto.ItemStackDto> input,
                                   List<UnifiedFlipDto.ItemStackDto> output) {
        return new UnifiedFlipDto(
                id,
                FlipType.AUCTION,
                input,
                output,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                Instant.now(),
                false,
                List.of(),
                List.of(),
                List.of()
        );
    }
}
