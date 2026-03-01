package com.skyblockflipper.backend.service.market;

import com.skyblockflipper.backend.api.dto.MarketOverviewDto;
import com.skyblockflipper.backend.api.dto.UnifiedFlipDto;
import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.model.Flipping.Flip;
import com.skyblockflipper.backend.model.market.BazaarMarketRecord;
import com.skyblockflipper.backend.model.market.MarketSnapshot;
import com.skyblockflipper.backend.repository.FlipRepository;
import com.skyblockflipper.backend.service.flipping.FlipCalculationContext;
import com.skyblockflipper.backend.service.flipping.FlipCalculationContextService;
import com.skyblockflipper.backend.service.flipping.FlipScoreFeatureSet;
import com.skyblockflipper.backend.service.flipping.UnifiedFlipDtoMapper;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MarketOverviewServiceTest {

    private static final Instant FIXED_INSTANT = Instant.parse("2026-02-21T12:00:00Z");

    @Test
    void overviewReturnsFallbackWhenNoSnapshotExists() {
        MarketSnapshotPersistenceService snapshotService = mock(MarketSnapshotPersistenceService.class);
        FlipRepository flipRepository = mock(FlipRepository.class);
        UnifiedFlipDtoMapper mapper = mock(UnifiedFlipDtoMapper.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        MarketOverviewService service = new MarketOverviewService(snapshotService, flipRepository, mapper, contextService);

        when(snapshotService.latest()).thenReturn(Optional.empty());

        MarketOverviewDto dto = service.overview("HYPERION");

        assertEquals("HYPERION", dto.productId());
        assertNull(dto.buy());
        assertEquals(0L, dto.activeFlips());
        assertNull(dto.bestProfit());
    }

    @Test
    void overviewComputesSnapshotMetricsAndBestProfit() {
        MarketSnapshotPersistenceService snapshotService = mock(MarketSnapshotPersistenceService.class);
        FlipRepository flipRepository = mock(FlipRepository.class);
        UnifiedFlipDtoMapper mapper = mock(UnifiedFlipDtoMapper.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        MarketOverviewService service = new MarketOverviewService(snapshotService, flipRepository, mapper, contextService);

        Instant ts = Instant.parse("2026-02-21T12:00:00Z");
        BazaarMarketRecord now = new BazaarMarketRecord("ENCHANTED_DIAMOND_BLOCK", 110D, 100D, 1_000L, 900L, 0, 0, 1, 1);
        MarketSnapshot latest = new MarketSnapshot(ts, List.of(), Map.of("ENCHANTED_DIAMOND_BLOCK", now));
        MarketSnapshot s1 = new MarketSnapshot(ts.minusSeconds(3600 * 24), List.of(), Map.of(
                "ENCHANTED_DIAMOND_BLOCK", new BazaarMarketRecord("ENCHANTED_DIAMOND_BLOCK", 100D, 95D, 800L, 700L, 0, 0, 1, 1)));
        MarketSnapshot s2 = new MarketSnapshot(ts.minusSeconds(3600), List.of(), Map.of(
                "ENCHANTED_DIAMOND_BLOCK", new BazaarMarketRecord("ENCHANTED_DIAMOND_BLOCK", 105D, 99D, 900L, 850L, 0, 0, 1, 1)));

        when(snapshotService.latest()).thenReturn(Optional.of(latest));
        when(snapshotService.between(ts.minusSeconds(7L * 24L * 60L * 60L), ts)).thenReturn(List.of(s1, s2, latest));

        Flip flipOne = new Flip(UUID.randomUUID(), FlipType.AUCTION, List.of(), "A", List.of());
        Flip flipTwo = new Flip(UUID.randomUUID(), FlipType.BAZAAR, List.of(), "B", List.of());
        when(flipRepository.findAllBySnapshotTimestampEpochMillis(ts.toEpochMilli())).thenReturn(List.of(flipOne, flipTwo));

        FlipCalculationContext context = new FlipCalculationContext(null, 0.0125D, 1D, false, FlipScoreFeatureSet.empty());
        when(contextService.loadContextAsOf(ts)).thenReturn(context);
        when(mapper.toDto(flipOne, context)).thenReturn(flipDto(1_500L));
        when(mapper.toDto(flipTwo, context)).thenReturn(flipDto(2_000L));

        MarketOverviewDto dto = service.overview("enchanted_diamond_block");

        assertEquals("ENCHANTED_DIAMOND_BLOCK", dto.productId());
        assertEquals(110D, dto.buy());
        assertEquals(100D, dto.sell());
        assertEquals(10D, dto.spread());
        assertEquals(9.0909090909D, dto.spreadPercent(), 0.0001D);
        assertEquals(110D, dto.sevenDayHigh());
        assertEquals(95D, dto.sevenDayLow());
        assertEquals(1_000L, dto.volume());
        assertEquals(900D, dto.averageVolume(), 0.0001D);
        assertEquals(2L, dto.activeFlips());
        assertEquals(2_000L, dto.bestProfit());
    }

    private UnifiedFlipDto flipDto(Long expectedProfit) {
        return new UnifiedFlipDto(
                UUID.randomUUID(),
                FlipType.AUCTION,
                List.of(),
                List.of(),
                null,
                expectedProfit,
                null,
                null,
                null,
                null,
                null,
                null,
                FIXED_INSTANT,
                false,
                List.of(),
                List.of(),
                List.of()
        );
    }
}
