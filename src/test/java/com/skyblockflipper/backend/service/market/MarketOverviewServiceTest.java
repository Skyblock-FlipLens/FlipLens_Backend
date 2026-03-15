package com.skyblockflipper.backend.service.market;

import com.skyblockflipper.backend.api.dto.MarketOverviewDto;
import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.model.Flipping.Flip;
import com.skyblockflipper.backend.model.market.BazaarMarketRecord;
import com.skyblockflipper.backend.model.market.BzItemSnapshotEntity;
import com.skyblockflipper.backend.model.market.MarketSnapshot;
import com.skyblockflipper.backend.repository.BzItemSnapshotRepository;
import com.skyblockflipper.backend.repository.FlipRepository;
import com.skyblockflipper.backend.service.flipping.FlipCalculationContext;
import com.skyblockflipper.backend.service.flipping.FlipCalculationContextService;
import com.skyblockflipper.backend.service.flipping.FlipScoreFeatureSet;
import com.skyblockflipper.backend.service.flipping.UnifiedFlipDtoMapper;
import com.skyblockflipper.backend.service.flipping.storage.UnifiedFlipCurrentReadService;
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
        BzItemSnapshotRepository bzItemSnapshotRepository = mock(BzItemSnapshotRepository.class);
        FlipRepository flipRepository = mock(FlipRepository.class);
        UnifiedFlipDtoMapper mapper = mock(UnifiedFlipDtoMapper.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        UnifiedFlipCurrentReadService unifiedFlipCurrentReadService = mock(UnifiedFlipCurrentReadService.class);
        MarketOverviewService service = new MarketOverviewService(
                snapshotService,
                bzItemSnapshotRepository,
                flipRepository,
                mapper,
                contextService,
                unifiedFlipCurrentReadService
        );

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
        BzItemSnapshotRepository bzItemSnapshotRepository = mock(BzItemSnapshotRepository.class);
        FlipRepository flipRepository = mock(FlipRepository.class);
        UnifiedFlipDtoMapper mapper = mock(UnifiedFlipDtoMapper.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        UnifiedFlipCurrentReadService unifiedFlipCurrentReadService = mock(UnifiedFlipCurrentReadService.class);
        MarketOverviewService service = new MarketOverviewService(
                snapshotService,
                bzItemSnapshotRepository,
                flipRepository,
                mapper,
                contextService,
                unifiedFlipCurrentReadService
        );

        Instant ts = Instant.parse("2026-02-21T12:00:00Z");
        BazaarMarketRecord now = new BazaarMarketRecord("ENCHANTED_DIAMOND_BLOCK", 110D, 100D, 1_000L, 900L, 0, 0, 1, 1);
        MarketSnapshot latest = new MarketSnapshot(ts, List.of(), Map.of("ENCHANTED_DIAMOND_BLOCK", now));

        when(snapshotService.latest()).thenReturn(Optional.of(latest));
        when(bzItemSnapshotRepository.findByProductIdAndSnapshotTsBetweenOrderBySnapshotTsAsc(
                "ENCHANTED_DIAMOND_BLOCK",
                ts.minusSeconds(7L * 24L * 60L * 60L).toEpochMilli(),
                ts.toEpochMilli()
        )).thenReturn(List.of(
                new BzItemSnapshotEntity(ts.minusSeconds(3600L * 24L).toEpochMilli(), "ENCHANTED_DIAMOND_BLOCK", 100D, 95D, 800L, 700L),
                new BzItemSnapshotEntity(ts.minusSeconds(3600L).toEpochMilli(), "ENCHANTED_DIAMOND_BLOCK", 105D, 99D, 900L, 850L),
                new BzItemSnapshotEntity(ts.toEpochMilli(), "ENCHANTED_DIAMOND_BLOCK", 110D, 100D, 1_000L, 900L)
        ));
        when(unifiedFlipCurrentReadService.currentSummary()).thenReturn(Optional.of(
                new UnifiedFlipCurrentReadService.CurrentSummary(2L, 2_000L, ts.toEpochMilli())
        ));

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
}
