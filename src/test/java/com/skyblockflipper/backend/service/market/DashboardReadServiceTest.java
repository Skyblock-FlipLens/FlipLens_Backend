package com.skyblockflipper.backend.service.market;

import com.skyblockflipper.backend.NEU.model.Item;
import com.skyblockflipper.backend.NEU.repository.ItemRepository;
import com.skyblockflipper.backend.api.dto.DashboardOverviewDto;
import com.skyblockflipper.backend.api.dto.MarketplaceType;
import com.skyblockflipper.backend.api.dto.TrendingItemDto;
import com.skyblockflipper.backend.api.dto.UnifiedFlipDto;
import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.model.market.BazaarMarketRecord;
import com.skyblockflipper.backend.model.market.BzItemSnapshotEntity;
import com.skyblockflipper.backend.model.market.MarketSnapshot;
import com.skyblockflipper.backend.repository.BzItemSnapshotRepository;
import com.skyblockflipper.backend.repository.FlipRepository;
import com.skyblockflipper.backend.repository.MarketSnapshotHistoryRepository;
import com.skyblockflipper.backend.service.flipping.FlipCalculationContextService;
import com.skyblockflipper.backend.service.flipping.UnifiedFlipDtoMapper;
import com.skyblockflipper.backend.service.flipping.storage.UnifiedFlipCurrentReadService;
import com.skyblockflipper.backend.service.item.ItemMarketplaceService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DashboardReadServiceTest {

    private static final Instant FIXED_INSTANT = Instant.parse("2026-02-21T12:00:00Z");

    @Mock
    private MarketSnapshotPersistenceService snapshotService;
    @Mock
    private MarketSnapshotHistoryRepository marketSnapshotHistoryRepository;
    @Mock
    private BzItemSnapshotRepository bzItemSnapshotRepository;
    @Mock
    private ItemRepository itemRepository;
    @Mock
    private FlipRepository flipRepository;
    @Mock
    private UnifiedFlipDtoMapper mapper;
    @Mock
    private FlipCalculationContextService contextService;
    @Mock
    private UnifiedFlipCurrentReadService unifiedFlipCurrentReadService;
    @Mock
    private ItemMarketplaceService marketplaceService;

    private DashboardReadService service;

    @BeforeEach
    void setUp() {
        service = new DashboardReadService(
                snapshotService,
                marketSnapshotHistoryRepository,
                bzItemSnapshotRepository,
                itemRepository,
                flipRepository,
                mapper,
                contextService,
                unifiedFlipCurrentReadService,
                marketplaceService
        );
    }

    @Test
    void overviewReturnsUnknownWhenNoSnapshotExists() {
        when(itemRepository.count()).thenReturn(1247L);
        when(marketSnapshotHistoryRepository.findLatestCombinedSnapshotSummary()).thenReturn(null);

        DashboardOverviewDto dto = service.overview();

        assertEquals(1247L, dto.totalItems());
        assertEquals("UNKNOWN", dto.marketTrend());
        assertNull(dto.topFlip());
    }

    @Test
    void overviewReturnsTopFlipAndBullishTrendFromLatestSnapshot() {
        Instant ts = FIXED_INSTANT;
        when(itemRepository.count()).thenReturn(10L);
        when(marketSnapshotHistoryRepository.findLatestCombinedSnapshotSummary()).thenReturn(new MarketSnapshotHistoryRepository.MarketSnapshotSummaryProjection() {
            @Override
            public String getIdText() {
                return UUID.randomUUID().toString();
            }

            @Override
            public long getSnapshotTimestampEpochMillis() {
                return ts.toEpochMilli();
            }

            @Override
            public int getAuctionCount() {
                return 2;
            }

            @Override
            public int getBazaarProductCount() {
                return 2;
            }

            @Override
            public long getCreatedAtEpochMillis() {
                return ts.toEpochMilli();
            }
        });
        when(bzItemSnapshotRepository.findBySnapshotTsOrderByProductIdAsc(ts.toEpochMilli())).thenReturn(List.of(
                new BzItemSnapshotEntity(ts.toEpochMilli(), "A", 100D, 90D, 10L, 10L),
                new BzItemSnapshotEntity(ts.toEpochMilli(), "B", 200D, 180D, 10L, 10L)
        ));
        when(unifiedFlipCurrentReadService.currentSummary()).thenReturn(Optional.of(
                new UnifiedFlipCurrentReadService.CurrentSummary(2L, 5_000L, ts.toEpochMilli())
        ));
        UUID topFlipId = UUID.randomUUID();
        when(unifiedFlipCurrentReadService.listCurrentPage(
                org.mockito.ArgumentMatchers.isNull(),
                org.mockito.ArgumentMatchers.any(PageRequest.class)
        )).thenReturn(new PageImpl<>(List.of(
                flipDto(topFlipId, "B_OUTPUT", 5_000L)
        )));

        DashboardOverviewDto dto = service.overview();

        assertEquals(10L, dto.totalItems());
        assertEquals(2L, dto.totalActiveFlips());
        assertEquals(2L, dto.totalAHListings());
        assertEquals(2L, dto.bazaarProducts());
        assertEquals("BULLISH", dto.marketTrend());
        assertEquals(topFlipId.toString(), dto.topFlip().id());
        assertEquals("B_OUTPUT", dto.topFlip().outputName());
        assertEquals(5_000L, dto.topFlip().expectedProfit());
    }

    @Test
    void trendingComputesChangesSortsAndRespectsLimit() {
        Instant end = FIXED_INSTANT;
        MarketSnapshot first = new MarketSnapshot(
                end.minusSeconds(24L * 60L * 60L),
                List.of(),
                Map.of(
                        "A", new BazaarMarketRecord("A", 100, 99, 100, 100, 0, 0, 1, 1),
                        "B", new BazaarMarketRecord("B", 100, 99, 200, 200, 0, 0, 1, 1)
                )
        );
        MarketSnapshot latest = new MarketSnapshot(
                end,
                List.of(),
                Map.of(
                        "A", new BazaarMarketRecord("A", 150, 140, 300, 300, 0, 0, 1, 1),
                        "B", new BazaarMarketRecord("B", 110, 105, 210, 210, 0, 0, 1, 1)
                )
        );

        when(snapshotService.latest()).thenReturn(Optional.of(latest));
        when(snapshotService.between(end.minusSeconds(24L * 60L * 60L), end)).thenReturn(List.of(first, latest));
        when(itemRepository.findAll()).thenReturn(List.of(
                Item.builder().id("A").displayName("Alpha").build(),
                Item.builder().id("B").displayName("Bravo").build()
        ));
        when(marketplaceService.resolveMarketplaces(any())).thenReturn(Map.of(
                "A", MarketplaceType.BAZAAR,
                "B", MarketplaceType.BOTH
        ));

        List<TrendingItemDto> trending = service.trending(1);

        assertEquals(1, trending.size());
        assertEquals("A", trending.getFirst().itemId());
        assertEquals("Alpha", trending.getFirst().displayName());
        assertEquals(50.0, trending.getFirst().priceChange24h());
        assertTrue(trending.getFirst().volumeChange24h() > 0);
        assertEquals(MarketplaceType.BAZAAR, trending.getFirst().marketplace());
    }

    private UnifiedFlipDto flipDto(UUID id, String outputItem, long expectedProfit) {
        return new UnifiedFlipDto(
                id,
                FlipType.AUCTION,
                List.of(),
                List.of(new UnifiedFlipDto.ItemStackDto(outputItem, 1)),
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
