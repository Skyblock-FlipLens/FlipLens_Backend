package com.skyblockflipper.backend.service.market;

import com.skyblockflipper.backend.NEU.model.Item;
import com.skyblockflipper.backend.NEU.repository.ItemRepository;
import com.skyblockflipper.backend.api.dto.DashboardOverviewDto;
import com.skyblockflipper.backend.api.dto.MarketplaceType;
import com.skyblockflipper.backend.api.dto.TrendingItemDto;
import com.skyblockflipper.backend.model.market.BazaarMarketRecord;
import com.skyblockflipper.backend.model.market.BzItemSnapshotEntity;
import com.skyblockflipper.backend.model.market.MarketSnapshot;
import com.skyblockflipper.backend.repository.BzItemSnapshotRepository;
import com.skyblockflipper.backend.repository.FlipRepository;
import com.skyblockflipper.backend.repository.MarketSnapshotHistoryRepository;
import com.skyblockflipper.backend.service.flipping.FlipCalculationContext;
import com.skyblockflipper.backend.service.flipping.FlipCalculationContextService;
import com.skyblockflipper.backend.service.flipping.UnifiedFlipDtoMapper;
import com.skyblockflipper.backend.service.flipping.storage.UnifiedFlipCurrentReadService;
import com.skyblockflipper.backend.service.item.ItemMarketplaceService;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

@Service
public class DashboardReadService {

    private final MarketSnapshotPersistenceService marketSnapshotPersistenceService;
    private final MarketSnapshotHistoryRepository marketSnapshotHistoryRepository;
    private final BzItemSnapshotRepository bzItemSnapshotRepository;
    private final ItemRepository itemRepository;
    private final FlipRepository flipRepository;
    private final UnifiedFlipDtoMapper unifiedFlipDtoMapper;
    private final FlipCalculationContextService flipCalculationContextService;
    private final UnifiedFlipCurrentReadService unifiedFlipCurrentReadService;
    private final ItemMarketplaceService itemMarketplaceService;

    public DashboardReadService(MarketSnapshotPersistenceService marketSnapshotPersistenceService,
                                MarketSnapshotHistoryRepository marketSnapshotHistoryRepository,
                                BzItemSnapshotRepository bzItemSnapshotRepository,
                                ItemRepository itemRepository,
                                FlipRepository flipRepository,
                                UnifiedFlipDtoMapper unifiedFlipDtoMapper,
                                FlipCalculationContextService flipCalculationContextService,
                                UnifiedFlipCurrentReadService unifiedFlipCurrentReadService,
                                ItemMarketplaceService itemMarketplaceService) {
        this.marketSnapshotPersistenceService = marketSnapshotPersistenceService;
        this.marketSnapshotHistoryRepository = marketSnapshotHistoryRepository;
        this.bzItemSnapshotRepository = bzItemSnapshotRepository;
        this.itemRepository = itemRepository;
        this.flipRepository = flipRepository;
        this.unifiedFlipDtoMapper = unifiedFlipDtoMapper;
        this.flipCalculationContextService = flipCalculationContextService;
        this.unifiedFlipCurrentReadService = unifiedFlipCurrentReadService;
        this.itemMarketplaceService = itemMarketplaceService;
    }

    @Transactional(readOnly = true)
    public DashboardOverviewDto overview() {
        long totalItems = itemRepository.count();
        MarketSnapshotHistoryRepository.MarketSnapshotSummaryProjection latestSummary =
                marketSnapshotHistoryRepository.findLatestCombinedSnapshotSummary();
        if (latestSummary == null) {
            return new DashboardOverviewDto(totalItems, 0L, 0L, 0L, null, "UNKNOWN", null);
        }

        long latestSnapshotEpochMillis = latestSummary.getSnapshotTimestampEpochMillis();
        long ahListings = latestSummary.getAuctionCount();
        long bazaarProducts = latestSummary.getBazaarProductCount();

        List<BzItemSnapshotEntity> latestBazaarRows = bzItemSnapshotRepository.findBySnapshotTsOrderByProductIdAsc(latestSnapshotEpochMillis);
        DashboardOverviewDto.TopFlipDto topFlip = resolveTopFlip(latestSnapshotEpochMillis);
        long totalActiveFlips = resolveTotalActiveFlips(latestSnapshotEpochMillis);

        return new DashboardOverviewDto(
                totalItems,
                totalActiveFlips,
                ahListings,
                bazaarProducts,
                topFlip,
                marketTrend(latestBazaarRows),
                Instant.ofEpochMilli(latestSnapshotEpochMillis)
        );
    }

    @Transactional(readOnly = true)
    public List<TrendingItemDto> trending(int limit) {
        int safeLimit = Math.max(1, limit);
        Optional<MarketSnapshot> latestOpt = marketSnapshotPersistenceService.latest();
        if (latestOpt.isEmpty()) {
            return List.of();
        }
        Instant end = latestOpt.get().snapshotTimestamp();
        Instant start = end.minus(Duration.ofHours(24));
        List<MarketSnapshot> snapshots = marketSnapshotPersistenceService.between(start, end);
        if (snapshots.size() < 2) {
            return List.of();
        }
        MarketSnapshot first = snapshots.getFirst();
        MarketSnapshot latest = snapshots.getLast();

        Map<String, Item> itemById = itemRepository.findAll().stream()
                .collect(java.util.stream.Collectors.toMap(
                        item -> item.getId().toUpperCase(Locale.ROOT),
                        item -> item,
                        (left, right) -> left
                ));
        Map<String, MarketplaceType> marketplaceById = itemMarketplaceService.resolveMarketplaces(itemById.values());
        Set<String> candidates = latest.bazaarProducts().keySet();

        return candidates.stream()
                .map(itemId -> toTrending(itemId, first, latest, itemById, marketplaceById))
                .filter(Objects::nonNull)
                .sorted(Comparator.comparingDouble((TrendingItemDto dto) -> Math.abs(dto.priceChange24h())).reversed())
                .limit(safeLimit)
                .toList();
    }

    private TrendingItemDto toTrending(String itemId,
                                       MarketSnapshot first,
                                       MarketSnapshot latest,
                                       Map<String, Item> itemById,
                                       Map<String, MarketplaceType> marketplaceById) {
        BazaarMarketRecord firstRecord = first.bazaarProducts().get(itemId);
        BazaarMarketRecord latestRecord = latest.bazaarProducts().get(itemId);
        if (firstRecord == null || latestRecord == null || firstRecord.buyPrice() <= 0 || firstRecord.buyVolume() <= 0) {
            return null;
        }
        double priceChange = ((latestRecord.buyPrice() - firstRecord.buyPrice()) / firstRecord.buyPrice()) * 100D;
        double volumeChange = ((latestRecord.buyVolume() - firstRecord.buyVolume()) * 100D) / firstRecord.buyVolume();
        Item item = itemById.get(itemId.toUpperCase(Locale.ROOT));
        String displayName = item == null || item.getDisplayName() == null ? itemId : item.getDisplayName();
        MarketplaceType marketplace = item == null
                ? MarketplaceType.BAZAAR
                : marketplaceById.getOrDefault(item.getId(), MarketplaceType.BAZAAR);

        return new TrendingItemDto(
                itemId,
                displayName,
                round2(priceChange),
                round2(volumeChange),
                round(latestRecord.buyPrice()),
                marketplace
        );
    }

    private DashboardOverviewDto.TopFlipDto resolveTopFlip(long latestSnapshotEpochMillis) {
        if (unifiedFlipCurrentReadService != null) {
            return unifiedFlipCurrentReadService.listCurrentPage(
                            null,
                            PageRequest.of(0, 1, Sort.by(
                                    Sort.Order.desc("expectedProfit"),
                                    Sort.Order.asc("stableFlipId")
                            ))
                    ).stream()
                    .findFirst()
                    .filter(dto -> dto.expectedProfit() != null)
                    .map(dto -> new DashboardOverviewDto.TopFlipDto(
                            dto.id() == null ? null : dto.id().toString(),
                            dto.outputItems().isEmpty() ? null : dto.outputItems().getFirst().itemId(),
                            dto.expectedProfit()
                    ))
                    .orElse(null);
        }

        if (latestSnapshotEpochMillis <= 0L) {
            return null;
        }
        List<com.skyblockflipper.backend.model.Flipping.Flip> flips =
                flipRepository.findAllBySnapshotTimestampEpochMillis(latestSnapshotEpochMillis);
        if (flips.isEmpty()) {
            return null;
        }
        FlipCalculationContext context = flipCalculationContextService.loadContextAsOf(Instant.ofEpochMilli(latestSnapshotEpochMillis));
        return flips.stream()
                .map(flip -> unifiedFlipDtoMapper.toDto(flip, context))
                .filter(dto -> dto != null && dto.expectedProfit() != null)
                .max(Comparator.comparingLong(com.skyblockflipper.backend.api.dto.UnifiedFlipDto::expectedProfit))
                .map(dto -> new DashboardOverviewDto.TopFlipDto(
                        dto.id() == null ? null : dto.id().toString(),
                        dto.outputItems().isEmpty() ? null : dto.outputItems().getFirst().itemId(),
                        dto.expectedProfit()
                ))
                .orElse(null);
    }

    private long resolveTotalActiveFlips(long latestSnapshotEpochMillis) {
        if (unifiedFlipCurrentReadService != null) {
            Optional<UnifiedFlipCurrentReadService.CurrentSummary> summary = unifiedFlipCurrentReadService.currentSummary();
            if (summary.isPresent() && summary.get().latestSnapshotEpochMillis() == latestSnapshotEpochMillis) {
                return summary.get().totalCount();
            }
        }
        if (latestSnapshotEpochMillis <= 0L) {
            return 0L;
        }
        return flipRepository.findAllBySnapshotTimestampEpochMillis(latestSnapshotEpochMillis).size();
    }

    private String marketTrend(List<BzItemSnapshotEntity> bazaarRows) {
        if (bazaarRows == null || bazaarRows.isEmpty()) {
            return "UNKNOWN";
        }
        double avgSpreadPct = bazaarRows.stream()
                .filter(row -> row != null && row.getBuyPrice() != null && row.getBuyPrice() > 0D && row.getSellPrice() != null)
                .mapToDouble(row -> (row.getBuyPrice() - row.getSellPrice()) / row.getBuyPrice())
                .average()
                .orElse(0D);
        if (avgSpreadPct >= 0.04D) {
            return "BULLISH";
        }
        if (avgSpreadPct <= 0.01D) {
            return "BEARISH";
        }
        return "SIDEWAYS";
    }

    private long round(double value) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return 0L;
        }
        return Math.round(value);
    }

    private double round2(double value) {
        return Math.round(value * 100D) / 100D;
    }
}
