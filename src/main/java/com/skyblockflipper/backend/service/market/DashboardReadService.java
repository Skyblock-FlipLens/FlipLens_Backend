package com.skyblockflipper.backend.service.market;

import com.skyblockflipper.backend.NEU.model.Item;
import com.skyblockflipper.backend.NEU.repository.ItemRepository;
import com.skyblockflipper.backend.api.DashboardOverviewDto;
import com.skyblockflipper.backend.api.MarketplaceType;
import com.skyblockflipper.backend.api.TrendingItemDto;
import com.skyblockflipper.backend.api.UnifiedFlipDto;
import com.skyblockflipper.backend.model.Flipping.Flip;
import com.skyblockflipper.backend.model.market.BazaarMarketRecord;
import com.skyblockflipper.backend.model.market.MarketSnapshot;
import com.skyblockflipper.backend.repository.FlipRepository;
import com.skyblockflipper.backend.service.flipping.FlipCalculationContext;
import com.skyblockflipper.backend.service.flipping.FlipCalculationContextService;
import com.skyblockflipper.backend.service.flipping.UnifiedFlipDtoMapper;
import com.skyblockflipper.backend.service.item.ItemMarketplaceService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

@Service
public class DashboardReadService {

    private final MarketSnapshotPersistenceService marketSnapshotPersistenceService;
    private final ItemRepository itemRepository;
    private final FlipRepository flipRepository;
    private final UnifiedFlipDtoMapper unifiedFlipDtoMapper;
    private final FlipCalculationContextService flipCalculationContextService;
    private final ItemMarketplaceService itemMarketplaceService;

    public DashboardReadService(MarketSnapshotPersistenceService marketSnapshotPersistenceService,
                                ItemRepository itemRepository,
                                FlipRepository flipRepository,
                                UnifiedFlipDtoMapper unifiedFlipDtoMapper,
                                FlipCalculationContextService flipCalculationContextService,
                                ItemMarketplaceService itemMarketplaceService) {
        this.marketSnapshotPersistenceService = marketSnapshotPersistenceService;
        this.itemRepository = itemRepository;
        this.flipRepository = flipRepository;
        this.unifiedFlipDtoMapper = unifiedFlipDtoMapper;
        this.flipCalculationContextService = flipCalculationContextService;
        this.itemMarketplaceService = itemMarketplaceService;
    }

    @Transactional(readOnly = true)
    public DashboardOverviewDto overview() {
        long totalItems = itemRepository.count();
        Optional<MarketSnapshot> latestOpt = marketSnapshotPersistenceService.latest();
        if (latestOpt.isEmpty()) {
            return new DashboardOverviewDto(totalItems, 0L, 0L, 0L, null, "UNKNOWN", null);
        }
        MarketSnapshot latest = latestOpt.get();

        long ahListings = latest.auctions().size();
        long bazaarProducts = latest.bazaarProducts().size();

        Long latestFlipEpoch = flipRepository.findMaxSnapshotTimestampEpochMillis().orElse(null);
        List<Flip> flips = latestFlipEpoch == null
                ? List.of()
                : flipRepository.findAllBySnapshotTimestampEpochMillis(latestFlipEpoch);

        DashboardOverviewDto.TopFlipDto topFlip = null;
        if (latestFlipEpoch != null && !flips.isEmpty()) {
            FlipCalculationContext context = flipCalculationContextService.loadContextAsOf(Instant.ofEpochMilli(latestFlipEpoch));
            topFlip = flips.stream()
                    .map(flip -> unifiedFlipDtoMapper.toDto(flip, context))
                    .filter(dto -> dto != null && dto.expectedProfit() != null)
                    .max(Comparator.comparingLong(UnifiedFlipDto::expectedProfit))
                    .map(dto -> new DashboardOverviewDto.TopFlipDto(
                            dto.id() == null ? null : dto.id().toString(),
                            dto.outputItems().isEmpty() ? null : dto.outputItems().getFirst().itemId(),
                            dto.expectedProfit()
                    ))
                    .orElse(null);
        }

        return new DashboardOverviewDto(
                totalItems,
                flips.size(),
                ahListings,
                bazaarProducts,
                topFlip,
                marketTrend(latest.bazaarProducts()),
                latest.snapshotTimestamp()
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

    private String marketTrend(Map<String, BazaarMarketRecord> bazaarProducts) {
        if (bazaarProducts == null || bazaarProducts.isEmpty()) {
            return "UNKNOWN";
        }
        double avgSpreadPct = bazaarProducts.values().stream()
                .filter(record -> record.buyPrice() > 0)
                .mapToDouble(record -> (record.buyPrice() - record.sellPrice()) / record.buyPrice())
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
