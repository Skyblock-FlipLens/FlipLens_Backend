package com.skyblockflipper.backend.service.market;

import com.skyblockflipper.backend.api.MarketOverviewDto;
import com.skyblockflipper.backend.api.UnifiedFlipDto;
import com.skyblockflipper.backend.model.Flipping.Flip;
import com.skyblockflipper.backend.model.market.BazaarMarketRecord;
import com.skyblockflipper.backend.model.market.MarketSnapshot;
import com.skyblockflipper.backend.repository.FlipRepository;
import com.skyblockflipper.backend.service.flipping.FlipCalculationContext;
import com.skyblockflipper.backend.service.flipping.FlipCalculationContextService;
import com.skyblockflipper.backend.service.flipping.UnifiedFlipDtoMapper;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@Service
public class MarketOverviewService {

    private static final long SEVEN_DAYS_SECONDS = 7L * 24L * 60L * 60L;

    private final MarketSnapshotPersistenceService marketSnapshotPersistenceService;
    private final FlipRepository flipRepository;
    private final UnifiedFlipDtoMapper unifiedFlipDtoMapper;
    private final FlipCalculationContextService flipCalculationContextService;

    public MarketOverviewService(MarketSnapshotPersistenceService marketSnapshotPersistenceService,
                                 FlipRepository flipRepository,
                                 UnifiedFlipDtoMapper unifiedFlipDtoMapper,
                                 FlipCalculationContextService flipCalculationContextService) {
        this.marketSnapshotPersistenceService = marketSnapshotPersistenceService;
        this.flipRepository = flipRepository;
        this.unifiedFlipDtoMapper = unifiedFlipDtoMapper;
        this.flipCalculationContextService = flipCalculationContextService;
    }

    public MarketOverviewDto overview(String productId) {
        Optional<MarketSnapshot> latestSnapshotOptional = marketSnapshotPersistenceService.latest();
        if (latestSnapshotOptional.isEmpty()) {
            return new MarketOverviewDto(productId, null, null, null, null, null, null, null, null, null, 0L, null, 0L, null);
        }

        MarketSnapshot latestSnapshot = latestSnapshotOptional.get();
        String normalizedProductId = normalizeProductId(productId);
        BazaarMarketRecord currentRecord = resolveCurrentRecord(latestSnapshot.bazaarProducts(), normalizedProductId);

        Instant now = Instant.now();
        Instant rangeEnd = latestSnapshot.snapshotTimestamp() != null ? latestSnapshot.snapshotTimestamp() : now;
        Instant rangeStart = rangeEnd.minusSeconds(SEVEN_DAYS_SECONDS);

        List<MarketSnapshot> weeklySnapshots = marketSnapshotPersistenceService.between(rangeStart, rangeEnd);

        List<BazaarMarketRecord> relevantRecords = weeklySnapshots.stream()
                .map(MarketSnapshot::bazaarProducts)
                .map(map -> resolveCurrentRecord(map, normalizedProductId))
                .filter(record -> record != null)
                .toList();

        long activeFlips = 0L;
        Long bestProfit = null;
        if (latestSnapshot.snapshotTimestamp() != null) {
            long snapshotEpochMillis = latestSnapshot.snapshotTimestamp().toEpochMilli();
            List<Flip> flips = flipRepository.findAllBySnapshotTimestampEpochMillis(snapshotEpochMillis);
            activeFlips = flips.size();

            FlipCalculationContext context = flipCalculationContextService.loadContextAsOf(latestSnapshot.snapshotTimestamp());
            bestProfit = flips.stream()
                    .map(flip -> unifiedFlipDtoMapper.toDto(flip, context))
                    .map(UnifiedFlipDto::expectedProfit)
                    .filter(value -> value != null)
                    .max(Long::compareTo)
                    .orElse(null);
        }

        Double buy = currentRecord == null ? null : currentRecord.buyPrice();
        Double sell = currentRecord == null ? null : currentRecord.sellPrice();
        Double spread = buy == null || sell == null ? null : buy - sell;
        Double spreadPercent = spread == null || buy == null || buy <= 0D ? null : (spread / buy) * 100D;

        Double avgBuy = average(relevantRecords.stream().map(BazaarMarketRecord::buyPrice).toList());
        Double avgSell = average(relevantRecords.stream().map(BazaarMarketRecord::sellPrice).toList());
        Double buyChange = percentageDelta(buy, avgBuy);
        Double sellChange = percentageDelta(sell, avgSell);

        Double high = relevantRecords.stream()
                .map(BazaarMarketRecord::buyPrice)
                .filter(Objects::nonNull)
                .max(Comparator.naturalOrder())
                .orElse(null);
        Double low = relevantRecords.stream()
                .map(BazaarMarketRecord::buyPrice)
                .filter(Objects::nonNull)
                .min(Comparator.naturalOrder())
                .orElse(null);

        Long volume = currentRecord == null ? 0L : currentRecord.buyVolume();
        Double averageVolume = averageLong(relevantRecords.stream().map(BazaarMarketRecord::buyVolume).toList());

        return new MarketOverviewDto(
                normalizedProductId,
                latestSnapshot.snapshotTimestamp(),
                buy,
                buyChange,
                sell,
                sellChange,
                spread,
                spreadPercent,
                high,
                low,
                volume,
                averageVolume,
                activeFlips,
                bestProfit
        );
    }

    private String normalizeProductId(String productId) {
        if (productId == null || productId.isBlank()) {
            return null;
        }
        return productId.trim().toUpperCase(Locale.ROOT);
    }

    private BazaarMarketRecord resolveCurrentRecord(Map<String, BazaarMarketRecord> bazaarProducts, String productId) {
        if (bazaarProducts == null || bazaarProducts.isEmpty()) {
            return null;
        }
        if (productId != null) {
            return bazaarProducts.get(productId);
        }
        return bazaarProducts.values().stream().findFirst().orElse(null);
    }

    private Double average(List<Double> values) {
        if (values == null || values.isEmpty()) {
            return null;
        }

        List<Double> nonNullValues = values.stream()
                .filter(Objects::nonNull)
                .toList();
        if (nonNullValues.isEmpty()) {
            return null;
        }

        return nonNullValues.stream().mapToDouble(Double::doubleValue).average().orElse(0D);
    }

    private Double averageLong(List<Long> values) {
        if (values == null || values.isEmpty()) {
            return null;
        }

        List<Long> nonNullValues = values.stream()
                .filter(Objects::nonNull)
                .toList();
        if (nonNullValues.isEmpty()) {
            return null;
        }

        return nonNullValues.stream().mapToLong(Long::longValue).average().orElse(0D);
    }

    private Double percentageDelta(Double value, Double base) {
        if (value == null || base == null || base == 0D) {
            return null;
        }
        return ((value - base) / base) * 100D;
    }
}

