package com.skyblockflipper.backend.service.market;

import com.skyblockflipper.backend.model.market.BazaarMarketRecord;
import com.skyblockflipper.backend.model.market.BzItemSnapshotEntity;
import com.skyblockflipper.backend.model.market.MarketSnapshot;
import com.skyblockflipper.backend.repository.BzItemSnapshotRepository;
import com.skyblockflipper.backend.service.flipping.FlipScoreFeatureSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
public class MarketTimescaleFeatureService {

    private static final long SECONDS_PER_UTC_DAY = 86_400L;
    private static final long MILLIS_PER_DAY = SECONDS_PER_UTC_DAY * 1_000L;
    private static final int MICRO_WINDOW_SECONDS = 60;
    private static final int MACRO_WINDOW_DAYS = 30;
    private static final int MICRO_HIGH_CONFIDENCE_POINTS = 10;
    private static final int MICRO_MEDIUM_CONFIDENCE_POINTS = 6;
    private static final int MACRO_HIGH_CONFIDENCE_RETURNS = 7;
    private static final int MACRO_MEDIUM_CONFIDENCE_RETURNS = 3;
    private static final double MICRO_LOG_RETURN_CAP = 0.20D;
    private static final double STRUCTURAL_SPREAD_THRESHOLD = 0.05D;
    private static final double STRUCTURAL_TURNOVER_PER_HOUR_THRESHOLD = 10D;

    private final BzItemSnapshotRepository bzItemSnapshotRepository;
    private final MarketItemKeyService marketItemKeyService;

    @Autowired
    public MarketTimescaleFeatureService(BzItemSnapshotRepository bzItemSnapshotRepository,
                                         MarketItemKeyService marketItemKeyService) {
        this.bzItemSnapshotRepository = bzItemSnapshotRepository;
        this.marketItemKeyService = marketItemKeyService == null ? new MarketItemKeyService() : marketItemKeyService;
    }

    public FlipScoreFeatureSet computeFor(MarketSnapshot latestSnapshot) {
        if (latestSnapshot == null || latestSnapshot.bazaarProducts().isEmpty()) {
            return FlipScoreFeatureSet.empty();
        }

        Instant evaluationTs = latestSnapshot.snapshotTimestamp();
        if (evaluationTs == null) {
            return FlipScoreFeatureSet.empty();
        }

        Set<String> normalizedIds = latestSnapshot.bazaarProducts().values().stream()
                .map(record -> marketItemKeyService.toBazaarItemKey(record))
                .filter(id -> id != null && !id.isBlank())
                .collect(java.util.stream.Collectors.toSet());
        if (normalizedIds.isEmpty()) {
            return FlipScoreFeatureSet.empty();
        }

        long evaluationEpochMillis = evaluationTs.toEpochMilli();
        long microStartInclusive = evaluationEpochMillis - (MICRO_WINDOW_SECONDS * 1_000L);
        List<BzItemSnapshotEntity> microRows = bzItemSnapshotRepository
                .findBySnapshotTsBetweenAndProductIdInOrderBySnapshotTsAsc(
                        microStartInclusive,
                        evaluationEpochMillis,
                        normalizedIds
                );

        long earliestEpochDay = epochDay(evaluationTs) - (MACRO_WINDOW_DAYS + 2L);
        long macroStartInclusive = earliestEpochDay * MILLIS_PER_DAY;
        List<Long> dailyAnchorSnapshotTs = bzItemSnapshotRepository.findFirstSnapshotTsPerDayBetween(
                macroStartInclusive,
                evaluationEpochMillis
        );
        List<BzItemSnapshotEntity> dailyRows = dailyAnchorSnapshotTs.isEmpty()
                ? List.of()
                : bzItemSnapshotRepository.findBySnapshotTsInAndProductIdInOrderBySnapshotTsAsc(
                        dailyAnchorSnapshotTs,
                        normalizedIds
                );

        Map<String, List<PricePoint>> microSeriesByItem = buildMicroSeriesByItem(microRows);
        Map<String, List<DailyPoint>> dailySeriesByItem = buildDailySeriesByItem(dailyRows);

        Map<String, FlipScoreFeatureSet.ItemTimescaleFeatures> byItem = new LinkedHashMap<>();
        for (Map.Entry<String, BazaarMarketRecord> entry : latestSnapshot.bazaarProducts().entrySet()) {
            BazaarMarketRecord latestRecord = entry.getValue();
            String normalizedId = marketItemKeyService.toBazaarItemKey(latestRecord);
            if (normalizedId == null || normalizedId.isBlank()) {
                continue;
            }
            List<PricePoint> microSeries = microSeriesByItem.getOrDefault(normalizedId, List.of());
            List<DailyPoint> dailyPoints = dailySeriesByItem.getOrDefault(normalizedId, List.of());
            DailyFeatureSeries dailySeries = buildDailySeries(dailyPoints);
            byItem.put(normalizedId, computeItemFeatures(evaluationTs, latestRecord, microSeries, dailySeries));
        }
        return new FlipScoreFeatureSet(byItem);
    }

    private Map<String, List<PricePoint>> buildMicroSeriesByItem(List<BzItemSnapshotEntity> rows) {
        if (rows == null || rows.isEmpty()) {
            return Map.of();
        }
        Map<String, List<PricePoint>> byItem = new LinkedHashMap<>();
        for (BzItemSnapshotEntity row : rows) {
            if (row == null) {
                continue;
            }
            Double mid = resolveMid(row.getBuyPrice(), row.getSellPrice());
            if (mid == null) {
                continue;
            }
            byItem.computeIfAbsent(row.getProductId(), ignored -> new ArrayList<>())
                    .add(new PricePoint(Instant.ofEpochMilli(row.getSnapshotTs()), mid));
        }
        for (List<PricePoint> points : byItem.values()) {
            points.sort(Comparator.comparing(PricePoint::timestamp));
        }
        return byItem;
    }

    private Map<String, List<DailyPoint>> buildDailySeriesByItem(List<BzItemSnapshotEntity> rows) {
        if (rows == null || rows.isEmpty()) {
            return Map.of();
        }
        Map<String, List<DailyPoint>> byItem = new LinkedHashMap<>();
        for (BzItemSnapshotEntity row : rows) {
            if (row == null) {
                continue;
            }
            Double mid = resolveMid(row.getBuyPrice(), row.getSellPrice());
            if (mid == null) {
                continue;
            }
            long day = Math.floorDiv(row.getSnapshotTs(), MILLIS_PER_DAY);
            byItem.computeIfAbsent(row.getProductId(), ignored -> new ArrayList<>())
                    .add(new DailyPoint(
                            day,
                            mid,
                            computeRelativeSpread(row.getBuyPrice(), row.getSellPrice()),
                            resolveConservativeTurnoverPerHour(row.getBuyVolume(), row.getSellVolume())
                    ));
        }
        for (List<DailyPoint> points : byItem.values()) {
            points.sort(Comparator.comparing(DailyPoint::day));
        }
        return byItem;
    }

    private FlipScoreFeatureSet.ItemTimescaleFeatures computeItemFeatures(Instant evaluationTs,
                                                                          BazaarMarketRecord latestRecord,
                                                                          List<PricePoint> microSeries,
                                                                          DailyFeatureSeries dailySeries) {
        Double microReturn = computeOneMinuteReturn(microSeries, evaluationTs);
        Double microVolatility = computeLogReturnStdev(microSeries);
        FlipScoreFeatureSet.ConfidenceLevel microConfidence = resolveMicroConfidence(microSeries, microReturn, microVolatility);

        Double macroReturn = resolveLatestDailyReturn(dailySeries, evaluationTs);
        Double macroVolatility = computeMacroVolatility(dailySeries.dailyLogReturns());
        FlipScoreFeatureSet.ConfidenceLevel macroConfidence = resolveMacroConfidence(dailySeries.dailyLogReturns().size());

        boolean structurallyIlliquid = isStructurallyIlliquid(latestRecord, dailySeries.dailyLiquidityObservations());
        return new FlipScoreFeatureSet.ItemTimescaleFeatures(
                microVolatility,
                microReturn,
                microConfidence,
                macroVolatility,
                macroReturn,
                macroConfidence,
                structurallyIlliquid
        );
    }

    private FlipScoreFeatureSet.ConfidenceLevel resolveMicroConfidence(List<PricePoint> points,
                                                                       Double microReturn,
                                                                       Double microVolatility) {
        int pointCount = points == null ? 0 : points.size();
        boolean hasSignal = microReturn != null || microVolatility != null;
        if (pointCount >= MICRO_HIGH_CONFIDENCE_POINTS && microReturn != null && microVolatility != null) {
            return FlipScoreFeatureSet.ConfidenceLevel.HIGH;
        }
        if (pointCount >= MICRO_MEDIUM_CONFIDENCE_POINTS && hasSignal) {
            return FlipScoreFeatureSet.ConfidenceLevel.MEDIUM;
        }
        return FlipScoreFeatureSet.ConfidenceLevel.LOW;
    }

    private FlipScoreFeatureSet.ConfidenceLevel resolveMacroConfidence(int returnCount) {
        if (returnCount >= MACRO_HIGH_CONFIDENCE_RETURNS) {
            return FlipScoreFeatureSet.ConfidenceLevel.HIGH;
        }
        if (returnCount >= MACRO_MEDIUM_CONFIDENCE_RETURNS) {
            return FlipScoreFeatureSet.ConfidenceLevel.MEDIUM;
        }
        return FlipScoreFeatureSet.ConfidenceLevel.LOW;
    }

    private Double computeOneMinuteReturn(List<PricePoint> points, Instant evaluationTs) {
        if (points == null || points.isEmpty() || evaluationTs == null) {
            return null;
        }
        PricePoint latest = points.getLast();
        if (latest.mid() <= 0) {
            return null;
        }

        Instant target = evaluationTs.minusSeconds(MICRO_WINDOW_SECONDS);
        PricePoint boundary = points.stream()
                .min(Comparator.comparingLong((PricePoint point) -> Math.abs(Duration.between(target, point.timestamp()).toMillis()))
                        .thenComparing(PricePoint::timestamp))
                .orElse(null);
        if (boundary == null || boundary.mid() <= 0 || !boundary.timestamp().isBefore(latest.timestamp())) {
            return null;
        }
        return safeLogRatio(latest.mid(), boundary.mid());
    }

    private DailyFeatureSeries buildDailySeries(List<DailyPoint> points) {
        if (points == null || points.isEmpty()) {
            return DailyFeatureSeries.empty();
        }
        List<DailyPoint> ordered = new ArrayList<>(points);
        ordered.sort(Comparator.comparing(DailyPoint::day));

        List<Double> dailyLogReturns = new ArrayList<>();
        for (int i = 1; i < ordered.size(); i++) {
            DailyPoint previous = ordered.get(i - 1);
            DailyPoint current = ordered.get(i);
            if (current.day() - previous.day() != 1L) {
                continue;
            }
            Double ret = safeLogRatio(current.mid(), previous.mid());
            if (ret != null) {
                dailyLogReturns.add(ret);
            }
        }
        return new DailyFeatureSeries(List.copyOf(ordered), dailyLogReturns);
    }

    private Double resolveLatestDailyReturn(DailyFeatureSeries dailySeries, Instant evaluationTs) {
        if (dailySeries == null || dailySeries.points().size() < 2 || evaluationTs == null) {
            return null;
        }
        long evaluationDay = epochDay(evaluationTs);
        List<DailyPoint> points = dailySeries.points();

        for (int i = points.size() - 1; i >= 1; i--) {
            DailyPoint current = points.get(i);
            DailyPoint previous = points.get(i - 1);
            if (current.day() - previous.day() != 1L) {
                continue;
            }
            if (current.day() == evaluationDay) {
                return safeLogRatio(current.mid(), previous.mid());
            }
        }

        DailyPoint latest = points.getLast();
        DailyPoint previous = points.get(points.size() - 2);
        if (latest.day() - previous.day() != 1L) {
            return null;
        }
        return safeLogRatio(latest.mid(), previous.mid());
    }

    private Double computeMacroVolatility(List<Double> dailyLogReturns) {
        if (dailyLogReturns == null || dailyLogReturns.isEmpty()) {
            return null;
        }
        int startIndex = Math.max(0, dailyLogReturns.size() - MACRO_WINDOW_DAYS);
        List<Double> tail = dailyLogReturns.subList(startIndex, dailyLogReturns.size());
        return computeStdev(tail);
    }

    private Double computeLogReturnStdev(List<PricePoint> points) {
        if (points == null || points.size() < 2) {
            return null;
        }
        List<Double> returns = new ArrayList<>();
        for (int i = 1; i < points.size(); i++) {
            PricePoint previous = points.get(i - 1);
            PricePoint current = points.get(i);
            Double ret = safeLogRatio(current.mid(), previous.mid());
            if (ret != null) {
                returns.add(clamp(ret, -MICRO_LOG_RETURN_CAP, MICRO_LOG_RETURN_CAP));
            }
        }
        return computeStdev(returns);
    }

    private boolean isStructurallyIlliquid(BazaarMarketRecord latestRecord, List<DailyLiquidityObservation> dailyObservations) {
        if (latestRecord == null) {
            return false;
        }
        double latestSpread = computeRelativeSpread(latestRecord);
        double latestTurnover = resolveConservativeTurnoverPerHour(latestRecord);
        if (latestSpread >= STRUCTURAL_SPREAD_THRESHOLD && latestTurnover <= STRUCTURAL_TURNOVER_PER_HOUR_THRESHOLD) {
            return true;
        }
        if (dailyObservations == null || dailyObservations.size() < 3) {
            return false;
        }

        int startIndex = Math.max(0, dailyObservations.size() - 7);
        List<DailyLiquidityObservation> recent = dailyObservations.subList(startIndex, dailyObservations.size());
        List<Double> spreads = new ArrayList<>(recent.size());
        List<Double> turnovers = new ArrayList<>(recent.size());
        for (DailyLiquidityObservation observation : recent) {
            spreads.add(observation.spreadRel());
            turnovers.add(observation.turnoverPerHour());
        }

        double medianSpread = median(spreads);
        double medianTurnover = median(turnovers);
        return medianSpread >= STRUCTURAL_SPREAD_THRESHOLD || medianTurnover <= STRUCTURAL_TURNOVER_PER_HOUR_THRESHOLD;
    }

    private Double resolveMid(Double buyPrice, Double sellPrice) {
        if (buyPrice == null || sellPrice == null) {
            return null;
        }
        double high = Math.max(buyPrice, sellPrice);
        double low = Math.min(buyPrice, sellPrice);
        double mid = (high + low) / 2D;
        if (mid <= 0D || Double.isNaN(mid) || Double.isInfinite(mid)) {
            return null;
        }
        return mid;
    }

    private Double resolveMid(BazaarMarketRecord record) {
        if (record == null) {
            return null;
        }
        return resolveMid(record.buyPrice(), record.sellPrice());
    }

    private double computeRelativeSpread(BazaarMarketRecord record) {
        if (record == null) {
            return 1D;
        }
        return computeRelativeSpread(record.buyPrice(), record.sellPrice());
    }

    private double computeRelativeSpread(Double buyPrice, Double sellPrice) {
        Double mid = resolveMid(buyPrice, sellPrice);
        if (mid == null) {
            return 1D;
        }
        double high = Math.max(buyPrice, sellPrice);
        double low = Math.min(buyPrice, sellPrice);
        return Math.max(0D, (high - low) / mid);
    }

    private double resolveConservativeTurnoverPerHour(BazaarMarketRecord record) {
        if (record == null) {
            return 0D;
        }
        double buyTurnover = record.buyMovingWeek() > 0 ? record.buyMovingWeek() / 168D : record.buyVolume() / 168D;
        double sellTurnover = record.sellMovingWeek() > 0 ? record.sellMovingWeek() / 168D : record.sellVolume() / 168D;
        return Math.max(0D, Math.min(buyTurnover, sellTurnover));
    }

    private double resolveConservativeTurnoverPerHour(Long buyVolume, Long sellVolume) {
        if (buyVolume == null || sellVolume == null) {
            return 0D;
        }
        return Math.max(0D, Math.min(buyVolume / 168D, sellVolume / 168D));
    }

    private Double safeLogRatio(double numerator, double denominator) {
        if (numerator <= 0D || denominator <= 0D) {
            return null;
        }
        double value = Math.log(numerator / denominator);
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return null;
        }
        return value;
    }

    private Double computeStdev(List<Double> values) {
        if (values == null || values.size() < 2) {
            return null;
        }
        double mean = values.stream().mapToDouble(Double::doubleValue).average().orElse(Double.NaN);
        if (Double.isNaN(mean) || Double.isInfinite(mean)) {
            return null;
        }
        double variance = values.stream()
                .mapToDouble(value -> {
                    double delta = value - mean;
                    return delta * delta;
                })
                .average()
                .orElse(Double.NaN);
        if (Double.isNaN(variance) || variance < 0D) {
            return null;
        }
        return Math.sqrt(variance);
    }

    private double median(List<Double> values) {
        if (values == null || values.isEmpty()) {
            return 0D;
        }
        List<Double> copy = new ArrayList<>(values);
        copy.sort(Double::compareTo);
        int mid = copy.size() / 2;
        if (copy.size() % 2 == 0) {
            return (copy.get(mid - 1) + copy.get(mid)) / 2D;
        }
        return copy.get(mid);
    }

    private double clamp(double value, double min, double max) {
        return Math.max(min, Math.min(max, value));
    }

    private long epochDay(Instant instant) {
        if (instant == null) {
            return 0L;
        }
        return Math.floorDiv(instant.getEpochSecond(), SECONDS_PER_UTC_DAY);
    }

    private record PricePoint(
            Instant timestamp,
            double mid
    ) {
    }

    private record DailyPoint(
            long day,
            double mid,
            double spreadRel,
            double turnoverPerHour
    ) {
    }

    private record DailyLiquidityObservation(
            double spreadRel,
            double turnoverPerHour
    ) {
    }

    private record DailyFeatureSeries(
            List<DailyPoint> points,
            List<Double> dailyLogReturns
    ) {
        private static DailyFeatureSeries empty() {
            return new DailyFeatureSeries(List.of(), List.of());
        }

        private List<DailyLiquidityObservation> dailyLiquidityObservations() {
            if (points.isEmpty()) {
                return List.of();
            }
            List<DailyLiquidityObservation> observations = new ArrayList<>(points.size());
            for (DailyPoint point : points) {
                observations.add(new DailyLiquidityObservation(point.spreadRel(), point.turnoverPerHour()));
            }
            return observations;
        }
    }
}
