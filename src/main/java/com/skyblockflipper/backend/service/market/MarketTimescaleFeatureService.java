package com.skyblockflipper.backend.service.market;

import com.skyblockflipper.backend.model.market.BazaarMarketRecord;
import com.skyblockflipper.backend.model.market.MarketSnapshot;
import com.skyblockflipper.backend.service.flipping.FlipScoreFeatureSet;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class MarketTimescaleFeatureService {

    private static final long SECONDS_PER_UTC_DAY = 86_400L;
    private static final int MICRO_WINDOW_SECONDS = 60;
    private static final int MACRO_WINDOW_DAYS = 30;
    private static final int MICRO_HIGH_CONFIDENCE_POINTS = 10;
    private static final int MICRO_MEDIUM_CONFIDENCE_POINTS = 6;
    private static final int MACRO_HIGH_CONFIDENCE_RETURNS = 7;
    private static final int MACRO_MEDIUM_CONFIDENCE_RETURNS = 3;
    private static final double MICRO_LOG_RETURN_CAP = 0.20D;
    private static final double STRUCTURAL_SPREAD_THRESHOLD = 0.05D;
    private static final double STRUCTURAL_TURNOVER_PER_HOUR_THRESHOLD = 10D;

    private final MarketSnapshotPersistenceService marketSnapshotPersistenceService;

    public MarketTimescaleFeatureService(MarketSnapshotPersistenceService marketSnapshotPersistenceService) {
        this.marketSnapshotPersistenceService = marketSnapshotPersistenceService;
    }

    public FlipScoreFeatureSet computeFor(MarketSnapshot latestSnapshot) {
        if (latestSnapshot == null || latestSnapshot.bazaarProducts().isEmpty()) {
            return FlipScoreFeatureSet.empty();
        }

        Instant evaluationTs = latestSnapshot.snapshotTimestamp();
        List<MarketSnapshot> microSnapshots = marketSnapshotPersistenceService
                .between(evaluationTs.minusSeconds(MICRO_WINDOW_SECONDS), evaluationTs);
        long earliestEpochDay = epochDay(evaluationTs) - (MACRO_WINDOW_DAYS + 2L);
        Instant macroStartInclusive = Instant.ofEpochSecond(earliestEpochDay * SECONDS_PER_UTC_DAY);
        List<MarketSnapshot> dailySnapshots = marketSnapshotPersistenceService
                .between(macroStartInclusive, evaluationTs);

        Map<String, List<PricePoint>> microSeriesByItem = buildMicroSeriesByItem(microSnapshots);
        Map<Long, MarketSnapshot> dailyAnchors = buildDailyAnchors(dailySnapshots);

        Map<String, FlipScoreFeatureSet.ItemTimescaleFeatures> byItem = new LinkedHashMap<>();
        for (Map.Entry<String, BazaarMarketRecord> entry : latestSnapshot.bazaarProducts().entrySet()) {
            String itemId = entry.getKey();
            BazaarMarketRecord latestRecord = entry.getValue();
            List<PricePoint> microSeries = microSeriesByItem.getOrDefault(itemId, List.of());
            byItem.put(itemId, computeItemFeatures(itemId, evaluationTs, latestRecord, microSeries, dailyAnchors));
        }
        return new FlipScoreFeatureSet(byItem);
    }

    private Map<String, List<PricePoint>> buildMicroSeriesByItem(List<MarketSnapshot> snapshots) {
        if (snapshots == null || snapshots.isEmpty()) {
            return Map.of();
        }
        Map<String, List<PricePoint>> byItem = new LinkedHashMap<>();
        for (MarketSnapshot snapshot : snapshots) {
            if (snapshot == null || snapshot.bazaarProducts().isEmpty()) {
                continue;
            }
            Instant ts = snapshot.snapshotTimestamp();
            for (Map.Entry<String, BazaarMarketRecord> entry : snapshot.bazaarProducts().entrySet()) {
                Double mid = resolveMid(entry.getValue());
                if (mid == null) {
                    continue;
                }
                byItem.computeIfAbsent(entry.getKey(), ignored -> new ArrayList<>())
                        .add(new PricePoint(ts, mid));
            }
        }
        for (List<PricePoint> points : byItem.values()) {
            points.sort(Comparator.comparing(PricePoint::timestamp));
        }
        return byItem;
    }

    private Map<Long, MarketSnapshot> buildDailyAnchors(List<MarketSnapshot> snapshots) {
        if (snapshots == null || snapshots.isEmpty()) {
            return Map.of();
        }
        Map<Long, SnapshotAnchorCandidate> byDay = new LinkedHashMap<>();
        for (MarketSnapshot snapshot : snapshots) {
            if (snapshot == null) {
                continue;
            }
            Instant ts = snapshot.snapshotTimestamp();
            long day = epochDay(ts);
            SnapshotAnchorCandidate current = byDay.get(day);
            SnapshotAnchorCandidate candidate = new SnapshotAnchorCandidate(snapshot, ts.toEpochMilli());
            if (current == null || candidate.isBetterThan(current)) {
                byDay.put(day, candidate);
            }
        }

        Map<Long, MarketSnapshot> anchors = new LinkedHashMap<>();
        byDay.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> anchors.put(entry.getKey(), entry.getValue().snapshot()));
        return anchors;
    }

    private FlipScoreFeatureSet.ItemTimescaleFeatures computeItemFeatures(String itemId,
                                                                          Instant evaluationTs,
                                                                          BazaarMarketRecord latestRecord,
                                                                          List<PricePoint> microSeries,
                                                                          Map<Long, MarketSnapshot> dailyAnchors) {
        Double microReturn = computeOneMinuteReturn(microSeries, evaluationTs);
        Double microVolatility = computeLogReturnStdev(microSeries);
        FlipScoreFeatureSet.ConfidenceLevel microConfidence = resolveMicroConfidence(microSeries, microReturn, microVolatility);

        DailyFeatureSeries dailySeries = buildDailySeries(itemId, dailyAnchors);
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

    private DailyFeatureSeries buildDailySeries(String itemId, Map<Long, MarketSnapshot> dailyAnchors) {
        if (itemId == null || itemId.isBlank() || dailyAnchors == null || dailyAnchors.isEmpty()) {
            return DailyFeatureSeries.empty();
        }

        List<DailyPoint> points = new ArrayList<>();
        for (Map.Entry<Long, MarketSnapshot> entry : dailyAnchors.entrySet()) {
            MarketSnapshot snapshot = entry.getValue();
            if (snapshot == null) {
                continue;
            }
            BazaarMarketRecord record = snapshot.bazaarProducts().get(itemId);
            if (record == null) {
                continue;
            }
            Double mid = resolveMid(record);
            if (mid == null) {
                continue;
            }
            points.add(new DailyPoint(entry.getKey(), mid, computeRelativeSpread(record), resolveConservativeTurnoverPerHour(record)));
        }
        if (points.isEmpty()) {
            return DailyFeatureSeries.empty();
        }

        points.sort(Comparator.comparing(DailyPoint::day));
        List<Double> dailyLogReturns = new ArrayList<>();
        for (int i = 1; i < points.size(); i++) {
            DailyPoint previous = points.get(i - 1);
            DailyPoint current = points.get(i);
            if (current.day() - previous.day() != 1L) {
                continue;
            }
            Double ret = safeLogRatio(current.mid(), previous.mid());
            if (ret != null) {
                dailyLogReturns.add(ret);
            }
        }
        return new DailyFeatureSeries(points, dailyLogReturns);
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

    private Double resolveMid(BazaarMarketRecord record) {
        if (record == null) {
            return null;
        }
        double high = Math.max(record.buyPrice(), record.sellPrice());
        double low = Math.min(record.buyPrice(), record.sellPrice());
        double mid = (high + low) / 2D;
        if (mid <= 0D || Double.isNaN(mid) || Double.isInfinite(mid)) {
            return null;
        }
        return mid;
    }

    private double computeRelativeSpread(BazaarMarketRecord record) {
        Double mid = resolveMid(record);
        if (mid == null) {
            return 1D;
        }
        double high = Math.max(record.buyPrice(), record.sellPrice());
        double low = Math.min(record.buyPrice(), record.sellPrice());
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

    private record SnapshotAnchorCandidate(
            MarketSnapshot snapshot,
            long timestampEpochMillis
    ) {
        private boolean isBetterThan(SnapshotAnchorCandidate other) {
            return timestampEpochMillis < other.timestampEpochMillis;
        }
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
