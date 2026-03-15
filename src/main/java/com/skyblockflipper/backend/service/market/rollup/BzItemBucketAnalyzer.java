package com.skyblockflipper.backend.service.market.rollup;

import com.skyblockflipper.backend.model.market.BzItemAnomalySegmentEntity;
import com.skyblockflipper.backend.model.market.BzItemBucketRollupEntity;
import com.skyblockflipper.backend.model.market.BzItemSnapshotEntity;
import com.skyblockflipper.backend.service.market.SnapshotRollupProperties;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

@Service
public class BzItemBucketAnalyzer {

    private static final long DEFAULT_SAMPLE_CADENCE_MILLIS = 5_000L;
    private static final double EPSILON = 1e-9D;
    private static final double ANOMALY_SHARE_THRESHOLD = 0.20D;

    private final SnapshotRollupProperties properties;

    public BzItemBucketAnalyzer(SnapshotRollupProperties properties) {
        this.properties = Objects.requireNonNull(properties, "properties must not be null");
    }

    public BzItemBucketAnalysisResult analyze(MarketBucketGranularity granularity,
                                              long bucketStartEpochMillis,
                                              long bucketEndEpochMillis,
                                              String productId,
                                              List<BzItemSnapshotEntity> rawSamples) {
        if (granularity == null || productId == null || productId.isBlank() || bucketEndEpochMillis <= bucketStartEpochMillis) {
            return new BzItemBucketAnalysisResult(Optional.empty(), List.of());
        }
        List<BzSamplePoint> validSamples = toValidSamples(productId, bucketStartEpochMillis, bucketEndEpochMillis, rawSamples);
        if (validSamples.isEmpty()) {
            return new BzItemBucketAnalysisResult(Optional.empty(), List.of());
        }

        List<BzSamplePoint> anomalySamples = List.of();
        List<BzItemAnomalySegmentEntity> anomalySegments = List.of();
        if (properties.getAnomaly().isEnabled() && validSamples.size() >= 2) {
            AnomalySelection selection = selectAnomalySamples(validSamples);
            anomalySamples = selection.samples();
            if (!anomalySamples.isEmpty()) {
                anomalySegments = List.of(toAnomalySegment(
                        granularity,
                        bucketStartEpochMillis,
                        bucketEndEpochMillis,
                        productId,
                        anomalySamples,
                        selection.fragmented(),
                        selection.reasonCode(),
                        selection.peakSnapshotTs(),
                        selection.anomalyScore()
                ));
            }
        }

        Set<Long> anomalyTimestamps = anomalySamples.stream()
                .map(BzSamplePoint::snapshotTs)
                .collect(java.util.stream.Collectors.toCollection(HashSet::new));
        List<BzSamplePoint> rollupSamples = validSamples.stream()
                .filter(sample -> !anomalyTimestamps.contains(sample.snapshotTs()))
                .toList();
        boolean fallbackToValidSamples = rollupSamples.isEmpty();
        List<BzSamplePoint> effectiveRollupSamples = fallbackToValidSamples ? validSamples : rollupSamples;

        SnapshotRollupProperties.Anomaly anomalyProperties = properties.getAnomaly();
        int minValidSamples = minValidSamples(granularity, anomalyProperties);
        int expectedSampleCount = granularity.expectedSampleCount(DEFAULT_SAMPLE_CADENCE_MILLIS);
        double coverageRatio = validSamples.size() / (double) expectedSampleCount;
        boolean partial = validSamples.size() < minValidSamples
                || coverageRatio < anomalyProperties.getMinBucketCoverageRatio()
                || fallbackToValidSamples;

        BzItemBucketRollupEntity rollup = toRollup(
                granularity,
                bucketStartEpochMillis,
                bucketEndEpochMillis,
                productId,
                rawSamples == null ? 0 : rawSamples.size(),
                validSamples,
                effectiveRollupSamples,
                anomalySamples.size(),
                partial
        );
        return new BzItemBucketAnalysisResult(Optional.of(rollup), anomalySegments);
    }

    private List<BzSamplePoint> toValidSamples(String productId,
                                               long bucketStartEpochMillis,
                                               long bucketEndEpochMillis,
                                               List<BzItemSnapshotEntity> rawSamples) {
        if (rawSamples == null || rawSamples.isEmpty()) {
            return List.of();
        }
        List<BzSamplePoint> points = new ArrayList<>();
        for (BzItemSnapshotEntity row : rawSamples) {
            if (row == null || !productId.equals(row.getProductId())) {
                continue;
            }
            long ts = row.getSnapshotTs();
            if (ts < bucketStartEpochMillis || ts >= bucketEndEpochMillis) {
                continue;
            }
            Double buyPrice = row.getBuyPrice();
            Double sellPrice = row.getSellPrice();
            if (buyPrice == null || sellPrice == null || buyPrice <= 0.0D || sellPrice <= 0.0D) {
                continue;
            }
            double midPrice = (buyPrice + sellPrice) / 2.0D;
            if (midPrice <= 0.0D) {
                continue;
            }
            long buyVolume = row.getBuyVolume() == null ? 0L : Math.max(0L, row.getBuyVolume());
            long sellVolume = row.getSellVolume() == null ? 0L : Math.max(0L, row.getSellVolume());
            double liquidity = Math.min(buyVolume, sellVolume);
            double relativeSpread = (buyPrice - sellPrice) / midPrice;
            points.add(new BzSamplePoint(
                    ts,
                    buyPrice,
                    sellPrice,
                    buyVolume,
                    sellVolume,
                    midPrice,
                    relativeSpread,
                    liquidity,
                    Math.log(midPrice),
                    Math.log1p(liquidity)
            ));
        }
        points.sort(Comparator.comparingLong(BzSamplePoint::snapshotTs));
        return List.copyOf(points);
    }

    private AnomalySelection selectAnomalySamples(List<BzSamplePoint> validSamples) {
        MetricCenters centers = MetricCenters.from(validSamples);
        List<AnomalyDecision> decisions = new ArrayList<>();
        for (BzSamplePoint sample : validSamples) {
            AnomalyDecision decision = detectAnomaly(sample, centers);
            if (decision != null) {
                decisions.add(decision);
            }
        }
        if (decisions.isEmpty()) {
            return AnomalySelection.empty();
        }

        List<AnomalyGroup> groups = mergeGroups(decisions);
        List<AnomalyGroup> qualifyingGroups = groups.stream()
                .filter(group -> qualifies(group, validSamples.size()))
                .toList();
        if (qualifyingGroups.isEmpty()) {
            return AnomalySelection.empty();
        }

        List<BzSamplePoint> anomalySamples = new ArrayList<>();
        boolean fragmented = qualifyingGroups.size() > 1;
        double peakScore = Double.NEGATIVE_INFINITY;
        long peakSnapshotTs = qualifyingGroups.getFirst().samples().getFirst().sample().snapshotTs();
        String reasonCode = fragmented ? "MULTI_ANOMALY_CLUSTER" : qualifyingGroups.getFirst().reasonCode();
        for (AnomalyGroup group : qualifyingGroups) {
            for (AnomalyDecision decision : group.samples()) {
                anomalySamples.add(decision.sample());
                if (decision.score() > peakScore) {
                    peakScore = decision.score();
                    peakSnapshotTs = decision.sample().snapshotTs();
                    if (!fragmented) {
                        reasonCode = decision.reasonCode();
                    }
                }
            }
        }
        anomalySamples.sort(Comparator.comparingLong(BzSamplePoint::snapshotTs));
        return new AnomalySelection(List.copyOf(anomalySamples), fragmented, reasonCode, peakSnapshotTs, peakScore);
    }

    private List<AnomalyGroup> mergeGroups(List<AnomalyDecision> decisions) {
        if (decisions.isEmpty()) {
            return List.of();
        }
        long maxGapMillis = Math.max(0L, properties.getAnomaly().getMergeGapSeconds()) * 1_000L;
        List<AnomalyGroup> groups = new ArrayList<>();
        List<AnomalyDecision> current = new ArrayList<>();
        current.add(decisions.getFirst());
        for (int i = 1; i < decisions.size(); i++) {
            AnomalyDecision previous = decisions.get(i - 1);
            AnomalyDecision decision = decisions.get(i);
            if (decision.sample().snapshotTs() - previous.sample().snapshotTs() <= maxGapMillis) {
                current.add(decision);
                continue;
            }
            groups.add(toGroup(current));
            current = new ArrayList<>();
            current.add(decision);
        }
        groups.add(toGroup(current));
        return List.copyOf(groups);
    }

    private AnomalyGroup toGroup(List<AnomalyDecision> decisions) {
        double peakScore = decisions.stream().mapToDouble(AnomalyDecision::score).max().orElse(0.0D);
        String reasonCode = decisions.stream()
                .max(Comparator.comparingDouble(AnomalyDecision::score))
                .map(AnomalyDecision::reasonCode)
                .orElse("ANOMALY");
        return new AnomalyGroup(List.copyOf(decisions), peakScore, reasonCode);
    }

    private boolean qualifies(AnomalyGroup group, int totalValidSamples) {
        long start = group.samples().getFirst().sample().snapshotTs();
        long end = group.samples().getLast().sample().snapshotTs();
        long durationMillis = Math.max(0L, end - start);
        int sampleCount = group.samples().size();
        return sampleCount >= Math.max(1, properties.getAnomaly().getConsecutiveSamplesThreshold())
                || durationMillis >= Math.max(0L, properties.getAnomaly().getAnomalyDurationThresholdSeconds()) * 1_000L
                || ((double) sampleCount / Math.max(1, totalValidSamples)) >= ANOMALY_SHARE_THRESHOLD;
    }

    private AnomalyDecision detectAnomaly(BzSamplePoint sample, MetricCenters centers) {
        List<AnomalySignal> signals = new ArrayList<>();

        double priceScore = Math.abs(robustZ(sample.logMidPrice(), centers.medianLogMidPrice(), centers.madLogMidPrice()));
        double priceRelativeChange = Math.abs(sample.midPrice() / centers.medianMidPrice() - 1.0D);
        if (priceScore >= properties.getAnomaly().getZThreshold()
                && priceRelativeChange >= properties.getAnomaly().getBazaarRelativePriceFloor()) {
            signals.add(new AnomalySignal("PRICE_SPIKE", priceScore));
        }

        if (centers.medianSpread() > EPSILON) {
            double spreadScore = robustZ(sample.relativeSpread(), centers.medianSpread(), centers.madSpread());
            if (spreadScore >= properties.getAnomaly().getZThreshold()
                    && sample.relativeSpread() >= centers.medianSpread() * properties.getAnomaly().getSpreadMultiplierThreshold()) {
                signals.add(new AnomalySignal("SPREAD_BLOWOUT", spreadScore));
            }
        }

        if (centers.medianLiquidity() > EPSILON) {
            double liquidityScore = -robustZ(sample.logLiquidity(), centers.medianLogLiquidity(), centers.madLogLiquidity());
            if (liquidityScore >= properties.getAnomaly().getZThreshold()
                    && sample.liquidity() <= centers.medianLiquidity() * properties.getAnomaly().getLiquidityDropThreshold()) {
                signals.add(new AnomalySignal("LIQUIDITY_COLLAPSE", liquidityScore));
            }
        }

        if (signals.isEmpty()) {
            return null;
        }
        AnomalySignal strongest = signals.stream()
                .max(Comparator.comparingDouble(AnomalySignal::score))
                .orElseGet(() -> new AnomalySignal("ANOMALY", 0.0D));
        String reasonCode = signals.size() > 1 ? "MULTI_SIGNAL" : strongest.reasonCode();
        return new AnomalyDecision(sample, reasonCode, strongest.score());
    }

    private BzItemBucketRollupEntity toRollup(MarketBucketGranularity granularity,
                                              long bucketStartEpochMillis,
                                              long bucketEndEpochMillis,
                                              String productId,
                                              int sampleCount,
                                              List<BzSamplePoint> validSamples,
                                              List<BzSamplePoint> rollupSamples,
                                              int anomalySampleCount,
                                              boolean partial) {
        long now = System.currentTimeMillis();
        BzItemBucketRollupEntity rollup = new BzItemBucketRollupEntity();
        rollup.setBucketStartEpochMillis(bucketStartEpochMillis);
        rollup.setBucketEndEpochMillis(bucketEndEpochMillis);
        rollup.setBucketGranularity(granularity.code());
        rollup.setProductId(productId);
        rollup.setSampleCount(sampleCount);
        rollup.setValidSampleCount(validSamples.size());
        rollup.setAnomalySampleCount(anomalySampleCount);
        rollup.setPartial(partial);
        rollup.setRepresentativeSnapshotTs(chooseRepresentativeSnapshotTs(rollupSamples));
        rollup.setMedianBuyPrice(RobustStatistics.median(values(rollupSamples, BzSamplePoint::buyPrice)));
        rollup.setMedianSellPrice(RobustStatistics.median(values(rollupSamples, BzSamplePoint::sellPrice)));
        rollup.setMedianMidPrice(RobustStatistics.median(values(rollupSamples, BzSamplePoint::midPrice)));
        rollup.setMedianSpread(RobustStatistics.median(values(rollupSamples, BzSamplePoint::relativeSpread)));
        rollup.setP10MidPrice(RobustStatistics.percentile(values(rollupSamples, BzSamplePoint::midPrice), 0.10D));
        rollup.setP25MidPrice(RobustStatistics.percentile(values(rollupSamples, BzSamplePoint::midPrice), 0.25D));
        rollup.setP75MidPrice(RobustStatistics.percentile(values(rollupSamples, BzSamplePoint::midPrice), 0.75D));
        rollup.setP90MidPrice(RobustStatistics.percentile(values(rollupSamples, BzSamplePoint::midPrice), 0.90D));
        rollup.setMinMidPrice(RobustStatistics.min(values(rollupSamples, BzSamplePoint::midPrice)));
        rollup.setMaxMidPrice(RobustStatistics.max(values(rollupSamples, BzSamplePoint::midPrice)));
        rollup.setWinsorizedAvgMidPrice(RobustStatistics.winsorizedMean(values(rollupSamples, BzSamplePoint::midPrice), 0.10D, 0.90D));
        rollup.setMedianBuyVolume(RobustStatistics.median(values(rollupSamples, sample -> (double) sample.buyVolume())));
        rollup.setMedianSellVolume(RobustStatistics.median(values(rollupSamples, sample -> (double) sample.sellVolume())));
        rollup.setMinLiquidity(RobustStatistics.min(values(rollupSamples, BzSamplePoint::liquidity)));
        rollup.setMaxLiquidity(RobustStatistics.max(values(rollupSamples, BzSamplePoint::liquidity)));
        rollup.setFirstSnapshotTs(validSamples.getFirst().snapshotTs());
        rollup.setLastSnapshotTs(validSamples.getLast().snapshotTs());
        rollup.setCreatedAtEpochMillis(now);
        rollup.setUpdatedAtEpochMillis(now);
        return rollup;
    }

    private BzItemAnomalySegmentEntity toAnomalySegment(MarketBucketGranularity granularity,
                                                        long bucketStartEpochMillis,
                                                        long bucketEndEpochMillis,
                                                        String productId,
                                                        List<BzSamplePoint> anomalySamples,
                                                        boolean fragmented,
                                                        String reasonCode,
                                                        long peakSnapshotTs,
                                                        double anomalyScore) {
        long now = System.currentTimeMillis();
        BzItemAnomalySegmentEntity segment = new BzItemAnomalySegmentEntity();
        segment.setBucketStartEpochMillis(bucketStartEpochMillis);
        segment.setBucketEndEpochMillis(bucketEndEpochMillis);
        segment.setBucketGranularity(granularity.code());
        segment.setProductId(productId);
        segment.setSegmentStartEpochMillis(anomalySamples.getFirst().snapshotTs());
        segment.setSegmentEndEpochMillis(anomalySamples.getLast().snapshotTs());
        segment.setRepresentativeSnapshotTs(chooseRepresentativeSnapshotTs(anomalySamples));
        segment.setPeakSnapshotTs(peakSnapshotTs);
        segment.setSampleCount(anomalySamples.size());
        segment.setAnomalyScore(anomalyScore);
        segment.setReasonCode(reasonCode);
        segment.setFragmented(fragmented);
        segment.setMedianBuyPrice(RobustStatistics.median(values(anomalySamples, BzSamplePoint::buyPrice)));
        segment.setMedianSellPrice(RobustStatistics.median(values(anomalySamples, BzSamplePoint::sellPrice)));
        segment.setMedianMidPrice(RobustStatistics.median(values(anomalySamples, BzSamplePoint::midPrice)));
        segment.setMedianSpread(RobustStatistics.median(values(anomalySamples, BzSamplePoint::relativeSpread)));
        segment.setMedianBuyVolume(RobustStatistics.median(values(anomalySamples, sample -> (double) sample.buyVolume())));
        segment.setMedianSellVolume(RobustStatistics.median(values(anomalySamples, sample -> (double) sample.sellVolume())));
        segment.setCreatedAtEpochMillis(now);
        segment.setUpdatedAtEpochMillis(now);
        return segment;
    }

    private Long chooseRepresentativeSnapshotTs(List<BzSamplePoint> samples) {
        if (samples == null || samples.isEmpty()) {
            return null;
        }
        MetricCenters centers = MetricCenters.from(samples);
        BzSamplePoint representative = samples.stream()
                .min(Comparator.<BzSamplePoint>comparingDouble(sample -> distanceToCenter(sample, centers))
                        .thenComparingLong(BzSamplePoint::snapshotTs))
                .orElse(samples.getFirst());
        return representative.snapshotTs();
    }

    private double distanceToCenter(BzSamplePoint sample, MetricCenters centers) {
        double distance = 0.0D;
        distance += Math.abs(sample.logMidPrice() - centers.medianLogMidPrice()) / safeScale(centers.madLogMidPrice());
        distance += Math.abs(sample.relativeSpread() - centers.medianSpread()) / safeScale(centers.madSpread());
        distance += Math.abs(sample.logLiquidity() - centers.medianLogLiquidity()) / safeScale(centers.madLogLiquidity());
        return distance;
    }

    private int minValidSamples(MarketBucketGranularity granularity, SnapshotRollupProperties.Anomaly anomalyProperties) {
        return switch (granularity) {
            case ONE_MINUTE -> anomalyProperties.getMinValidSamples1m();
            case TWO_HOURS -> anomalyProperties.getMinValidSamples2h();
            case ONE_DAY -> anomalyProperties.getMinValidSamples1d();
        };
    }

    private double robustZ(double value, double median, double mad) {
        return 0.6745D * (value - median) / safeScale(mad);
    }

    private double safeScale(double mad) {
        return Math.max(EPSILON, mad);
    }

    private List<Double> values(List<BzSamplePoint> samples, Function<BzSamplePoint, Double> extractor) {
        List<Double> values = new ArrayList<>();
        if (samples == null || samples.isEmpty()) {
            return values;
        }
        for (BzSamplePoint sample : samples) {
            if (sample == null) {
                continue;
            }
            Double value = extractor.apply(sample);
            if (value != null) {
                values.add(value);
            }
        }
        return values;
    }

    private record BzSamplePoint(long snapshotTs,
                                 double buyPrice,
                                 double sellPrice,
                                 long buyVolume,
                                 long sellVolume,
                                 double midPrice,
                                 double relativeSpread,
                                 double liquidity,
                                 double logMidPrice,
                                 double logLiquidity) {
    }

    private record AnomalySignal(String reasonCode, double score) {
    }

    private record AnomalyDecision(BzSamplePoint sample, String reasonCode, double score) {
    }

    private record AnomalyGroup(List<AnomalyDecision> samples, double peakScore, String reasonCode) {
    }

    private record AnomalySelection(List<BzSamplePoint> samples,
                                    boolean fragmented,
                                    String reasonCode,
                                    long peakSnapshotTs,
                                    double anomalyScore) {
        private static AnomalySelection empty() {
            return new AnomalySelection(List.of(), false, null, 0L, 0.0D);
        }
    }

    private record MetricCenters(double medianMidPrice,
                                 double medianSpread,
                                 double medianLiquidity,
                                 double medianLogMidPrice,
                                 double madLogMidPrice,
                                 double madSpread,
                                 double medianLogLiquidity,
                                 double madLogLiquidity) {

        private static MetricCenters from(List<BzSamplePoint> samples) {
            List<Double> midPrices = samples.stream().map(BzSamplePoint::midPrice).toList();
            List<Double> spreads = samples.stream().map(BzSamplePoint::relativeSpread).toList();
            List<Double> liquidities = samples.stream().map(BzSamplePoint::liquidity).toList();
            List<Double> logMidPrices = samples.stream().map(BzSamplePoint::logMidPrice).toList();
            List<Double> logLiquidities = samples.stream().map(BzSamplePoint::logLiquidity).toList();
            return new MetricCenters(
                    defaultIfNull(RobustStatistics.median(midPrices), 0.0D),
                    defaultIfNull(RobustStatistics.median(spreads), 0.0D),
                    defaultIfNull(RobustStatistics.median(liquidities), 0.0D),
                    defaultIfNull(RobustStatistics.median(logMidPrices), 0.0D),
                    defaultIfNull(RobustStatistics.mad(logMidPrices), 0.0D),
                    defaultIfNull(RobustStatistics.mad(spreads), 0.0D),
                    defaultIfNull(RobustStatistics.median(logLiquidities), 0.0D),
                    defaultIfNull(RobustStatistics.mad(logLiquidities), 0.0D)
            );
        }

        private static double defaultIfNull(Double value, double fallback) {
            return value == null ? fallback : value;
        }
    }
}
