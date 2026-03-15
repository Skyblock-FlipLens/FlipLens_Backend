package com.skyblockflipper.backend.service.market.rollup;

import com.skyblockflipper.backend.model.market.AhItemAnomalySegmentEntity;
import com.skyblockflipper.backend.model.market.AhItemBucketRollupEntity;
import com.skyblockflipper.backend.model.market.AhItemSnapshotEntity;
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
public class AhItemBucketAnalyzer {

    private static final long DEFAULT_SAMPLE_CADENCE_MILLIS = 5_000L;
    private static final double EPSILON = 1e-9D;
    private static final double ANOMALY_SHARE_THRESHOLD = 0.20D;

    private final SnapshotRollupProperties properties;

    public AhItemBucketAnalyzer(SnapshotRollupProperties properties) {
        this.properties = Objects.requireNonNull(properties, "properties must not be null");
    }

    public AhItemBucketAnalysisResult analyze(MarketBucketGranularity granularity,
                                              long bucketStartEpochMillis,
                                              long bucketEndEpochMillis,
                                              String itemKey,
                                              List<AhItemSnapshotEntity> rawSamples) {
        if (granularity == null || itemKey == null || itemKey.isBlank() || bucketEndEpochMillis <= bucketStartEpochMillis) {
            return new AhItemBucketAnalysisResult(Optional.empty(), List.of());
        }
        List<AhSamplePoint> validSamples = toValidSamples(itemKey, bucketStartEpochMillis, bucketEndEpochMillis, rawSamples);
        if (validSamples.isEmpty()) {
            return new AhItemBucketAnalysisResult(Optional.empty(), List.of());
        }

        List<AhSamplePoint> anomalySamples = List.of();
        List<AhItemAnomalySegmentEntity> anomalySegments = List.of();
        if (properties.getAnomaly().isEnabled() && validSamples.size() >= 2) {
            AhAnomalySelection selection = selectAnomalySamples(validSamples);
            anomalySamples = selection.samples();
            if (!anomalySamples.isEmpty()) {
                anomalySegments = List.of(toAnomalySegment(
                        granularity,
                        bucketStartEpochMillis,
                        bucketEndEpochMillis,
                        itemKey,
                        anomalySamples,
                        selection.fragmented(),
                        selection.reasonCode(),
                        selection.peakSnapshotTs(),
                        selection.anomalyScore()
                ));
            }
        }

        Set<Long> anomalyTimestamps = anomalySamples.stream()
                .map(AhSamplePoint::snapshotTs)
                .collect(java.util.stream.Collectors.toCollection(HashSet::new));
        List<AhSamplePoint> rollupSamples = validSamples.stream()
                .filter(sample -> !anomalyTimestamps.contains(sample.snapshotTs()))
                .toList();
        boolean fallbackToValidSamples = rollupSamples.isEmpty();
        List<AhSamplePoint> effectiveRollupSamples = fallbackToValidSamples ? validSamples : rollupSamples;

        SnapshotRollupProperties.Anomaly anomalyProperties = properties.getAnomaly();
        int minValidSamples = minValidSamples(granularity, anomalyProperties);
        int expectedSampleCount = granularity.expectedSampleCount(DEFAULT_SAMPLE_CADENCE_MILLIS);
        double coverageRatio = validSamples.size() / (double) expectedSampleCount;
        boolean partial = validSamples.size() < minValidSamples
                || coverageRatio < anomalyProperties.getMinBucketCoverageRatio()
                || fallbackToValidSamples;

        AhItemBucketRollupEntity rollup = toRollup(
                granularity,
                bucketStartEpochMillis,
                bucketEndEpochMillis,
                itemKey,
                rawSamples == null ? 0 : rawSamples.size(),
                validSamples,
                effectiveRollupSamples,
                anomalySamples.size(),
                partial
        );
        return new AhItemBucketAnalysisResult(Optional.of(rollup), anomalySegments);
    }

    private List<AhSamplePoint> toValidSamples(String itemKey,
                                               long bucketStartEpochMillis,
                                               long bucketEndEpochMillis,
                                               List<AhItemSnapshotEntity> rawSamples) {
        if (rawSamples == null || rawSamples.isEmpty()) {
            return List.of();
        }
        List<AhSamplePoint> points = new ArrayList<>();
        for (AhItemSnapshotEntity row : rawSamples) {
            if (row == null || !itemKey.equals(row.getItemKey())) {
                continue;
            }
            long ts = row.getSnapshotTs();
            if (ts < bucketStartEpochMillis || ts >= bucketEndEpochMillis) {
                continue;
            }
            Double priceAnchor = positiveDouble(row.getBinLowest5Mean());
            Double binP50 = positiveDouble(row.getBinP50());
            if (priceAnchor == null && binP50 == null) {
                continue;
            }
            if (priceAnchor == null) {
                priceAnchor = binP50;
            }
            long binCount = Math.max(0, row.getBinCount());
            int endingSoonCount = Math.max(0, row.getEndingSoonCount());
            points.add(new AhSamplePoint(
                    ts,
                    priceAnchor,
                    positiveDouble(row.getBinLowest5Mean()),
                    binP50,
                    positiveDouble(row.getBinP95()),
                    positiveDouble(row.getBidP50()),
                    (double) binCount,
                    (double) endingSoonCount,
                    Math.log(priceAnchor),
                    Math.log1p(binCount)
            ));
        }
        points.sort(Comparator.comparingLong(AhSamplePoint::snapshotTs));
        return List.copyOf(points);
    }

    private AhAnomalySelection selectAnomalySamples(List<AhSamplePoint> validSamples) {
        AhMetricCenters centers = AhMetricCenters.from(validSamples);
        List<AhAnomalyDecision> decisions = new ArrayList<>();
        for (AhSamplePoint sample : validSamples) {
            AhAnomalyDecision decision = detectAnomaly(sample, centers);
            if (decision != null) {
                decisions.add(decision);
            }
        }
        if (decisions.isEmpty()) {
            return AhAnomalySelection.empty();
        }

        List<AhAnomalyGroup> groups = mergeGroups(decisions);
        List<AhAnomalyGroup> qualifyingGroups = groups.stream()
                .filter(group -> qualifies(group, validSamples.size()))
                .toList();
        if (qualifyingGroups.isEmpty()) {
            return AhAnomalySelection.empty();
        }

        List<AhSamplePoint> anomalySamples = new ArrayList<>();
        boolean fragmented = qualifyingGroups.size() > 1;
        double peakScore = Double.NEGATIVE_INFINITY;
        long peakSnapshotTs = qualifyingGroups.getFirst().samples().getFirst().sample().snapshotTs();
        String reasonCode = fragmented ? "MULTI_ANOMALY_CLUSTER" : qualifyingGroups.getFirst().reasonCode();
        for (AhAnomalyGroup group : qualifyingGroups) {
            for (AhAnomalyDecision decision : group.samples()) {
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
        anomalySamples.sort(Comparator.comparingLong(AhSamplePoint::snapshotTs));
        return new AhAnomalySelection(List.copyOf(anomalySamples), fragmented, reasonCode, peakSnapshotTs, peakScore);
    }

    private List<AhAnomalyGroup> mergeGroups(List<AhAnomalyDecision> decisions) {
        if (decisions.isEmpty()) {
            return List.of();
        }
        long maxGapMillis = Math.max(0L, properties.getAnomaly().getMergeGapSeconds()) * 1_000L;
        List<AhAnomalyGroup> groups = new ArrayList<>();
        List<AhAnomalyDecision> current = new ArrayList<>();
        current.add(decisions.getFirst());
        for (int i = 1; i < decisions.size(); i++) {
            AhAnomalyDecision previous = decisions.get(i - 1);
            AhAnomalyDecision decision = decisions.get(i);
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

    private AhAnomalyGroup toGroup(List<AhAnomalyDecision> decisions) {
        double peakScore = decisions.stream().mapToDouble(AhAnomalyDecision::score).max().orElse(0.0D);
        String reasonCode = decisions.stream()
                .max(Comparator.comparingDouble(AhAnomalyDecision::score))
                .map(AhAnomalyDecision::reasonCode)
                .orElse("ANOMALY");
        return new AhAnomalyGroup(List.copyOf(decisions), peakScore, reasonCode);
    }

    private boolean qualifies(AhAnomalyGroup group, int totalValidSamples) {
        long start = group.samples().getFirst().sample().snapshotTs();
        long end = group.samples().getLast().sample().snapshotTs();
        long durationMillis = Math.max(0L, end - start);
        int sampleCount = group.samples().size();
        return sampleCount >= Math.max(1, properties.getAnomaly().getConsecutiveSamplesThreshold())
                || durationMillis >= Math.max(0L, properties.getAnomaly().getAnomalyDurationThresholdSeconds()) * 1_000L
                || ((double) sampleCount / Math.max(1, totalValidSamples)) >= ANOMALY_SHARE_THRESHOLD;
    }

    private AhAnomalyDecision detectAnomaly(AhSamplePoint sample, AhMetricCenters centers) {
        List<AhAnomalySignal> signals = new ArrayList<>();

        double anchorScore = Math.abs(robustZ(sample.logPriceAnchor(), centers.medianLogPriceAnchor(), centers.madLogPriceAnchor()));
        double anchorRelativeChange = Math.abs(sample.priceAnchor() / centers.medianPriceAnchor() - 1.0D);
        if (anchorScore >= properties.getAnomaly().getZThreshold()
                && anchorRelativeChange >= properties.getAnomaly().getAhRelativePriceFloor()) {
            signals.add(new AhAnomalySignal("PRICE_SPIKE", anchorScore));
        }

        if (sample.binP50() != null && centers.medianBinP50() > EPSILON) {
            double binP50Score = Math.abs(robustZ(Math.log(sample.binP50()), centers.medianLogBinP50(), centers.madLogBinP50()));
            double relativeChange = Math.abs(sample.binP50() / centers.medianBinP50() - 1.0D);
            if (binP50Score >= properties.getAnomaly().getZThreshold()
                    && relativeChange >= properties.getAnomaly().getAhRelativePriceFloor()) {
                signals.add(new AhAnomalySignal("PRICE_SPIKE", binP50Score));
            }
        }

        if (centers.medianBinCount() > EPSILON) {
            double inventoryScore = -robustZ(sample.logBinCount(), centers.medianLogBinCount(), centers.madLogBinCount());
            if (inventoryScore >= properties.getAnomaly().getZThreshold()
                    && sample.binCount() <= centers.medianBinCount() * properties.getAnomaly().getAhBinCountDropThreshold()) {
                signals.add(new AhAnomalySignal("INVENTORY_COLLAPSE", inventoryScore));
            }
        }

        if (signals.isEmpty()) {
            return null;
        }
        AhAnomalySignal strongest = signals.stream()
                .max(Comparator.comparingDouble(AhAnomalySignal::score))
                .orElseGet(() -> new AhAnomalySignal("ANOMALY", 0.0D));
        String reasonCode = signals.size() > 1 ? "MULTI_SIGNAL" : strongest.reasonCode();
        return new AhAnomalyDecision(sample, reasonCode, strongest.score());
    }

    private AhItemBucketRollupEntity toRollup(MarketBucketGranularity granularity,
                                              long bucketStartEpochMillis,
                                              long bucketEndEpochMillis,
                                              String itemKey,
                                              int sampleCount,
                                              List<AhSamplePoint> validSamples,
                                              List<AhSamplePoint> rollupSamples,
                                              int anomalySampleCount,
                                              boolean partial) {
        long now = System.currentTimeMillis();
        AhItemBucketRollupEntity rollup = new AhItemBucketRollupEntity();
        rollup.setBucketStartEpochMillis(bucketStartEpochMillis);
        rollup.setBucketEndEpochMillis(bucketEndEpochMillis);
        rollup.setBucketGranularity(granularity.code());
        rollup.setItemKey(itemKey);
        rollup.setSampleCount(sampleCount);
        rollup.setValidSampleCount(validSamples.size());
        rollup.setAnomalySampleCount(anomalySampleCount);
        rollup.setPartial(partial);
        rollup.setRepresentativeSnapshotTs(chooseRepresentativeSnapshotTs(rollupSamples));
        rollup.setMedianBinLowest5Mean(RobustStatistics.median(values(rollupSamples, AhSamplePoint::binLowest5Mean)));
        rollup.setMedianBinP50(RobustStatistics.median(values(rollupSamples, AhSamplePoint::binP50)));
        rollup.setMedianBinP95(RobustStatistics.median(values(rollupSamples, AhSamplePoint::binP95)));
        rollup.setMedianBidP50(RobustStatistics.median(values(rollupSamples, AhSamplePoint::bidP50)));
        rollup.setMedianBinCount(RobustStatistics.median(values(rollupSamples, AhSamplePoint::binCount)));
        rollup.setMedianEndingSoonCount(RobustStatistics.median(values(rollupSamples, AhSamplePoint::endingSoonCount)));
        rollup.setP10BinP50(RobustStatistics.percentile(values(rollupSamples, AhSamplePoint::binP50), 0.10D));
        rollup.setP25BinP50(RobustStatistics.percentile(values(rollupSamples, AhSamplePoint::binP50), 0.25D));
        rollup.setP75BinP50(RobustStatistics.percentile(values(rollupSamples, AhSamplePoint::binP50), 0.75D));
        rollup.setP90BinP50(RobustStatistics.percentile(values(rollupSamples, AhSamplePoint::binP50), 0.90D));
        rollup.setMinBinP50(RobustStatistics.min(values(rollupSamples, AhSamplePoint::binP50)));
        rollup.setMaxBinP50(RobustStatistics.max(values(rollupSamples, AhSamplePoint::binP50)));
        rollup.setWinsorizedAvgBinP50(RobustStatistics.winsorizedMean(values(rollupSamples, AhSamplePoint::binP50), 0.10D, 0.90D));
        rollup.setFirstSnapshotTs(validSamples.getFirst().snapshotTs());
        rollup.setLastSnapshotTs(validSamples.getLast().snapshotTs());
        rollup.setCreatedAtEpochMillis(now);
        rollup.setUpdatedAtEpochMillis(now);
        return rollup;
    }

    private AhItemAnomalySegmentEntity toAnomalySegment(MarketBucketGranularity granularity,
                                                        long bucketStartEpochMillis,
                                                        long bucketEndEpochMillis,
                                                        String itemKey,
                                                        List<AhSamplePoint> anomalySamples,
                                                        boolean fragmented,
                                                        String reasonCode,
                                                        long peakSnapshotTs,
                                                        double anomalyScore) {
        long now = System.currentTimeMillis();
        AhItemAnomalySegmentEntity segment = new AhItemAnomalySegmentEntity();
        segment.setBucketStartEpochMillis(bucketStartEpochMillis);
        segment.setBucketEndEpochMillis(bucketEndEpochMillis);
        segment.setBucketGranularity(granularity.code());
        segment.setItemKey(itemKey);
        segment.setSegmentStartEpochMillis(anomalySamples.getFirst().snapshotTs());
        segment.setSegmentEndEpochMillis(anomalySamples.getLast().snapshotTs());
        segment.setRepresentativeSnapshotTs(chooseRepresentativeSnapshotTs(anomalySamples));
        segment.setPeakSnapshotTs(peakSnapshotTs);
        segment.setSampleCount(anomalySamples.size());
        segment.setAnomalyScore(anomalyScore);
        segment.setReasonCode(reasonCode);
        segment.setFragmented(fragmented);
        segment.setMedianBinLowest5Mean(RobustStatistics.median(values(anomalySamples, AhSamplePoint::binLowest5Mean)));
        segment.setMedianBinP50(RobustStatistics.median(values(anomalySamples, AhSamplePoint::binP50)));
        segment.setMedianBinP95(RobustStatistics.median(values(anomalySamples, AhSamplePoint::binP95)));
        segment.setMedianBidP50(RobustStatistics.median(values(anomalySamples, AhSamplePoint::bidP50)));
        segment.setMedianBinCount(RobustStatistics.median(values(anomalySamples, AhSamplePoint::binCount)));
        segment.setMedianEndingSoonCount(RobustStatistics.median(values(anomalySamples, AhSamplePoint::endingSoonCount)));
        segment.setCreatedAtEpochMillis(now);
        segment.setUpdatedAtEpochMillis(now);
        return segment;
    }

    private Long chooseRepresentativeSnapshotTs(List<AhSamplePoint> samples) {
        if (samples == null || samples.isEmpty()) {
            return null;
        }
        AhMetricCenters centers = AhMetricCenters.from(samples);
        AhSamplePoint representative = samples.stream()
                .min(Comparator.<AhSamplePoint>comparingDouble(sample -> distanceToCenter(sample, centers))
                        .thenComparingLong(AhSamplePoint::snapshotTs))
                .orElse(samples.getFirst());
        return representative.snapshotTs();
    }

    private double distanceToCenter(AhSamplePoint sample, AhMetricCenters centers) {
        double distance = 0.0D;
        distance += Math.abs(sample.logPriceAnchor() - centers.medianLogPriceAnchor()) / safeScale(centers.madLogPriceAnchor());
        distance += Math.abs(sample.logBinCount() - centers.medianLogBinCount()) / safeScale(centers.madLogBinCount());
        if (sample.binP50() != null && centers.medianBinP50() > EPSILON) {
            distance += Math.abs(Math.log(sample.binP50()) - centers.medianLogBinP50()) / safeScale(centers.madLogBinP50());
        }
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

    private Double positiveDouble(Number value) {
        if (value == null) {
            return null;
        }
        double asDouble = value.doubleValue();
        return asDouble > 0.0D ? asDouble : null;
    }

    private List<Double> values(List<AhSamplePoint> samples, Function<AhSamplePoint, Double> extractor) {
        List<Double> values = new ArrayList<>();
        if (samples == null || samples.isEmpty()) {
            return values;
        }
        for (AhSamplePoint sample : samples) {
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

    private record AhSamplePoint(long snapshotTs,
                                 double priceAnchor,
                                 Double binLowest5Mean,
                                 Double binP50,
                                 Double binP95,
                                 Double bidP50,
                                 Double binCount,
                                 Double endingSoonCount,
                                 double logPriceAnchor,
                                 double logBinCount) {
    }

    private record AhAnomalySignal(String reasonCode, double score) {
    }

    private record AhAnomalyDecision(AhSamplePoint sample, String reasonCode, double score) {
    }

    private record AhAnomalyGroup(List<AhAnomalyDecision> samples, double peakScore, String reasonCode) {
    }

    private record AhAnomalySelection(List<AhSamplePoint> samples,
                                      boolean fragmented,
                                      String reasonCode,
                                      long peakSnapshotTs,
                                      double anomalyScore) {
        private static AhAnomalySelection empty() {
            return new AhAnomalySelection(List.of(), false, null, 0L, 0.0D);
        }
    }

    private record AhMetricCenters(double medianPriceAnchor,
                                   double medianLogPriceAnchor,
                                   double madLogPriceAnchor,
                                   double medianBinP50,
                                   double medianLogBinP50,
                                   double madLogBinP50,
                                   double medianBinCount,
                                   double medianLogBinCount,
                                   double madLogBinCount) {

        private static AhMetricCenters from(List<AhSamplePoint> samples) {
            List<Double> priceAnchors = samples.stream().map(AhSamplePoint::priceAnchor).toList();
            List<Double> logPriceAnchors = samples.stream().map(AhSamplePoint::logPriceAnchor).toList();
            List<Double> binP50Values = values(samples, AhSamplePoint::binP50);
            List<Double> logBinP50Values = values(samples, sample -> sample.binP50() == null ? null : Math.log(sample.binP50()));
            List<Double> binCounts = samples.stream().map(AhSamplePoint::binCount).toList();
            List<Double> logBinCounts = samples.stream().map(AhSamplePoint::logBinCount).toList();
            return new AhMetricCenters(
                    defaultIfNull(RobustStatistics.median(priceAnchors), 0.0D),
                    defaultIfNull(RobustStatistics.median(logPriceAnchors), 0.0D),
                    defaultIfNull(RobustStatistics.mad(logPriceAnchors), 0.0D),
                    defaultIfNull(RobustStatistics.median(binP50Values), 0.0D),
                    defaultIfNull(RobustStatistics.median(logBinP50Values), 0.0D),
                    defaultIfNull(RobustStatistics.mad(logBinP50Values), 0.0D),
                    defaultIfNull(RobustStatistics.median(binCounts), 0.0D),
                    defaultIfNull(RobustStatistics.median(logBinCounts), 0.0D),
                    defaultIfNull(RobustStatistics.mad(logBinCounts), 0.0D)
            );
        }

        private static List<Double> values(List<AhSamplePoint> samples, Function<AhSamplePoint, Double> extractor) {
            List<Double> values = new ArrayList<>();
            for (AhSamplePoint sample : samples) {
                Double value = extractor.apply(sample);
                if (value != null) {
                    values.add(value);
                }
            }
            return values;
        }

        private static double defaultIfNull(Double value, double fallback) {
            return value == null ? fallback : value;
        }
    }
}
