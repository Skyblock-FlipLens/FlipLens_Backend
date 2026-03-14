package com.skyblockflipper.backend.service.market.rollup;

import com.skyblockflipper.backend.model.market.AhItemAnomalySegmentEntity;
import com.skyblockflipper.backend.model.market.AhItemBucketRollupEntity;
import com.skyblockflipper.backend.model.market.AhItemSnapshotEntity;
import com.skyblockflipper.backend.model.market.BzItemAnomalySegmentEntity;
import com.skyblockflipper.backend.model.market.BzItemBucketRollupEntity;
import com.skyblockflipper.backend.model.market.BzItemSnapshotEntity;
import com.skyblockflipper.backend.model.market.ItemBucketMaterializationStateEntity;
import com.skyblockflipper.backend.repository.AhItemAnomalySegmentRepository;
import com.skyblockflipper.backend.repository.AhItemBucketRollupRepository;
import com.skyblockflipper.backend.repository.AhItemSnapshotRepository;
import com.skyblockflipper.backend.repository.BzItemAnomalySegmentRepository;
import com.skyblockflipper.backend.repository.BzItemBucketRollupRepository;
import com.skyblockflipper.backend.repository.BzItemSnapshotRepository;
import com.skyblockflipper.backend.repository.ItemBucketMaterializationStateRepository;
import com.skyblockflipper.backend.service.market.SnapshotRollupProperties;
import com.skyblockflipper.backend.service.market.partitioning.PartitioningProperties;
import com.skyblockflipper.backend.service.market.partitioning.UtcDayBucket;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Service
@Profile("compactor")
@Slf4j
public class MarketBucketMaterializationService {

    private final SnapshotRollupProperties rollupProperties;
    private final PartitioningProperties partitioningProperties;
    private final BzItemSnapshotRepository bzItemSnapshotRepository;
    private final AhItemSnapshotRepository ahItemSnapshotRepository;
    private final BzItemBucketRollupRepository bzItemBucketRollupRepository;
    private final BzItemAnomalySegmentRepository bzItemAnomalySegmentRepository;
    private final AhItemBucketRollupRepository ahItemBucketRollupRepository;
    private final AhItemAnomalySegmentRepository ahItemAnomalySegmentRepository;
    private final ItemBucketMaterializationStateRepository materializationStateRepository;
    private final BzItemBucketAnalyzer bzItemBucketAnalyzer;
    private final AhItemBucketAnalyzer ahItemBucketAnalyzer;
    private final TransactionTemplate requiresNewTransactionTemplate;

    public MarketBucketMaterializationService(SnapshotRollupProperties rollupProperties,
                                              PartitioningProperties partitioningProperties,
                                              BzItemSnapshotRepository bzItemSnapshotRepository,
                                              AhItemSnapshotRepository ahItemSnapshotRepository,
                                              BzItemBucketRollupRepository bzItemBucketRollupRepository,
                                              BzItemAnomalySegmentRepository bzItemAnomalySegmentRepository,
                                              AhItemBucketRollupRepository ahItemBucketRollupRepository,
                                              AhItemAnomalySegmentRepository ahItemAnomalySegmentRepository,
                                              ItemBucketMaterializationStateRepository materializationStateRepository,
                                              BzItemBucketAnalyzer bzItemBucketAnalyzer,
                                              AhItemBucketAnalyzer ahItemBucketAnalyzer,
                                              PlatformTransactionManager transactionManager) {
        this.rollupProperties = Objects.requireNonNull(rollupProperties, "rollupProperties must not be null");
        this.partitioningProperties = Objects.requireNonNull(partitioningProperties, "partitioningProperties must not be null");
        this.bzItemSnapshotRepository = Objects.requireNonNull(bzItemSnapshotRepository, "bzItemSnapshotRepository must not be null");
        this.ahItemSnapshotRepository = Objects.requireNonNull(ahItemSnapshotRepository, "ahItemSnapshotRepository must not be null");
        this.bzItemBucketRollupRepository = Objects.requireNonNull(bzItemBucketRollupRepository, "bzItemBucketRollupRepository must not be null");
        this.bzItemAnomalySegmentRepository = Objects.requireNonNull(bzItemAnomalySegmentRepository, "bzItemAnomalySegmentRepository must not be null");
        this.ahItemBucketRollupRepository = Objects.requireNonNull(ahItemBucketRollupRepository, "ahItemBucketRollupRepository must not be null");
        this.ahItemAnomalySegmentRepository = Objects.requireNonNull(ahItemAnomalySegmentRepository, "ahItemAnomalySegmentRepository must not be null");
        this.materializationStateRepository = Objects.requireNonNull(materializationStateRepository, "materializationStateRepository must not be null");
        this.bzItemBucketAnalyzer = Objects.requireNonNull(bzItemBucketAnalyzer, "bzItemBucketAnalyzer must not be null");
        this.ahItemBucketAnalyzer = Objects.requireNonNull(ahItemBucketAnalyzer, "ahItemBucketAnalyzer must not be null");
        this.requiresNewTransactionTemplate = new TransactionTemplate(Objects.requireNonNull(
                transactionManager,
                "transactionManager must not be null"
        ));
        this.requiresNewTransactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    public boolean isEnabled() {
        return rollupProperties.isEnabled();
    }

    public BucketMaterializationReport materializeDueBuckets(Instant now) {
        if (!rollupProperties.isEnabled()) {
            return BucketMaterializationReport.empty();
        }
        Instant safeNow = now == null ? Instant.now() : now;
        int processed = 0;
        int failed = 0;
        Map<String, Integer> processedByScope = new LinkedHashMap<>();

        for (MarketBucketGranularity granularity : MarketBucketGranularity.values()) {
            BucketProcessingReport bzReport = materializeBz(granularity, safeNow);
            processed += bzReport.processedBuckets();
            failed += bzReport.failedBuckets();
            processedByScope.put("BZ:" + granularity.code(), bzReport.processedBuckets());

            BucketProcessingReport ahReport = materializeAh(granularity, safeNow);
            processed += ahReport.processedBuckets();
            failed += ahReport.failedBuckets();
            processedByScope.put("AH:" + granularity.code(), ahReport.processedBuckets());
        }

        return new BucketMaterializationReport(processed, failed, Map.copyOf(processedByScope));
    }

    public boolean isAggregatePartitionMaterialized(String parentTable, LocalDate partitionDayUtc) {
        String marketType = marketTypeForParent(parentTable);
        if (marketType == null || partitionDayUtc == null) {
            return false;
        }
        String sourcePartition = partitionName(parentTable, partitionDayUtc);
        for (MarketBucketGranularity granularity : MarketBucketGranularity.values()) {
            long failed = materializationStateRepository.countBySourcePartitionAndMarketTypeAndBucketGranularityAndFailedTrue(
                    sourcePartition,
                    marketType,
                    granularity.code()
            );
            if (failed > 0) {
                return false;
            }
            long finalized = materializationStateRepository.countBySourcePartitionAndMarketTypeAndBucketGranularityAndFinalizedTrue(
                    sourcePartition,
                    marketType,
                    granularity.code()
            );
            if (finalized < expectedBucketsPerDay(granularity)) {
                return false;
            }
        }
        return hasDailyRollupCoverage(parentTable, partitionDayUtc, marketType);
    }

    private BucketProcessingReport materializeBz(MarketBucketGranularity granularity, Instant now) {
        Long minSnapshotTs = bzItemSnapshotRepository.findMinSnapshotTs();
        Long maxSnapshotTs = bzItemSnapshotRepository.findMaxSnapshotTs();
        return materializeMarket(
                "BZ",
                partitioningProperties.getBzSnapshotParentTable(),
                granularity,
                now,
                minSnapshotTs,
                maxSnapshotTs,
                (start, end) -> bzItemSnapshotRepository.findBySnapshotTsGreaterThanEqualAndSnapshotTsLessThanOrderBySnapshotTsAsc(start, end)
        );
    }

    private BucketProcessingReport materializeAh(MarketBucketGranularity granularity, Instant now) {
        Long minSnapshotTs = ahItemSnapshotRepository.findMinSnapshotTs();
        Long maxSnapshotTs = ahItemSnapshotRepository.findMaxSnapshotTs();
        return materializeMarket(
                "AH",
                partitioningProperties.getAhSnapshotParentTable(),
                granularity,
                now,
                minSnapshotTs,
                maxSnapshotTs,
                (start, end) -> ahItemSnapshotRepository.findBySnapshotTsGreaterThanEqualAndSnapshotTsLessThanOrderBySnapshotTsAsc(start, end)
        );
    }

    private <T> BucketProcessingReport materializeMarket(String marketType,
                                                         String parentTable,
                                                         MarketBucketGranularity granularity,
                                                         Instant now,
                                                         Long minSnapshotTs,
                                                         Long maxSnapshotTs,
                                                         BucketLoader<T> bucketLoader) {
        if (minSnapshotTs == null || maxSnapshotTs == null) {
            return BucketProcessingReport.empty();
        }

        long latestClosedBucketStart = latestClosedBucketStart(now.toEpochMilli(), granularity);
        long latestRawBucketStart = alignToBucketStart(maxSnapshotTs, granularity);
        long upperBoundBucketStart = Math.min(latestClosedBucketStart, latestRawBucketStart);
        if (upperBoundBucketStart < 0L) {
            return BucketProcessingReport.empty();
        }

        long nextBucketStart = resolveNextBucketStart(marketType, granularity, minSnapshotTs);
        if (nextBucketStart > upperBoundBucketStart) {
            return BucketProcessingReport.empty();
        }

        int maxBuckets = Math.max(1, rollupProperties.getMaxBucketsPerRun());
        int processed = 0;
        int failed = 0;
        for (int i = 0; i < maxBuckets && nextBucketStart <= upperBoundBucketStart; i++) {
            long bucketEnd = nextBucketStart + granularity.durationMillis();
            try {
                List<T> rows = bucketLoader.load(nextBucketStart, bucketEnd);
                long currentBucketStart = nextBucketStart;
                requiresNewTransactionTemplate.executeWithoutResult(status -> {
                    if ("BZ".equals(marketType)) {
                        materializeBzBucket(
                                granularity,
                                parentTable,
                                currentBucketStart,
                                bucketEnd,
                                castRows(rows, BzItemSnapshotEntity.class)
                        );
                    } else {
                        materializeAhBucket(
                                granularity,
                                parentTable,
                                currentBucketStart,
                                bucketEnd,
                                castRows(rows, AhItemSnapshotEntity.class)
                        );
                    }
                });
                processed++;
            } catch (Exception e) {
                failed++;
                saveFailedState(marketType, granularity, parentTable, nextBucketStart, bucketEnd);
                log.warn("Failed to materialize {} bucket {} {}: {}", marketType, granularity.code(), nextBucketStart, e.toString(), e);
            }
            nextBucketStart += granularity.durationMillis();
        }
        return new BucketProcessingReport(processed, failed);
    }

    @SuppressWarnings("unchecked")
    private <T> List<T> castRows(List<?> rows, Class<T> expectedType) {
        if (rows == null || rows.isEmpty()) {
            return List.of();
        }
        List<T> casted = new ArrayList<>(rows.size());
        for (Object row : rows) {
            casted.add((T) expectedType.cast(row));
        }
        return casted;
    }

    protected void materializeBzBucket(MarketBucketGranularity granularity,
                                       String parentTable,
                                       long bucketStart,
                                       long bucketEnd,
                                       List<BzItemSnapshotEntity> rows) {
        Map<String, List<BzItemSnapshotEntity>> byProduct = new LinkedHashMap<>();
        for (BzItemSnapshotEntity row : rows) {
            if (row == null || row.getProductId() == null || row.getProductId().isBlank()) {
                continue;
            }
            byProduct.computeIfAbsent(row.getProductId(), ignored -> new ArrayList<>()).add(row);
        }

        List<BzItemBucketRollupEntity> rollups = new ArrayList<>();
        List<BzItemAnomalySegmentEntity> anomalies = new ArrayList<>();
        for (Map.Entry<String, List<BzItemSnapshotEntity>> entry : byProduct.entrySet()) {
            BzItemBucketAnalysisResult analysis = bzItemBucketAnalyzer.analyze(granularity, bucketStart, bucketEnd, entry.getKey(), entry.getValue());
            analysis.rollup().ifPresent(rollups::add);
            anomalies.addAll(analysis.anomalySegments());
        }

        bzItemAnomalySegmentRepository.deleteByBucketStartEpochMillisAndBucketGranularity(bucketStart, granularity.code());
        bzItemBucketRollupRepository.deleteByBucketStartEpochMillisAndBucketGranularity(bucketStart, granularity.code());
        if (!rollups.isEmpty()) {
            bzItemBucketRollupRepository.saveAll(rollups);
        }
        if (!anomalies.isEmpty()) {
            bzItemAnomalySegmentRepository.saveAll(anomalies);
        }
        saveSuccessState("BZ", granularity, parentTable, bucketStart, bucketEnd, rows.size(), rollups.size(), anomalies.size());
    }

    protected void materializeAhBucket(MarketBucketGranularity granularity,
                                       String parentTable,
                                       long bucketStart,
                                       long bucketEnd,
                                       List<AhItemSnapshotEntity> rows) {
        Map<String, List<AhItemSnapshotEntity>> byItemKey = new LinkedHashMap<>();
        for (AhItemSnapshotEntity row : rows) {
            if (row == null || row.getItemKey() == null || row.getItemKey().isBlank()) {
                continue;
            }
            byItemKey.computeIfAbsent(row.getItemKey(), ignored -> new ArrayList<>()).add(row);
        }

        List<AhItemBucketRollupEntity> rollups = new ArrayList<>();
        List<AhItemAnomalySegmentEntity> anomalies = new ArrayList<>();
        for (Map.Entry<String, List<AhItemSnapshotEntity>> entry : byItemKey.entrySet()) {
            AhItemBucketAnalysisResult analysis = ahItemBucketAnalyzer.analyze(granularity, bucketStart, bucketEnd, entry.getKey(), entry.getValue());
            analysis.rollup().ifPresent(rollups::add);
            anomalies.addAll(analysis.anomalySegments());
        }

        ahItemAnomalySegmentRepository.deleteByBucketStartEpochMillisAndBucketGranularity(bucketStart, granularity.code());
        ahItemBucketRollupRepository.deleteByBucketStartEpochMillisAndBucketGranularity(bucketStart, granularity.code());
        if (!rollups.isEmpty()) {
            ahItemBucketRollupRepository.saveAll(rollups);
        }
        if (!anomalies.isEmpty()) {
            ahItemAnomalySegmentRepository.saveAll(anomalies);
        }
        saveSuccessState("AH", granularity, parentTable, bucketStart, bucketEnd, rows.size(), rollups.size(), anomalies.size());
    }

    private void saveSuccessState(String marketType,
                                  MarketBucketGranularity granularity,
                                  String parentTable,
                                  long bucketStart,
                                  long bucketEnd,
                                  long rawRowCount,
                                  long rollupRowCount,
                                  long anomalyRowCount) {
        long now = System.currentTimeMillis();
        ItemBucketMaterializationStateEntity state = new ItemBucketMaterializationStateEntity();
        state.setBucketStartEpochMillis(bucketStart);
        state.setBucketEndEpochMillis(bucketEnd);
        state.setBucketGranularity(granularity.code());
        state.setMarketType(marketType);
        state.setSourcePartition(partitionName(parentTable, UtcDayBucket.utcDay(Instant.ofEpochMilli(bucketStart))));
        state.setFinalized(true);
        state.setFailed(false);
        state.setRawRowCount(rawRowCount);
        state.setRollupRowCount(rollupRowCount);
        state.setAnomalyRowCount(anomalyRowCount);
        state.setFinalizedAtEpochMillis(now);
        state.setUpdatedAtEpochMillis(now);
        materializationStateRepository.save(state);
    }

    private void saveFailedState(String marketType,
                                 MarketBucketGranularity granularity,
                                 String parentTable,
                                 long bucketStart,
                                 long bucketEnd) {
        long now = System.currentTimeMillis();
        ItemBucketMaterializationStateEntity state = new ItemBucketMaterializationStateEntity();
        state.setBucketStartEpochMillis(bucketStart);
        state.setBucketEndEpochMillis(bucketEnd);
        state.setBucketGranularity(granularity.code());
        state.setMarketType(marketType);
        state.setSourcePartition(partitionName(parentTable, UtcDayBucket.utcDay(Instant.ofEpochMilli(bucketStart))));
        state.setFinalized(false);
        state.setFailed(true);
        state.setRawRowCount(0L);
        state.setRollupRowCount(0L);
        state.setAnomalyRowCount(0L);
        state.setFinalizedAtEpochMillis(null);
        state.setUpdatedAtEpochMillis(now);
        materializationStateRepository.save(state);
    }

    private long resolveNextBucketStart(String marketType, MarketBucketGranularity granularity, long minSnapshotTs) {
        ItemBucketMaterializationStateEntity latest = materializationStateRepository
                .findTopByMarketTypeAndBucketGranularityAndFinalizedTrueOrderByBucketStartEpochMillisDesc(
                        marketType,
                        granularity.code()
                );
        if (latest != null) {
            return latest.getBucketStartEpochMillis() + granularity.durationMillis();
        }
        return startOfUtcDay(minSnapshotTs);
    }

    private long latestClosedBucketStart(long nowEpochMillis, MarketBucketGranularity granularity) {
        long effectiveMillis = nowEpochMillis - Math.max(0L, rollupProperties.getWatermarkSeconds()) * 1_000L;
        if (effectiveMillis < granularity.durationMillis()) {
            return -1L;
        }
        return alignToBucketStart(effectiveMillis, granularity) - granularity.durationMillis();
    }

    private long alignToBucketStart(long epochMillis, MarketBucketGranularity granularity) {
        long duration = granularity.durationMillis();
        return Math.floorDiv(epochMillis, duration) * duration;
    }

    private long startOfUtcDay(long epochMillis) {
        return Instant.ofEpochMilli(epochMillis)
                .atZone(ZoneOffset.UTC)
                .toLocalDate()
                .atStartOfDay(ZoneOffset.UTC)
                .toInstant()
                .toEpochMilli();
    }

    private String marketTypeForParent(String parentTable) {
        if (parentTable == null || parentTable.isBlank()) {
            return null;
        }
        if (parentTable.equalsIgnoreCase(partitioningProperties.getBzSnapshotParentTable())) {
            return "BZ";
        }
        if (parentTable.equalsIgnoreCase(partitioningProperties.getAhSnapshotParentTable())) {
            return "AH";
        }
        return null;
    }

    private String partitionName(String parentTable, LocalDate day) {
        return parentTable + "_" + String.format("%04d_%02d_%02d", day.getYear(), day.getMonthValue(), day.getDayOfMonth());
    }

    private long expectedBucketsPerDay(MarketBucketGranularity granularity) {
        return 86_400_000L / granularity.durationMillis();
    }

    private boolean hasDailyRollupCoverage(String parentTable, LocalDate partitionDayUtc, String marketType) {
        long bucketStart = partitionDayUtc.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        ItemBucketMaterializationStateEntity dailyState =
                materializationStateRepository.findByBucketStartEpochMillisAndBucketGranularityAndMarketType(
                        bucketStart,
                        MarketBucketGranularity.ONE_DAY.code(),
                        marketType
                );
        if (dailyState == null || !dailyState.isFinalized() || dailyState.isFailed()) {
            return false;
        }

        long distinctItemCount = distinctItemCountForDay(parentTable, bucketStart);
        if (distinctItemCount <= 0L) {
            return dailyState.getRollupRowCount() <= 0L;
        }

        double ratio = dailyState.getRollupRowCount() / (double) distinctItemCount;
        double requiredRatio = Math.max(0.0D, Math.min(1.0D, rollupProperties.getMinDailyRollupDistinctCoverageRatio()));
        return ratio >= requiredRatio;
    }

    private long distinctItemCountForDay(String parentTable, long bucketStart) {
        long bucketEnd = bucketStart + MarketBucketGranularity.ONE_DAY.durationMillis();
        if (parentTable.equalsIgnoreCase(partitioningProperties.getBzSnapshotParentTable())) {
            return bzItemSnapshotRepository.countDistinctProductIdBySnapshotTsGreaterThanEqualAndSnapshotTsLessThan(bucketStart, bucketEnd);
        }
        if (parentTable.equalsIgnoreCase(partitioningProperties.getAhSnapshotParentTable())) {
            return ahItemSnapshotRepository.countDistinctItemKeyBySnapshotTsGreaterThanEqualAndSnapshotTsLessThan(bucketStart, bucketEnd);
        }
        return 0L;
    }

    private interface BucketLoader<T> {
        List<T> load(long fromInclusive, long toExclusive);
    }

    private record BucketProcessingReport(int processedBuckets, int failedBuckets) {
        private static BucketProcessingReport empty() {
            return new BucketProcessingReport(0, 0);
        }
    }

    public record BucketMaterializationReport(int processedBuckets,
                                              int failedBuckets,
                                              Map<String, Integer> processedByScope) {
        public static BucketMaterializationReport empty() {
            return new BucketMaterializationReport(0, 0, Map.of());
        }
    }
}
