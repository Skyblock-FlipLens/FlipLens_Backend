package com.skyblockflipper.backend.service.market;

import com.skyblockflipper.backend.instrumentation.BlockingTimeTracker;
import com.skyblockflipper.backend.model.market.AuctionMarketRecord;
import com.skyblockflipper.backend.model.market.BazaarMarketRecord;
import com.skyblockflipper.backend.model.market.MarketSnapshot;
import com.skyblockflipper.backend.model.market.MarketSnapshotEntity;
import com.skyblockflipper.backend.model.market.RetainedMarketSnapshotEntity;
import com.skyblockflipper.backend.repository.FlipRepository;
import com.skyblockflipper.backend.repository.MarketSnapshotCompactionCandidate;
import com.skyblockflipper.backend.repository.MarketSnapshotRepository;
import com.skyblockflipper.backend.repository.RetainedMarketSnapshotRepository;
import com.skyblockflipper.backend.service.market.partitioning.PartitionLifecycleService;
import com.skyblockflipper.backend.service.market.partitioning.PartitionRetentionReport;
import com.skyblockflipper.backend.service.market.partitioning.PartitioningProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@Service
@Slf4j
public class MarketSnapshotPersistenceService {

    private static final long SECONDS_PER_DAY = 86_400L;
    private static final int DEFAULT_COMPACTION_CANDIDATE_BATCH_SIZE = 500;
    private static final int DEFAULT_FLIP_DELETE_BATCH_SIZE = 100;

    private static final TypeReference<List<AuctionMarketRecord>> AUCTIONS_TYPE = new TypeReference<>() {};
    private static final TypeReference<Map<String, BazaarMarketRecord>> BAZAAR_TYPE = new TypeReference<>() {};

    private final MarketSnapshotRepository marketSnapshotRepository;
    private final RetainedMarketSnapshotRepository retainedMarketSnapshotRepository;
    private final FlipRepository flipRepository;
    private final ObjectMapper objectMapper;
    private final BlockingTimeTracker blockingTimeTracker;
    private final long rawWindowSeconds;
    private final long minuteTierUpperSeconds;
    private final long twoHourTierUpperSeconds;
    private final long minuteIntervalMillis;
    private final long twoHourIntervalMillis;
    private final int compactionCandidateBatchSize;
    private final int flipDeleteBatchSize;
    private final long flipDeleteBatchPauseMillis;
    private final TransactionTemplate requiresNewTransactionTemplate;
    private volatile PartitionLifecycleService partitionLifecycleService;
    private volatile PartitioningProperties partitioningProperties = new PartitioningProperties();

    public MarketSnapshotPersistenceService(MarketSnapshotRepository marketSnapshotRepository,
                                            RetainedMarketSnapshotRepository retainedMarketSnapshotRepository,
                                            FlipRepository flipRepository,
                                            ObjectMapper objectMapper,
                                            BlockingTimeTracker blockingTimeTracker,
                                            SnapshotRetentionProperties retentionProperties,
                                            PlatformTransactionManager transactionManager) {
        this.marketSnapshotRepository = marketSnapshotRepository;
        this.retainedMarketSnapshotRepository = retainedMarketSnapshotRepository;
        this.flipRepository = flipRepository;
        this.objectMapper = objectMapper;
        this.blockingTimeTracker = blockingTimeTracker;
        SnapshotRetentionProperties configuredRetention = Objects.requireNonNull(
                retentionProperties,
                "SnapshotRetentionProperties must be injected"
        );
        this.rawWindowSeconds = sanitizeSeconds(configuredRetention.getRawWindowSeconds(), 90L);
        this.minuteTierUpperSeconds = sanitizeSeconds(configuredRetention.getMinuteTierUpperSeconds(), 30L * 60L);
        this.twoHourTierUpperSeconds = sanitizeSeconds(configuredRetention.getTwoHourTierUpperSeconds(), 12L * 60L * 60L);
        long minuteIntervalSeconds = sanitizeSeconds(configuredRetention.getMinuteIntervalSeconds(), 60L);
        long twoHourIntervalSeconds = sanitizeSeconds(configuredRetention.getTwoHourIntervalSeconds(), 2L * 60L * 60L);
        this.minuteIntervalMillis = minuteIntervalSeconds * 1_000L;
        this.twoHourIntervalMillis = twoHourIntervalSeconds * 1_000L;
        this.compactionCandidateBatchSize = sanitizePositiveInt(
                configuredRetention.getCompactionCandidateBatchSize(),
                DEFAULT_COMPACTION_CANDIDATE_BATCH_SIZE
        );
        this.flipDeleteBatchSize = sanitizePositiveInt(configuredRetention.getFlipDeleteBatchSize(), DEFAULT_FLIP_DELETE_BATCH_SIZE);
        this.flipDeleteBatchPauseMillis = Math.max(0L, configuredRetention.getFlipDeleteBatchPauseMillis());
        this.requiresNewTransactionTemplate = new TransactionTemplate(transactionManager);
        this.requiresNewTransactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    private long sanitizeSeconds(long configured, long fallback) {
        if (configured <= 0L) {
            return fallback;
        }
        return configured;
    }

    private int sanitizePositiveInt(int configured, int fallback) {
        if (configured <= 0) {
            return fallback;
        }
        return configured;
    }

    @Autowired(required = false)
    public void setPartitionLifecycleService(PartitionLifecycleService partitionLifecycleService) {
        this.partitionLifecycleService = partitionLifecycleService;
    }

    @Autowired(required = false)
    public void setPartitioningProperties(PartitioningProperties partitioningProperties) {
        if (partitioningProperties != null) {
            this.partitioningProperties = partitioningProperties;
        }
    }

    public MarketSnapshot save(MarketSnapshot snapshot) {
        try {
            MarketSnapshotEntity entity = new MarketSnapshotEntity(
                    snapshot.snapshotTimestamp().toEpochMilli(),
                    snapshot.auctions().size(),
                    snapshot.bazaarProducts().size(),
                    objectMapper.writeValueAsString(snapshot.auctions()),
                    objectMapper.writeValueAsString(snapshot.bazaarProducts())
            );
            blockingTimeTracker.record("db.marketSnapshot.save", "db", () -> marketSnapshotRepository.save(entity));
            return snapshot;
        } catch (JacksonException e) {
            throw new IllegalStateException("Failed to serialize market snapshot for persistence.", e);
        }
    }

    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public Optional<MarketSnapshot> latest() {
        Optional<SnapshotPayload> payload = blockingTimeTracker.record(
                "db.marketSnapshot.latest",
                "db",
                this::loadLatestPayload
        );
        return payload.map(this::toDomain);
    }

    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public Optional<MarketSnapshot> asOf(Instant asOfTimestamp) {
        if (asOfTimestamp == null) {
            return latest();
        }
        Optional<SnapshotPayload> payload = blockingTimeTracker.record(
                "db.marketSnapshot.asOf",
                "db",
                () -> loadAsOfPayload(asOfTimestamp)
        );
        return payload.map(this::toDomain);
    }

    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public List<MarketSnapshot> between(Instant fromInclusive, Instant toInclusive) {
        if (fromInclusive == null || toInclusive == null || fromInclusive.isAfter(toInclusive)) {
            return List.of();
        }
        List<SnapshotPayload> payloads = blockingTimeTracker.record(
                "db.marketSnapshot.between",
                "db",
                () -> loadBetweenPayloads(fromInclusive, toInclusive)
        );
        return payloads.stream().map(this::toDomain).toList();
    }

    public SnapshotCompactionResult compactSnapshots() {
        return compactSnapshots(Instant.now());
    }

    public SnapshotCompactionResult compactSnapshots(Instant now) {
        Instant safeNow = now == null ? Instant.now() : now;
        SnapshotCompactionResult partitionResult = compactUsingPartitionLifecycleIfEnabled(safeNow);
        if (partitionResult != null) {
            return partitionResult;
        }
        return compactSnapshotsRowDelete(safeNow);
    }

    private SnapshotCompactionResult compactUsingPartitionLifecycleIfEnabled(Instant safeNow) {
        PartitionLifecycleService lifecycleService = this.partitionLifecycleService;
        PartitioningProperties properties = this.partitioningProperties;
        if (lifecycleService == null || properties == null || !lifecycleService.isPartitionCompactionEnabled()) {
            return null;
        }

        PartitionRetentionReport report = lifecycleService.executeRawSnapshotRetention(safeNow);
        if (report == null) {
            return null;
        }

        if (report.dryRun()) {
            log.info("Partition retention dry-run: scanned={} wouldDrop={} dropped={}",
                    report.scannedPartitions(),
                    report.wouldDropPartitions(),
                    report.droppedPartitions());
            return new SnapshotCompactionResult(
                    report.scannedPartitions(),
                    0,
                    Math.max(0, report.scannedPartitions())
            );
        }

        if (report.partitionedTargetsDetected()) {
            int kept = Math.max(0, report.scannedPartitions() - report.droppedPartitions());
            log.info("Partition retention applied: scanned={} dropped={} kept={}",
                    report.scannedPartitions(),
                    report.droppedPartitions(),
                    kept);
            return new SnapshotCompactionResult(
                    report.scannedPartitions(),
                    report.droppedPartitions(),
                    kept
            );
        }

        if (properties.isFallbackToRowDelete()) {
            log.info("Partition mode enabled but no partitioned targets detected. Falling back to row-delete compaction.");
            return null;
        }

        log.info("Partition mode enabled and fallback disabled. No row-delete compaction executed.");
        return new SnapshotCompactionResult(0, 0, 0);
    }

    private SnapshotCompactionResult compactSnapshotsRowDelete(Instant safeNow) {
        long nowMillis = safeNow.toEpochMilli();
        long compactionCandidateUpperBound = nowMillis - (rawWindowSeconds * 1_000L);

        CompactionScanResult scanResult = scanCompactionCandidates(compactionCandidateUpperBound, nowMillis);
        if (scanResult.candidates().isEmpty()) {
            return new SnapshotCompactionResult(0, 0, 0);
        }
        int deletedFlips = deleteSnapshotsAndOrphansInBatches(scanResult.candidates(), scanResult.retainedSnapshotIds(), nowMillis);
        if (deletedFlips > 0) {
            log.info("Compacted flip rows after snapshot deletion: deleted={}", deletedFlips);
        }

        int keptCount = scanResult.retainedSnapshotIds().size();
        return new SnapshotCompactionResult(
                scanResult.scannedCount(),
                Math.max(0, scanResult.scannedCount() - keptCount),
                keptCount
        );
    }


    private CompactionScanResult scanCompactionCandidates(long compactionCandidateUpperBound, long nowMillis) {
        Map<Long, UUID> minuteKeepers = new LinkedHashMap<>();
        Map<Long, UUID> twoHourKeepers = new LinkedHashMap<>();
        Map<Long, UUID> dailyKeepers = new LinkedHashMap<>();
        List<MarketSnapshotCompactionCandidate> allCandidates = new ArrayList<>();

        int offset = 0;
        while (true) {
            int batchOffset = offset;
            List<MarketSnapshotCompactionCandidate> batch = blockingTimeTracker.record(
                    "db.marketSnapshot.compactionCandidates",
                    "db",
                    () -> marketSnapshotRepository.findCompactionCandidates(
                            compactionCandidateUpperBound,
                            PageRequest.of(
                                    batchOffset / compactionCandidateBatchSize,
                                    compactionCandidateBatchSize,
                                    Sort.unsorted()
                            )
                    )
            );
            if (batch.isEmpty()) {
                break;
            }

            allCandidates.addAll(batch);
            for (MarketSnapshotCompactionCandidate entity : batch) {
                long snapshotMillis = entity.getSnapshotTimestampEpochMillis();
                long ageSeconds = Math.max(0L, (nowMillis - snapshotMillis) / 1_000L);

                if (ageSeconds <= minuteTierUpperSeconds) {
                    long slot = Math.floorDiv(snapshotMillis, minuteIntervalMillis);
                    minuteKeepers.putIfAbsent(slot, entity.getId());
                    continue;
                }

                if (ageSeconds <= twoHourTierUpperSeconds) {
                    long slot = Math.floorDiv(snapshotMillis, twoHourIntervalMillis);
                    twoHourKeepers.putIfAbsent(slot, entity.getId());
                    continue;
                }

                long epochDay = Math.floorDiv(snapshotMillis / 1_000L, SECONDS_PER_DAY);
                dailyKeepers.putIfAbsent(epochDay, entity.getId());
            }

            if (batch.size() < compactionCandidateBatchSize) {
                break;
            }
            offset += batch.size();
        }

        Set<UUID> retainedSnapshotIds = new HashSet<>();
        retainedSnapshotIds.addAll(minuteKeepers.values());
        retainedSnapshotIds.addAll(twoHourKeepers.values());
        retainedSnapshotIds.addAll(dailyKeepers.values());
        return new CompactionScanResult(allCandidates, retainedSnapshotIds, allCandidates.size());
    }

    private int deleteSnapshotsAndOrphansInBatches(List<MarketSnapshotCompactionCandidate> candidates,
                                                   Set<UUID> retainedSnapshotIds,
                                                   long retainedAtEpochMillis) {
        if (candidates.isEmpty()) {
            return 0;
        }

        Set<UUID> retainedSnapshotIdSet = retainedSnapshotIds == null ? Set.of() : Set.copyOf(retainedSnapshotIds);
        List<SnapshotDeleteCandidate> deleteCandidates = candidates.stream()
                .map(candidate -> new SnapshotDeleteCandidate(candidate.getId(), candidate.getSnapshotTimestampEpochMillis()))
                .toList();

        int deletedFlips = 0;
        int deletedStepRows = 0;
        int deletedConstraintRows = 0;
        for (int fromIndex = 0; fromIndex < deleteCandidates.size(); fromIndex += flipDeleteBatchSize) {
            int toIndex = Math.min(fromIndex + flipDeleteBatchSize, deleteCandidates.size());
            int batchFromIndex = fromIndex;
            List<SnapshotDeleteCandidate> deleteBatch = deleteCandidates.subList(fromIndex, toIndex);
            List<UUID> snapshotIdBatch = deleteBatch.stream().map(SnapshotDeleteCandidate::id).toList();
            List<UUID> retainedSnapshotIdBatch = snapshotIdBatch.stream()
                    .filter(retainedSnapshotIdSet::contains)
                    .toList();
            List<Long> timestampBatch = deleteBatch.stream()
                    .map(SnapshotDeleteCandidate::timestampEpochMillis)
                    .distinct()
                    .toList();

            requiresNewTransactionTemplate.executeWithoutResult(status -> {
                retainSnapshotHistory(retainedSnapshotIdBatch, retainedAtEpochMillis);
                blockingTimeTracker.recordRunnable("db.marketSnapshot.deleteBatch", "db", () -> marketSnapshotRepository.deleteAllByIdInBatch(snapshotIdBatch));
            });
            BatchDeleteStats batchStats = deleteOrphanedFlipsForSnapshotTimestampBatch(timestampBatch, batchFromIndex, toIndex);
            if (batchStats != null) {
                deletedFlips += batchStats.deletedFlips();
                deletedStepRows += batchStats.deletedStepRows();
                deletedConstraintRows += batchStats.deletedConstraintRows();
            }

            if (flipDeleteBatchPauseMillis > 0L && toIndex < deleteCandidates.size()) {
                pauseBetweenDeleteBatches();
            }
        }
        if (deletedStepRows > 0 || deletedConstraintRows > 0) {
            log.info("Compaction orphan child cleanup: deletedStepRows={} deletedConstraintRows={}",
                    deletedStepRows,
                    deletedConstraintRows);
        }
        return deletedFlips;
    }

    private BatchDeleteStats deleteOrphanedFlipsForSnapshotTimestampBatch(List<Long> timestampBatch,
                                                                           int fromIndex,
                                                                           int toIndex) {
        int deletedFlips = 0;
        int deletedStepRows = 0;
        int deletedConstraintRows = 0;
        while (true) {
            List<UUID> orphanFlipIds = blockingTimeTracker.record(
                    "db.flip.findOrphanIdsBySnapshotBatch",
                    "db",
                    () -> flipRepository.findOrphanFlipIdsBySnapshotTimestampEpochMillisIn(
                            timestampBatch,
                            PageRequest.of(0, flipDeleteBatchSize)
                    )
            );
            if (orphanFlipIds.isEmpty()) {
                break;
            }

            BatchDeleteStats chunkStats = requiresNewTransactionTemplate.execute(status -> deleteOrphanedFlipChunk(orphanFlipIds));
            if (chunkStats == null) {
                log.warn("Compaction orphan cleanup returned no chunk stats for timestamp batch {}-{}; stopping this batch.",
                        fromIndex,
                        toIndex);
                break;
            }
            deletedStepRows += chunkStats.deletedStepRows();
            deletedConstraintRows += chunkStats.deletedConstraintRows();
            int deletedFlipsInChunk = chunkStats.deletedFlips();
            deletedFlips += deletedFlipsInChunk;

            if (deletedFlipsInChunk == 0) {
                log.warn("Compaction orphan cleanup made no progress for timestamp batch {}-{}; stopping this batch to avoid tight loop.",
                        fromIndex,
                        toIndex);
                break;
            }
        }
        return new BatchDeleteStats(deletedFlips, deletedStepRows, deletedConstraintRows);
    }

    private BatchDeleteStats deleteOrphanedFlipChunk(List<UUID> orphanFlipIds) {
        int deletedStepRows = blockingTimeTracker.record(
                "db.flip.deleteStepsByFlipIdBatch",
                "db",
                () -> flipRepository.deleteStepRowsByFlipIdIn(orphanFlipIds)
        );
        int deletedConstraintRows = blockingTimeTracker.record(
                "db.flip.deleteConstraintsByFlipIdBatch",
                "db",
                () -> flipRepository.deleteConstraintRowsByFlipIdIn(orphanFlipIds)
        );
        int deletedFlips = blockingTimeTracker.record(
                "db.flip.deleteByFlipIdBatch",
                "db",
                () -> flipRepository.deleteByIdIn(orphanFlipIds)
        );
        return new BatchDeleteStats(deletedFlips, deletedStepRows, deletedConstraintRows);
    }

    private void pauseBetweenDeleteBatches() {
        try {
            Thread.sleep(flipDeleteBatchPauseMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted during compaction delete batch pause");
        }
    }

    private void retainSnapshotHistory(List<UUID> retainedSnapshotIds, long retainedAtEpochMillis) {
        if (retainedSnapshotIds == null || retainedSnapshotIds.isEmpty()) {
            return;
        }

        List<MarketSnapshotEntity> sourceEntities = blockingTimeTracker.record(
                "db.marketSnapshot.retained.load",
                "db",
                () -> marketSnapshotRepository.findAllById(retainedSnapshotIds)
        );
        if (sourceEntities == null) {
            sourceEntities = List.of();
        }
        if (sourceEntities.isEmpty()) {
            return;
        }

        Map<Long, MarketSnapshotEntity> sourceByTimestamp = new LinkedHashMap<>();
        for (MarketSnapshotEntity sourceEntity : sourceEntities) {
            sourceByTimestamp.putIfAbsent(sourceEntity.getSnapshotTimestampEpochMillis(), sourceEntity);
        }

        List<Long> timestamps = new ArrayList<>(sourceByTimestamp.keySet());
        Map<Long, RetainedMarketSnapshotEntity> existingByTimestamp = new LinkedHashMap<>();
        List<RetainedMarketSnapshotEntity> existingRetainedSnapshots =
                retainedMarketSnapshotRepository.findBySnapshotTimestampEpochMillisIn(timestamps);
        if (existingRetainedSnapshots == null) {
            existingRetainedSnapshots = List.of();
        }
        existingRetainedSnapshots
                .forEach(entity -> existingByTimestamp.put(entity.getSnapshotTimestampEpochMillis(), entity));

        List<RetainedMarketSnapshotEntity> retainedEntities = new ArrayList<>(sourceByTimestamp.size());
        for (MarketSnapshotEntity sourceEntity : sourceByTimestamp.values()) {
            RetainedMarketSnapshotEntity retainedEntity = existingByTimestamp.get(sourceEntity.getSnapshotTimestampEpochMillis());
            if (retainedEntity == null) {
                retainedEntity = new RetainedMarketSnapshotEntity(
                        sourceEntity.getId(),
                        sourceEntity.getSnapshotTimestampEpochMillis(),
                        sourceEntity.getAuctionCount(),
                        sourceEntity.getBazaarProductCount(),
                        sourceEntity.getAuctionsJson(),
                        sourceEntity.getBazaarProductsJson(),
                        sourceEntity.getCreatedAtEpochMillis(),
                        retainedAtEpochMillis
                );
            } else {
                retainedEntity.setAuctionCount(sourceEntity.getAuctionCount());
                retainedEntity.setBazaarProductCount(sourceEntity.getBazaarProductCount());
                retainedEntity.setAuctionsJson(sourceEntity.getAuctionsJson());
                retainedEntity.setBazaarProductsJson(sourceEntity.getBazaarProductsJson());
                retainedEntity.setCreatedAtEpochMillis(sourceEntity.getCreatedAtEpochMillis());
                retainedEntity.setRetainedAtEpochMillis(retainedAtEpochMillis);
            }
            retainedEntities.add(retainedEntity);
        }

        blockingTimeTracker.recordRunnable(
                "db.marketSnapshot.retained.saveAll",
                "db",
                () -> retainedMarketSnapshotRepository.saveAll(retainedEntities)
        );
    }

    private Optional<SnapshotPayload> loadLatestPayload() {
        return newerSnapshot(
                marketSnapshotRepository.findTopByOrderBySnapshotTimestampEpochMillisDesc().map(this::toPayload),
                retainedMarketSnapshotRepository.findTopByOrderBySnapshotTimestampEpochMillisDesc().map(this::toPayload)
        );
    }

    private Optional<SnapshotPayload> loadAsOfPayload(Instant asOfTimestamp) {
        long asOfEpochMillis = asOfTimestamp.toEpochMilli();
        return newerSnapshot(
                marketSnapshotRepository
                        .findTopBySnapshotTimestampEpochMillisLessThanEqualOrderBySnapshotTimestampEpochMillisDesc(asOfEpochMillis)
                        .map(this::toPayload),
                retainedMarketSnapshotRepository
                        .findTopBySnapshotTimestampEpochMillisLessThanEqualOrderBySnapshotTimestampEpochMillisDesc(asOfEpochMillis)
                        .map(this::toPayload)
        );
    }

    private List<SnapshotPayload> loadBetweenPayloads(Instant fromInclusive, Instant toInclusive) {
        Map<Long, SnapshotPayload> snapshotsByTimestamp = new LinkedHashMap<>();
        List<RetainedMarketSnapshotEntity> retainedSnapshots = retainedMarketSnapshotRepository
                .findBySnapshotTimestampEpochMillisBetweenOrderBySnapshotTimestampEpochMillisAsc(
                        fromInclusive.toEpochMilli(),
                        toInclusive.toEpochMilli()
                );
        if (retainedSnapshots == null) {
            retainedSnapshots = List.of();
        }
        retainedSnapshots.forEach(entity -> snapshotsByTimestamp.put(entity.getSnapshotTimestampEpochMillis(), toPayload(entity)));

        List<MarketSnapshotEntity> rawSnapshots = marketSnapshotRepository
                .findBySnapshotTimestampEpochMillisBetweenOrderBySnapshotTimestampEpochMillisAsc(
                        fromInclusive.toEpochMilli(),
                        toInclusive.toEpochMilli()
                );
        if (rawSnapshots == null) {
            rawSnapshots = List.of();
        }
        rawSnapshots.forEach(entity -> snapshotsByTimestamp.put(entity.getSnapshotTimestampEpochMillis(), toPayload(entity)));

        return snapshotsByTimestamp.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(Map.Entry::getValue)
                .toList();
    }

    private MarketSnapshot toDomain(SnapshotPayload payload) {
        try {
            List<AuctionMarketRecord> auctions = objectMapper.readValue(payload.auctionsJson(), AUCTIONS_TYPE);
            Map<String, BazaarMarketRecord> bazaar = objectMapper.readValue(payload.bazaarProductsJson(), BAZAAR_TYPE);
            return new MarketSnapshot(Instant.ofEpochMilli(payload.snapshotTimestampEpochMillis()), auctions, bazaar);
        } catch (JacksonException e) {
            throw new IllegalStateException("Failed to deserialize market snapshot from persistence.", e);
        }
    }

    private SnapshotPayload toPayload(MarketSnapshotEntity entity) {
        return new SnapshotPayload(
                entity.getSnapshotTimestampEpochMillis(),
                entity.getAuctionsJson(),
                entity.getBazaarProductsJson()
        );
    }

    private SnapshotPayload toPayload(RetainedMarketSnapshotEntity entity) {
        return new SnapshotPayload(
                entity.getSnapshotTimestampEpochMillis(),
                entity.getAuctionsJson(),
                entity.getBazaarProductsJson()
        );
    }

    private Optional<SnapshotPayload> newerSnapshot(Optional<SnapshotPayload> rawSnapshot,
                                                    Optional<SnapshotPayload> retainedSnapshot) {
        if (rawSnapshot.isEmpty()) {
            return retainedSnapshot;
        }
        if (retainedSnapshot.isEmpty()) {
            return rawSnapshot;
        }
        return rawSnapshot.get().snapshotTimestampEpochMillis() >= retainedSnapshot.get().snapshotTimestampEpochMillis()
                ? rawSnapshot
                : retainedSnapshot;
    }

    public record SnapshotCompactionResult(
            int scannedCount,
            int deletedCount,
            int keptCount
    ) {
    }

    private record SnapshotDeleteCandidate(UUID id, long timestampEpochMillis) {
    }

    private record CompactionScanResult(
            List<MarketSnapshotCompactionCandidate> candidates,
            Set<UUID> retainedSnapshotIds,
            int scannedCount
    ) {
    }

    private record BatchDeleteStats(int deletedFlips, int deletedStepRows, int deletedConstraintRows) {
    }

    private record SnapshotPayload(long snapshotTimestampEpochMillis,
                                   String auctionsJson,
                                   String bazaarProductsJson) {
    }
}
