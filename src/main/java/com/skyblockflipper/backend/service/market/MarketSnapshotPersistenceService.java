package com.skyblockflipper.backend.service.market;

import com.skyblockflipper.backend.instrumentation.BlockingTimeTracker;
import com.skyblockflipper.backend.model.market.AuctionMarketRecord;
import com.skyblockflipper.backend.model.market.BazaarMarketRecord;
import com.skyblockflipper.backend.model.market.MarketSnapshot;
import com.skyblockflipper.backend.model.market.MarketSnapshotEntity;
import com.skyblockflipper.backend.repository.FlipRepository;
import com.skyblockflipper.backend.repository.MarketSnapshotCompactionCandidate;
import com.skyblockflipper.backend.repository.MarketSnapshotRepository;
import com.skyblockflipper.backend.service.market.partitioning.PartitionLifecycleService;
import com.skyblockflipper.backend.service.market.partitioning.PartitionRetentionReport;
import com.skyblockflipper.backend.service.market.partitioning.PartitioningProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
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
    private static final int DEFAULT_FLIP_DELETE_BATCH_SIZE = 1_000;

    private static final TypeReference<List<AuctionMarketRecord>> AUCTIONS_TYPE = new TypeReference<>() {};
    private static final TypeReference<Map<String, BazaarMarketRecord>> BAZAAR_TYPE = new TypeReference<>() {};

    private final MarketSnapshotRepository marketSnapshotRepository;
    private final FlipRepository flipRepository;
    private final ObjectMapper objectMapper;
    private final BlockingTimeTracker blockingTimeTracker;
    private final long rawWindowSeconds;
    private final long minuteTierUpperSeconds;
    private final long twoHourTierUpperSeconds;
    private final long minuteIntervalMillis;
    private final long twoHourIntervalMillis;
    private final int flipDeleteBatchSize;
    private final long flipDeleteBatchPauseMillis;
    private final TransactionTemplate requiresNewTransactionTemplate;
    private volatile PartitionLifecycleService partitionLifecycleService;
    private volatile PartitioningProperties partitioningProperties = new PartitioningProperties();

    public MarketSnapshotPersistenceService(MarketSnapshotRepository marketSnapshotRepository,
                                            FlipRepository flipRepository,
                                            ObjectMapper objectMapper,
                                            BlockingTimeTracker blockingTimeTracker,
                                            SnapshotRetentionProperties retentionProperties,
                                            PlatformTransactionManager transactionManager) {
        this.marketSnapshotRepository = marketSnapshotRepository;
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
            MarketSnapshotEntity saved = blockingTimeTracker.record("db.marketSnapshot.save", "db", () -> marketSnapshotRepository.save(entity));
            return toDomain(saved);
        } catch (JacksonException e) {
            throw new IllegalStateException("Failed to serialize market snapshot for persistence.", e);
        }
    }

    public Optional<MarketSnapshot> latest() {
        return blockingTimeTracker.record("db.marketSnapshot.latest", "db", () -> marketSnapshotRepository.findTopByOrderBySnapshotTimestampEpochMillisDesc().map(this::toDomain));
    }

    public Optional<MarketSnapshot> asOf(Instant asOfTimestamp) {
        if (asOfTimestamp == null) {
            return latest();
        }
        return blockingTimeTracker.record("db.marketSnapshot.asOf", "db", () -> marketSnapshotRepository
                .findTopBySnapshotTimestampEpochMillisLessThanEqualOrderBySnapshotTimestampEpochMillisDesc(asOfTimestamp.toEpochMilli())
                .map(this::toDomain));
    }

    public List<MarketSnapshot> between(Instant fromInclusive, Instant toInclusive) {
        if (fromInclusive == null || toInclusive == null || fromInclusive.isAfter(toInclusive)) {
            return List.of();
        }
        return blockingTimeTracker.record("db.marketSnapshot.between", "db", () -> marketSnapshotRepository
                .findBySnapshotTimestampEpochMillisBetweenOrderBySnapshotTimestampEpochMillisAsc(
                        fromInclusive.toEpochMilli(),
                        toInclusive.toEpochMilli()
                )
                .stream()
                .map(this::toDomain)
                .toList());
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

        List<MarketSnapshotCompactionCandidate> candidates = blockingTimeTracker.record("db.marketSnapshot.compactionCandidates", "db", () -> marketSnapshotRepository
                .findCompactionCandidates(compactionCandidateUpperBound));
        if (candidates.isEmpty()) {
            return new SnapshotCompactionResult(0, 0, 0);
        }

        Map<Long, UUID> minuteKeepers = new LinkedHashMap<>();
        Map<Long, UUID> twoHourKeepers = new LinkedHashMap<>();
        Map<Long, UUID> dailyKeepers = new LinkedHashMap<>();
        List<UUID> toDelete = new ArrayList<>();

        for (MarketSnapshotCompactionCandidate entity : candidates) {
            long snapshotMillis = entity.getSnapshotTimestampEpochMillis();
            long ageSeconds = Math.max(0L, (nowMillis - snapshotMillis) / 1_000L);

            if (ageSeconds <= minuteTierUpperSeconds) {
                long slot = Math.floorDiv(snapshotMillis, minuteIntervalMillis);
                if (minuteKeepers.putIfAbsent(slot, entity.getId()) != null) {
                    toDelete.add(entity.getId());
                }
                continue;
            }

            if (ageSeconds <= twoHourTierUpperSeconds) {
                long slot = Math.floorDiv(snapshotMillis, twoHourIntervalMillis);
                if (twoHourKeepers.putIfAbsent(slot, entity.getId()) != null) {
                    toDelete.add(entity.getId());
                }
                continue;
            }

            long epochDay = Math.floorDiv(snapshotMillis / 1_000L, SECONDS_PER_DAY);
            if (dailyKeepers.putIfAbsent(epochDay, entity.getId()) != null) {
                toDelete.add(entity.getId());
            }
        }

        if (!toDelete.isEmpty()) {
            int deletedFlips = deleteSnapshotsAndOrphansInBatches(candidates, toDelete);
            if (deletedFlips > 0) {
                log.info("Compacted flip rows after snapshot deletion: deleted={}", deletedFlips);
            }
        }

        int keptCount = candidates.size() - toDelete.size();
        return new SnapshotCompactionResult(candidates.size(), toDelete.size(), keptCount);
    }

    private int deleteSnapshotsAndOrphansInBatches(List<MarketSnapshotCompactionCandidate> candidates, List<UUID> deletedSnapshotIds) {
        if (deletedSnapshotIds.isEmpty()) {
            return 0;
        }

        Set<UUID> deletedSnapshotIdSet = new HashSet<>(deletedSnapshotIds);
        List<SnapshotDeleteCandidate> deleteCandidates = candidates.stream()
                .filter(candidate -> deletedSnapshotIdSet.contains(candidate.getId()))
                .map(candidate -> new SnapshotDeleteCandidate(candidate.getId(), candidate.getSnapshotTimestampEpochMillis()))
                .toList();

        int deletedFlips = 0;
        int deletedStepRows = 0;
        int deletedConstraintRows = 0;
        for (int fromIndex = 0; fromIndex < deleteCandidates.size(); fromIndex += flipDeleteBatchSize) {
            int toIndex = Math.min(fromIndex + flipDeleteBatchSize, deleteCandidates.size());
            int batchFromIndex = fromIndex;
            int batchToIndex = toIndex;
            List<SnapshotDeleteCandidate> deleteBatch = deleteCandidates.subList(fromIndex, toIndex);
            List<UUID> snapshotIdBatch = deleteBatch.stream().map(SnapshotDeleteCandidate::id).toList();
            List<Long> timestampBatch = deleteBatch.stream()
                    .map(SnapshotDeleteCandidate::timestampEpochMillis)
                    .distinct()
                    .toList();

            BatchDeleteStats batchStats = requiresNewTransactionTemplate.execute(status -> {
                blockingTimeTracker.recordRunnable("db.marketSnapshot.deleteBatch", "db", () -> marketSnapshotRepository.deleteAllByIdInBatch(snapshotIdBatch));
                return deleteOrphanedFlipsForSnapshotTimestampBatch(timestampBatch, batchFromIndex, batchToIndex);
            });
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

            deletedStepRows += blockingTimeTracker.record(
                    "db.flip.deleteStepsByFlipIdBatch",
                    "db",
                    () -> flipRepository.deleteStepRowsByFlipIdIn(orphanFlipIds)
            );
            deletedConstraintRows += blockingTimeTracker.record(
                    "db.flip.deleteConstraintsByFlipIdBatch",
                    "db",
                    () -> flipRepository.deleteConstraintRowsByFlipIdIn(orphanFlipIds)
            );
            int deletedFlipsInChunk = blockingTimeTracker.record(
                    "db.flip.deleteByFlipIdBatch",
                    "db",
                    () -> flipRepository.deleteByIdIn(orphanFlipIds)
            );
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

    private void pauseBetweenDeleteBatches() {
        try {
            Thread.sleep(flipDeleteBatchPauseMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted during compaction delete batch pause");
        }
    }

    private MarketSnapshot toDomain(MarketSnapshotEntity entity) {
        try {
            List<AuctionMarketRecord> auctions = objectMapper.readValue(entity.getAuctionsJson(), AUCTIONS_TYPE);
            Map<String, BazaarMarketRecord> bazaar = objectMapper.readValue(entity.getBazaarProductsJson(), BAZAAR_TYPE);
            return new MarketSnapshot(Instant.ofEpochMilli(entity.getSnapshotTimestampEpochMillis()), auctions, bazaar);
        } catch (JacksonException e) {
            throw new IllegalStateException("Failed to deserialize market snapshot from persistence.", e);
        }
    }

    public record SnapshotCompactionResult(
            int scannedCount,
            int deletedCount,
            int keptCount
    ) {
    }

    private record SnapshotDeleteCandidate(UUID id, long timestampEpochMillis) {
    }

    private record BatchDeleteStats(int deletedFlips, int deletedStepRows, int deletedConstraintRows) {
    }
}
