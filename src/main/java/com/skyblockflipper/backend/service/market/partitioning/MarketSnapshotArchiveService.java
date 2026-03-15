package com.skyblockflipper.backend.service.market.partitioning;

import com.skyblockflipper.backend.model.market.MarketSnapshotArchiveStateEntity;
import com.skyblockflipper.backend.model.market.MarketSnapshotEntity;
import com.skyblockflipper.backend.model.market.RetainedMarketSnapshotEntity;
import com.skyblockflipper.backend.repository.FlipRepository;
import com.skyblockflipper.backend.repository.MarketSnapshotArchiveStateRepository;
import com.skyblockflipper.backend.repository.MarketSnapshotRepository;
import com.skyblockflipper.backend.repository.RetainedMarketSnapshotRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

@Service
@Profile("compactor")
@Slf4j
public class MarketSnapshotArchiveService {

    private static final int DEFAULT_ORPHAN_DELETE_BATCH_SIZE = 1_000;

    private final MarketSnapshotRepository marketSnapshotRepository;
    private final RetainedMarketSnapshotRepository retainedMarketSnapshotRepository;
    private final MarketSnapshotArchiveStateRepository marketSnapshotArchiveStateRepository;
    private final FlipRepository flipRepository;
    private final PartitioningProperties partitioningProperties;

    public MarketSnapshotArchiveService(MarketSnapshotRepository marketSnapshotRepository,
                                        RetainedMarketSnapshotRepository retainedMarketSnapshotRepository,
                                        MarketSnapshotArchiveStateRepository marketSnapshotArchiveStateRepository,
                                        FlipRepository flipRepository,
                                        PartitioningProperties partitioningProperties) {
        this.marketSnapshotRepository = Objects.requireNonNull(marketSnapshotRepository, "marketSnapshotRepository must not be null");
        this.retainedMarketSnapshotRepository = Objects.requireNonNull(retainedMarketSnapshotRepository, "retainedMarketSnapshotRepository must not be null");
        this.marketSnapshotArchiveStateRepository = Objects.requireNonNull(marketSnapshotArchiveStateRepository, "marketSnapshotArchiveStateRepository must not be null");
        this.flipRepository = Objects.requireNonNull(flipRepository, "flipRepository must not be null");
        this.partitioningProperties = Objects.requireNonNull(partitioningProperties, "partitioningProperties must not be null");
    }

    public boolean isPartitionArchived(String parentTable, LocalDate partitionDayUtc) {
        if (!isSupportedParent(parentTable) || partitionDayUtc == null) {
            return false;
        }
        MarketSnapshotArchiveStateEntity state =
                marketSnapshotArchiveStateRepository.findBySourcePartition(partitionName(parentTable, partitionDayUtc));
        if (state == null || !state.isFinalized() || state.isFailed()) {
            return false;
        }
        Long retainedSnapshotTs = state.getRetainedSnapshotTimestampEpochMillis();
        return retainedSnapshotTs == null || retainedMarketSnapshotRepository.existsBySnapshotTimestampEpochMillis(retainedSnapshotTs);
    }

    @Transactional
    public MarketSnapshotArchiveResult ensurePartitionArchived(String parentTable, LocalDate partitionDayUtc, boolean dryRun) {
        if (!isSupportedParent(parentTable) || partitionDayUtc == null) {
            return MarketSnapshotArchiveResult.unsupported(parentTable, partitionDayUtc);
        }

        String sourcePartition = partitionName(parentTable, partitionDayUtc);
        MarketSnapshotArchiveStateEntity existingState = marketSnapshotArchiveStateRepository.findBySourcePartition(sourcePartition);
        if (existingState != null && existingState.isFinalized() && !existingState.isFailed()) {
            return MarketSnapshotArchiveResult.finalized(
                    sourcePartition,
                    existingState.getRawRowCount(),
                    existingState.getRetainedSnapshotTimestampEpochMillis()
            );
        }

        long dayStart = UtcDayBucket.startEpochMillis(partitionDayUtc);
        long dayEnd = UtcDayBucket.endEpochMillis(partitionDayUtc);
        long rawRowCount = marketSnapshotRepository
                .countBySnapshotTimestampEpochMillisGreaterThanEqualAndSnapshotTimestampEpochMillisLessThan(dayStart, dayEnd);
        MarketSnapshotEntity representative = marketSnapshotRepository
                .findTopBySnapshotTimestampEpochMillisGreaterThanEqualAndSnapshotTimestampEpochMillisLessThanOrderBySnapshotTimestampEpochMillisAsc(
                        dayStart,
                        dayEnd
                )
                .orElse(null);

        Long retainedSnapshotTs = representative == null ? null : representative.getSnapshotTimestampEpochMillis();
        if (dryRun) {
            return MarketSnapshotArchiveResult.finalized(sourcePartition, rawRowCount, retainedSnapshotTs);
        }

        if (representative != null) {
            upsertRetainedSnapshot(representative);
        }

        long nowMillis = System.currentTimeMillis();
        MarketSnapshotArchiveStateEntity state = existingState == null ? new MarketSnapshotArchiveStateEntity() : existingState;
        state.setSourcePartition(sourcePartition);
        state.setParentTable(parentTable);
        state.setPartitionDayUtc(partitionDayUtc);
        state.setRawRowCount(rawRowCount);
        state.setRetainedSnapshotId(representative == null ? null : representative.getId());
        state.setRetainedSnapshotTimestampEpochMillis(retainedSnapshotTs);
        state.setFinalized(true);
        state.setFailed(false);
        state.setFinalizedAtEpochMillis(nowMillis);
        state.setUpdatedAtEpochMillis(nowMillis);
        marketSnapshotArchiveStateRepository.save(state);

        return MarketSnapshotArchiveResult.finalized(sourcePartition, rawRowCount, retainedSnapshotTs);
    }

    @Transactional
    public PartitionOrphanCleanupResult cleanupDroppedPartitionOrphans(String parentTable, LocalDate partitionDayUtc) {
        return cleanupDroppedPartitionOrphans(parentTable, partitionDayUtc, DEFAULT_ORPHAN_DELETE_BATCH_SIZE);
    }

    @Transactional
    public PartitionOrphanCleanupResult cleanupDroppedPartitionOrphans(String parentTable,
                                                                       LocalDate partitionDayUtc,
                                                                       int batchSize) {
        if (!isSupportedParent(parentTable) || partitionDayUtc == null) {
            return PartitionOrphanCleanupResult.empty();
        }

        long dayStart = UtcDayBucket.startEpochMillis(partitionDayUtc);
        long dayEnd = UtcDayBucket.endEpochMillis(partitionDayUtc);
        int safeBatchSize = batchSize > 0 ? batchSize : DEFAULT_ORPHAN_DELETE_BATCH_SIZE;

        int deletedFlips = 0;
        int deletedStepRows = 0;
        int deletedConstraintRows = 0;
        while (true) {
            List<UUID> orphanFlipIds = flipRepository
                    .findOrphanFlipIdsBySnapshotTimestampEpochMillisGreaterThanEqualAndSnapshotTimestampEpochMillisLessThan(
                            dayStart,
                            dayEnd,
                            PageRequest.of(0, safeBatchSize)
                    );
            if (orphanFlipIds == null || orphanFlipIds.isEmpty()) {
                break;
            }

            deletedStepRows += flipRepository.deleteStepRowsByFlipIdIn(orphanFlipIds);
            deletedConstraintRows += flipRepository.deleteConstraintRowsByFlipIdIn(orphanFlipIds);
            int deletedChunk = flipRepository.deleteByIdIn(orphanFlipIds);
            deletedFlips += deletedChunk;
            if (deletedChunk == 0) {
                log.warn("Dropped-partition orphan cleanup made no progress for {} {}; stopping batch loop.",
                        parentTable,
                        partitionDayUtc);
                break;
            }
        }

        return new PartitionOrphanCleanupResult(deletedFlips, deletedStepRows, deletedConstraintRows);
    }

    private void upsertRetainedSnapshot(MarketSnapshotEntity sourceEntity) {
        RetainedMarketSnapshotEntity retainedEntity = retainedMarketSnapshotRepository
                .findBySnapshotTimestampEpochMillis(sourceEntity.getSnapshotTimestampEpochMillis())
                .orElse(null);
        long nowMillis = System.currentTimeMillis();
        if (retainedEntity == null) {
            retainedEntity = new RetainedMarketSnapshotEntity(
                    sourceEntity.getId(),
                    sourceEntity.getSnapshotTimestampEpochMillis(),
                    sourceEntity.getAuctionCount(),
                    sourceEntity.getBazaarProductCount(),
                    sourceEntity.getAuctionsJson(),
                    sourceEntity.getBazaarProductsJson(),
                    sourceEntity.getCreatedAtEpochMillis(),
                    nowMillis
            );
        } else {
            retainedEntity.setAuctionCount(sourceEntity.getAuctionCount());
            retainedEntity.setBazaarProductCount(sourceEntity.getBazaarProductCount());
            retainedEntity.setAuctionsJson(sourceEntity.getAuctionsJson());
            retainedEntity.setBazaarProductsJson(sourceEntity.getBazaarProductsJson());
            retainedEntity.setCreatedAtEpochMillis(sourceEntity.getCreatedAtEpochMillis());
            retainedEntity.setRetainedAtEpochMillis(nowMillis);
        }
        retainedMarketSnapshotRepository.save(retainedEntity);
    }

    private boolean isSupportedParent(String parentTable) {
        return parentTable != null
                && !parentTable.isBlank()
                && parentTable.equalsIgnoreCase(partitioningProperties.getMarketSnapshotParentTable());
    }

    private String partitionName(String parentTable, LocalDate day) {
        return parentTable + "_" + String.format("%04d_%02d_%02d", day.getYear(), day.getMonthValue(), day.getDayOfMonth());
    }

    public record MarketSnapshotArchiveResult(boolean archived,
                                              boolean unsupported,
                                              String sourcePartition,
                                              long rawRowCount,
                                              Long retainedSnapshotTimestampEpochMillis) {
        private static MarketSnapshotArchiveResult finalized(String sourcePartition,
                                                             long rawRowCount,
                                                             Long retainedSnapshotTimestampEpochMillis) {
            return new MarketSnapshotArchiveResult(true, false, sourcePartition, rawRowCount, retainedSnapshotTimestampEpochMillis);
        }

        private static MarketSnapshotArchiveResult unsupported(String parentTable, LocalDate partitionDayUtc) {
            String partition = parentTable == null || partitionDayUtc == null
                    ? null
                    : parentTable + "_" + String.format("%04d_%02d_%02d",
                    partitionDayUtc.getYear(),
                    partitionDayUtc.getMonthValue(),
                    partitionDayUtc.getDayOfMonth());
            return new MarketSnapshotArchiveResult(false, true, partition, 0L, null);
        }
    }

    public record PartitionOrphanCleanupResult(int deletedFlips,
                                               int deletedStepRows,
                                               int deletedConstraintRows) {
        private static PartitionOrphanCleanupResult empty() {
            return new PartitionOrphanCleanupResult(0, 0, 0);
        }
    }
}
