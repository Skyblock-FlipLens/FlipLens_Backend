package com.skyblockflipper.backend.service.market.partitioning;

import com.skyblockflipper.backend.model.market.MarketSnapshotArchiveStateEntity;
import com.skyblockflipper.backend.model.market.MarketSnapshotEntity;
import com.skyblockflipper.backend.model.market.RetainedMarketSnapshotEntity;
import com.skyblockflipper.backend.repository.FlipRepository;
import com.skyblockflipper.backend.repository.MarketSnapshotArchiveStateRepository;
import com.skyblockflipper.backend.repository.MarketSnapshotRepository;
import com.skyblockflipper.backend.repository.RetainedMarketSnapshotRepository;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.domain.Pageable;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MarketSnapshotArchiveServiceTest {

    @Test
    void ensurePartitionArchivedPersistsRepresentativeSnapshotAndState() {
        MarketSnapshotRepository marketSnapshotRepository = mock(MarketSnapshotRepository.class);
        RetainedMarketSnapshotRepository retainedRepository = mock(RetainedMarketSnapshotRepository.class);
        MarketSnapshotArchiveStateRepository stateRepository = mock(MarketSnapshotArchiveStateRepository.class);
        FlipRepository flipRepository = mock(FlipRepository.class);
        PartitioningProperties properties = new PartitioningProperties();
        properties.setMarketSnapshotParentTable("market_snapshot");

        LocalDate day = LocalDate.parse("2026-03-08");
        long ts = Instant.parse("2026-03-08T00:00:05Z").toEpochMilli();
        MarketSnapshotEntity representative = new MarketSnapshotEntity(ts, 10, 20, "[]", "{}");

        when(stateRepository.findBySourcePartition("market_snapshot_2026_03_08")).thenReturn(null);
        when(marketSnapshotRepository.countBySnapshotTimestampEpochMillisGreaterThanEqualAndSnapshotTimestampEpochMillisLessThan(
                UtcDayBucket.startEpochMillis(day),
                UtcDayBucket.endEpochMillis(day)
        )).thenReturn(42L);
        when(marketSnapshotRepository.findTopBySnapshotTimestampEpochMillisGreaterThanEqualAndSnapshotTimestampEpochMillisLessThanOrderBySnapshotTimestampEpochMillisAsc(
                UtcDayBucket.startEpochMillis(day),
                UtcDayBucket.endEpochMillis(day)
        )).thenReturn(Optional.of(representative));
        when(retainedRepository.findBySnapshotTimestampEpochMillis(ts)).thenReturn(Optional.empty());

        MarketSnapshotArchiveService service = new MarketSnapshotArchiveService(
                marketSnapshotRepository,
                retainedRepository,
                stateRepository,
                flipRepository,
                properties
        );

        MarketSnapshotArchiveService.MarketSnapshotArchiveResult result =
                service.ensurePartitionArchived("market_snapshot", day, false);

        assertTrue(result.archived());
        assertEquals(42L, result.rawRowCount());
        assertEquals(ts, result.retainedSnapshotTimestampEpochMillis());

        ArgumentCaptor<RetainedMarketSnapshotEntity> retainedCaptor = ArgumentCaptor.forClass(RetainedMarketSnapshotEntity.class);
        verify(retainedRepository).save(retainedCaptor.capture());
        assertEquals(ts, retainedCaptor.getValue().getSnapshotTimestampEpochMillis());

        ArgumentCaptor<MarketSnapshotArchiveStateEntity> stateCaptor = ArgumentCaptor.forClass(MarketSnapshotArchiveStateEntity.class);
        verify(stateRepository).save(stateCaptor.capture());
        assertEquals("market_snapshot_2026_03_08", stateCaptor.getValue().getSourcePartition());
        assertTrue(stateCaptor.getValue().isFinalized());
        assertEquals(ts, stateCaptor.getValue().getRetainedSnapshotTimestampEpochMillis());
    }

    @Test
    void ensurePartitionArchivedDryRunDoesNotPersistRows() {
        MarketSnapshotRepository marketSnapshotRepository = mock(MarketSnapshotRepository.class);
        RetainedMarketSnapshotRepository retainedRepository = mock(RetainedMarketSnapshotRepository.class);
        MarketSnapshotArchiveStateRepository stateRepository = mock(MarketSnapshotArchiveStateRepository.class);
        FlipRepository flipRepository = mock(FlipRepository.class);
        PartitioningProperties properties = new PartitioningProperties();
        properties.setMarketSnapshotParentTable("market_snapshot");

        LocalDate day = LocalDate.parse("2026-03-08");
        when(stateRepository.findBySourcePartition("market_snapshot_2026_03_08")).thenReturn(null);
        when(marketSnapshotRepository.countBySnapshotTimestampEpochMillisGreaterThanEqualAndSnapshotTimestampEpochMillisLessThan(
                UtcDayBucket.startEpochMillis(day),
                UtcDayBucket.endEpochMillis(day)
        )).thenReturn(0L);
        when(marketSnapshotRepository.findTopBySnapshotTimestampEpochMillisGreaterThanEqualAndSnapshotTimestampEpochMillisLessThanOrderBySnapshotTimestampEpochMillisAsc(
                UtcDayBucket.startEpochMillis(day),
                UtcDayBucket.endEpochMillis(day)
        )).thenReturn(Optional.empty());

        MarketSnapshotArchiveService service = new MarketSnapshotArchiveService(
                marketSnapshotRepository,
                retainedRepository,
                stateRepository,
                flipRepository,
                properties
        );

        MarketSnapshotArchiveService.MarketSnapshotArchiveResult result =
                service.ensurePartitionArchived("market_snapshot", day, true);

        assertTrue(result.archived());
        verify(retainedRepository, never()).save(any());
        verify(stateRepository, never()).save(any());
    }

    @Test
    void cleanupDroppedPartitionOrphansDeletesBatchesUntilDone() {
        MarketSnapshotRepository marketSnapshotRepository = mock(MarketSnapshotRepository.class);
        RetainedMarketSnapshotRepository retainedRepository = mock(RetainedMarketSnapshotRepository.class);
        MarketSnapshotArchiveStateRepository stateRepository = mock(MarketSnapshotArchiveStateRepository.class);
        FlipRepository flipRepository = mock(FlipRepository.class);
        PartitioningProperties properties = new PartitioningProperties();
        properties.setMarketSnapshotParentTable("market_snapshot");

        UUID first = UUID.randomUUID();
        UUID second = UUID.randomUUID();
        LocalDate day = LocalDate.parse("2026-03-08");
        when(flipRepository.findOrphanFlipIdsBySnapshotTimestampEpochMillisGreaterThanEqualAndSnapshotTimestampEpochMillisLessThan(
                eq(UtcDayBucket.startEpochMillis(day)),
                eq(UtcDayBucket.endEpochMillis(day)),
                any(Pageable.class)
        )).thenReturn(List.of(first, second)).thenReturn(List.of());
        when(flipRepository.deleteStepRowsByFlipIdIn(List.of(first, second))).thenReturn(2);
        when(flipRepository.deleteConstraintRowsByFlipIdIn(List.of(first, second))).thenReturn(1);
        when(flipRepository.deleteByIdIn(List.of(first, second))).thenReturn(2);

        MarketSnapshotArchiveService service = new MarketSnapshotArchiveService(
                marketSnapshotRepository,
                retainedRepository,
                stateRepository,
                flipRepository,
                properties
        );

        MarketSnapshotArchiveService.PartitionOrphanCleanupResult result =
                service.cleanupDroppedPartitionOrphans("market_snapshot", day, 100);

        assertEquals(2, result.deletedFlips());
        assertEquals(2, result.deletedStepRows());
        assertEquals(1, result.deletedConstraintRows());
    }
}
