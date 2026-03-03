package com.skyblockflipper.backend.service.market;

import com.skyblockflipper.backend.instrumentation.BlockingTimeTracker;
import com.skyblockflipper.backend.model.market.MarketSnapshotEntity;
import com.skyblockflipper.backend.repository.FlipRepository;
import com.skyblockflipper.backend.repository.MarketSnapshotCompactionCandidate;
import com.skyblockflipper.backend.repository.MarketSnapshotRepository;
import com.skyblockflipper.backend.service.market.partitioning.PartitionLifecycleService;
import com.skyblockflipper.backend.service.market.partitioning.PartitionRetentionReport;
import com.skyblockflipper.backend.service.market.partitioning.PartitioningMode;
import com.skyblockflipper.backend.service.market.partitioning.PartitioningProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.SimpleTransactionStatus;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class MarketSnapshotPersistenceServiceUnitTest {

    private MarketSnapshotRepository marketSnapshotRepository;
    private FlipRepository flipRepository;
    private BlockingTimeTracker blockingTimeTracker;
    private PlatformTransactionManager transactionManager;

    @BeforeEach
    void setUp() {
        marketSnapshotRepository = mock(MarketSnapshotRepository.class);
        flipRepository = mock(FlipRepository.class);
        blockingTimeTracker = mock(BlockingTimeTracker.class);
        transactionManager = mock(PlatformTransactionManager.class);

        when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
        doNothing().when(transactionManager).commit(any(TransactionStatus.class));
        doNothing().when(transactionManager).rollback(any(TransactionStatus.class));

        doAnswer(invocation -> {
            BlockingTimeTracker.CheckedSupplier<Object> supplier = invocation.getArgument(2);
            return supplier.get();
        }).when(blockingTimeTracker).record(anyString(), anyString(), any());

        doAnswer(invocation -> {
            BlockingTimeTracker.CheckedRunnable runnable = invocation.getArgument(2);
            runnable.run();
            return null;
        }).when(blockingTimeTracker).recordRunnable(anyString(), anyString(), any());
    }

    @Test
    void betweenReturnsEmptyForInvalidRangeWithoutRepositoryCall() {
        MarketSnapshotPersistenceService service = createService(defaultRetention());

        assertTrue(service.between(Instant.parse("2026-03-01T12:00:00Z"), Instant.parse("2026-03-01T11:59:59Z")).isEmpty());

        verifyNoInteractions(marketSnapshotRepository);
    }

    @Test
    void asOfNullDelegatesToLatestLookup() {
        MarketSnapshotEntity entity = new MarketSnapshotEntity(
                Instant.parse("2026-03-01T12:00:00Z").toEpochMilli(),
                0,
                0,
                "[]",
                "{}"
        );
        when(marketSnapshotRepository.findTopByOrderBySnapshotTimestampEpochMillisDesc()).thenReturn(Optional.of(entity));

        MarketSnapshotPersistenceService service = createService(defaultRetention());

        service.asOf(null).orElseThrow();

        verify(marketSnapshotRepository, times(1)).findTopByOrderBySnapshotTimestampEpochMillisDesc();
    }

    @Test
    void compactSnapshotsReturnsZerosWhenNoCandidatesFound() {
        when(marketSnapshotRepository.findCompactionCandidates(anyLong())).thenReturn(List.of());
        MarketSnapshotPersistenceService service = createService(defaultRetention());

        MarketSnapshotPersistenceService.SnapshotCompactionResult result =
                service.compactSnapshots(Instant.parse("2026-03-01T12:00:00Z"));

        assertEquals(0, result.scannedCount());
        assertEquals(0, result.deletedCount());
        assertEquals(0, result.keptCount());
        verify(flipRepository, times(0)).findOrphanFlipIdsBySnapshotTimestampEpochMillisIn(anyCollection(), any());
    }

    @Test
    void compactSnapshotsUsesPartitionDryRunWithoutRowDeleteFallback() {
        MarketSnapshotPersistenceService service = createService(defaultRetention());
        PartitionLifecycleService lifecycleService = mock(PartitionLifecycleService.class);
        PartitioningProperties partitioningProperties = new PartitioningProperties();
        partitioningProperties.setEnabled(true);
        partitioningProperties.setMode(PartitioningMode.PARTITION_DROP);
        partitioningProperties.setDryRun(true);
        partitioningProperties.setFallbackToRowDelete(true);

        when(lifecycleService.isPartitionCompactionEnabled()).thenReturn(true);
        when(lifecycleService.executeRawSnapshotRetention(any())).thenReturn(new PartitionRetentionReport(
                11,
                0,
                4,
                true,
                true,
                Map.of(),
                Map.of("market_snapshot", 4),
                List.of()
        ));

        service.setPartitionLifecycleService(lifecycleService);
        service.setPartitioningProperties(partitioningProperties);
        MarketSnapshotPersistenceService.SnapshotCompactionResult result =
                service.compactSnapshots(Instant.parse("2026-03-01T12:00:00Z"));

        assertEquals(11, result.scannedCount());
        assertEquals(0, result.deletedCount());
        assertEquals(11, result.keptCount());
        verifyNoInteractions(marketSnapshotRepository);
    }

    @Test
    void compactSnapshotsFallsBackToRowDeleteWhenNoPartitionedTargetsDetected() {
        MarketSnapshotPersistenceService service = createService(defaultRetention());
        PartitionLifecycleService lifecycleService = mock(PartitionLifecycleService.class);
        PartitioningProperties partitioningProperties = new PartitioningProperties();
        partitioningProperties.setEnabled(true);
        partitioningProperties.setMode(PartitioningMode.PARTITION_DROP);
        partitioningProperties.setDryRun(false);
        partitioningProperties.setFallbackToRowDelete(true);

        when(lifecycleService.isPartitionCompactionEnabled()).thenReturn(true);
        when(lifecycleService.executeRawSnapshotRetention(any())).thenReturn(new PartitionRetentionReport(
                0,
                0,
                0,
                false,
                false,
                Map.of(),
                Map.of(),
                List.of("skip market_snapshot: table not partitioned")
        ));
        when(marketSnapshotRepository.findCompactionCandidates(anyLong())).thenReturn(List.of());

        service.setPartitionLifecycleService(lifecycleService);
        service.setPartitioningProperties(partitioningProperties);
        MarketSnapshotPersistenceService.SnapshotCompactionResult result =
                service.compactSnapshots(Instant.parse("2026-03-01T12:00:00Z"));

        assertEquals(0, result.scannedCount());
        assertEquals(0, result.deletedCount());
        assertEquals(0, result.keptCount());
        verify(marketSnapshotRepository, times(1)).findCompactionCandidates(anyLong());
    }

    @Test
    void compactSnapshotsStopsOrphanCleanupBatchWhenFlipDeleteMakesNoProgress() {
        UUID keepId = UUID.randomUUID();
        UUID deleteId = UUID.randomUUID();
        long ts = Instant.parse("2026-03-01T11:57:10Z").toEpochMilli();

        when(marketSnapshotRepository.findCompactionCandidates(anyLong())).thenReturn(List.of(
                candidate(keepId, ts),
                candidate(deleteId, ts)
        ));
        when(flipRepository.findOrphanFlipIdsBySnapshotTimestampEpochMillisIn(anyCollection(), any()))
                .thenReturn(List.of(UUID.randomUUID()));
        when(flipRepository.deleteStepRowsByFlipIdIn(anyCollection())).thenReturn(1);
        when(flipRepository.deleteConstraintRowsByFlipIdIn(anyCollection())).thenReturn(1);
        when(flipRepository.deleteByIdIn(anyCollection())).thenReturn(0);

        MarketSnapshotPersistenceService service = createService(defaultRetention());

        MarketSnapshotPersistenceService.SnapshotCompactionResult result =
                service.compactSnapshots(Instant.parse("2026-03-01T12:00:00Z"));

        assertEquals(2, result.scannedCount());
        assertEquals(1, result.deletedCount());
        assertEquals(1, result.keptCount());
        verify(flipRepository, times(1)).findOrphanFlipIdsBySnapshotTimestampEpochMillisIn(anyCollection(), any());
    }

    @Test
    void compactSnapshotsPreservesInterruptedFlagWhenPauseIsInterrupted() {
        SnapshotRetentionProperties retention = defaultRetention();
        retention.setFlipDeleteBatchSize(1);
        retention.setFlipDeleteBatchPauseMillis(5L);

        long ts = Instant.parse("2026-03-01T11:57:10Z").toEpochMilli();
        when(marketSnapshotRepository.findCompactionCandidates(anyLong())).thenReturn(List.of(
                candidate(UUID.randomUUID(), ts),
                candidate(UUID.randomUUID(), ts),
                candidate(UUID.randomUUID(), ts)
        ));
        when(flipRepository.findOrphanFlipIdsBySnapshotTimestampEpochMillisIn(anyCollection(), any()))
                .thenReturn(List.of());

        MarketSnapshotPersistenceService service = createService(retention);

        try {
            Thread.currentThread().interrupt();
            service.compactSnapshots(Instant.parse("2026-03-01T12:00:00Z"));
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    private MarketSnapshotPersistenceService createService(SnapshotRetentionProperties retentionProperties) {
        return new MarketSnapshotPersistenceService(
                marketSnapshotRepository,
                flipRepository,
                new ObjectMapper(),
                blockingTimeTracker,
                retentionProperties,
                transactionManager
        );
    }

    private SnapshotRetentionProperties defaultRetention() {
        return new SnapshotRetentionProperties();
    }

    private static MarketSnapshotCompactionCandidate candidate(UUID id, long timestampEpochMillis) {
        return new MarketSnapshotCompactionCandidate() {
            @Override
            public UUID getId() {
                return id;
            }

            @Override
            public long getSnapshotTimestampEpochMillis() {
                return timestampEpochMillis;
            }
        };
    }
}
