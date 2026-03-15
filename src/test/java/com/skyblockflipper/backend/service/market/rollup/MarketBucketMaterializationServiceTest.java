package com.skyblockflipper.backend.service.market.rollup;

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
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.SimpleTransactionStatus;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MarketBucketMaterializationServiceTest {

    @Test
    void materializeDueBucketsPersistsClosedBzBucketAndState() {
        SnapshotRollupProperties properties = new SnapshotRollupProperties();
        properties.setMaxBucketsPerRun(1);
        PartitioningProperties partitioningProperties = new PartitioningProperties();
        partitioningProperties.setBzSnapshotParentTable("bz_item_snapshot");

        BzItemSnapshotRepository bzItemSnapshotRepository = mock(BzItemSnapshotRepository.class);
        AhItemSnapshotRepository ahItemSnapshotRepository = mock(AhItemSnapshotRepository.class);
        BzItemBucketRollupRepository bzItemBucketRollupRepository = mock(BzItemBucketRollupRepository.class);
        BzItemAnomalySegmentRepository bzItemAnomalySegmentRepository = mock(BzItemAnomalySegmentRepository.class);
        AhItemBucketRollupRepository ahItemBucketRollupRepository = mock(AhItemBucketRollupRepository.class);
        AhItemAnomalySegmentRepository ahItemAnomalySegmentRepository = mock(AhItemAnomalySegmentRepository.class);
        ItemBucketMaterializationStateRepository materializationStateRepository = mock(ItemBucketMaterializationStateRepository.class);
        PlatformTransactionManager transactionManager = mock(PlatformTransactionManager.class);

        long bucketStart = Instant.parse("2026-03-14T00:00:00Z").toEpochMilli();
        long bucketEnd = bucketStart + 60_000L;
        when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
        when(bzItemSnapshotRepository.findMinSnapshotTs()).thenReturn(bucketStart);
        when(bzItemSnapshotRepository.findMaxSnapshotTs()).thenReturn(bucketStart + 25_000L);
        List<com.skyblockflipper.backend.model.market.BzItemSnapshotEntity> rows = List.of(
                new com.skyblockflipper.backend.model.market.BzItemSnapshotEntity(bucketStart, "ENCHANTED_DIAMOND", 100.0D, 99.0D, 1_000L, 1_000L),
                new com.skyblockflipper.backend.model.market.BzItemSnapshotEntity(bucketStart + 5_000L, "ENCHANTED_DIAMOND", 100.1D, 99.1D, 1_010L, 1_000L),
                new com.skyblockflipper.backend.model.market.BzItemSnapshotEntity(bucketStart + 10_000L, "ENCHANTED_DIAMOND", 100.2D, 99.2D, 1_005L, 1_020L)
        );
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            Consumer<com.skyblockflipper.backend.model.market.BzItemSnapshotEntity> consumer =
                    (Consumer<com.skyblockflipper.backend.model.market.BzItemSnapshotEntity>) invocation.getArgument(3);
            rows.forEach(consumer);
            return null;
        }).when(bzItemSnapshotRepository).scanBucketRows(eq(bucketStart), eq(bucketEnd), anyInt(), any());
        when(ahItemSnapshotRepository.findMinSnapshotTs()).thenReturn(null);
        when(ahItemSnapshotRepository.findMaxSnapshotTs()).thenReturn(null);
        when(materializationStateRepository.findTopByMarketTypeAndBucketGranularityAndFinalizedTrueOrderByBucketStartEpochMillisDesc("BZ", "1m"))
                .thenReturn(null);

        MarketBucketMaterializationService service = new MarketBucketMaterializationService(
                properties,
                partitioningProperties,
                bzItemSnapshotRepository,
                ahItemSnapshotRepository,
                bzItemBucketRollupRepository,
                bzItemAnomalySegmentRepository,
                ahItemBucketRollupRepository,
                ahItemAnomalySegmentRepository,
                materializationStateRepository,
                new BzItemBucketAnalyzer(properties),
                new AhItemBucketAnalyzer(properties),
                transactionManager
        );

        MarketBucketMaterializationService.BucketMaterializationReport report =
                service.materializeDueBuckets(Instant.parse("2026-03-14T00:02:00Z"));

        assertEquals(1, report.processedBuckets());
        verify(bzItemBucketRollupRepository).saveAll(any());
        verify(bzItemAnomalySegmentRepository, never()).saveAll(any());

        ArgumentCaptor<ItemBucketMaterializationStateEntity> stateCaptor =
                ArgumentCaptor.forClass(ItemBucketMaterializationStateEntity.class);
        verify(materializationStateRepository).save(stateCaptor.capture());
        ItemBucketMaterializationStateEntity state = stateCaptor.getValue();
        assertEquals("BZ", state.getMarketType());
        assertEquals("1m", state.getBucketGranularity());
        assertTrue(state.isFinalized());
        assertEquals("bz_item_snapshot_2026_03_14", state.getSourcePartition());
    }

    @Test
    void isAggregatePartitionMaterializedRequiresAllGranularities() {
        SnapshotRollupProperties properties = new SnapshotRollupProperties();
        PartitioningProperties partitioningProperties = new PartitioningProperties();
        partitioningProperties.setBzSnapshotParentTable("bz_item_snapshot");

        BzItemSnapshotRepository bzItemSnapshotRepository = mock(BzItemSnapshotRepository.class);
        ItemBucketMaterializationStateRepository materializationStateRepository = mock(ItemBucketMaterializationStateRepository.class);
        when(materializationStateRepository.countBySourcePartitionAndMarketTypeAndBucketGranularityAndFailedTrue(anyString(), anyString(), anyString()))
                .thenReturn(0L);
        when(materializationStateRepository.countBySourcePartitionAndMarketTypeAndBucketGranularityAndFinalizedTrue("bz_item_snapshot_2026_03_14", "BZ", "1m"))
                .thenReturn(1_440L);
        when(materializationStateRepository.countBySourcePartitionAndMarketTypeAndBucketGranularityAndFinalizedTrue("bz_item_snapshot_2026_03_14", "BZ", "2h"))
                .thenReturn(12L);
        when(materializationStateRepository.countBySourcePartitionAndMarketTypeAndBucketGranularityAndFinalizedTrue("bz_item_snapshot_2026_03_14", "BZ", "1d"))
                .thenReturn(1L);
        ItemBucketMaterializationStateEntity dailyState = new ItemBucketMaterializationStateEntity();
        dailyState.setBucketStartEpochMillis(Instant.parse("2026-03-14T00:00:00Z").toEpochMilli());
        dailyState.setBucketGranularity("1d");
        dailyState.setMarketType("BZ");
        dailyState.setFinalized(true);
        dailyState.setFailed(false);
        dailyState.setRollupRowCount(9L);
        when(materializationStateRepository.findByBucketStartEpochMillisAndBucketGranularityAndMarketType(
                Instant.parse("2026-03-14T00:00:00Z").toEpochMilli(),
                "1d",
                "BZ"
        )).thenReturn(dailyState);
        when(bzItemSnapshotRepository.countDistinctProductIdBySnapshotTsGreaterThanEqualAndSnapshotTsLessThan(
                Instant.parse("2026-03-14T00:00:00Z").toEpochMilli(),
                Instant.parse("2026-03-15T00:00:00Z").toEpochMilli()
        )).thenReturn(10L);

        MarketBucketMaterializationService service = new MarketBucketMaterializationService(
                properties,
                partitioningProperties,
                bzItemSnapshotRepository,
                mock(AhItemSnapshotRepository.class),
                mock(BzItemBucketRollupRepository.class),
                mock(BzItemAnomalySegmentRepository.class),
                mock(AhItemBucketRollupRepository.class),
                mock(AhItemAnomalySegmentRepository.class),
                materializationStateRepository,
                new BzItemBucketAnalyzer(properties),
                new AhItemBucketAnalyzer(properties),
                transactionManager()
        );

        assertTrue(service.isAggregatePartitionMaterialized("bz_item_snapshot", LocalDate.parse("2026-03-14")));
        when(materializationStateRepository.countBySourcePartitionAndMarketTypeAndBucketGranularityAndFailedTrue("bz_item_snapshot_2026_03_14", "BZ", "2h"))
                .thenReturn(1L);
        org.junit.jupiter.api.Assertions.assertFalse(
                service.isAggregatePartitionMaterialized("bz_item_snapshot", LocalDate.parse("2026-03-14"))
        );
    }

    private PlatformTransactionManager transactionManager() {
        PlatformTransactionManager transactionManager = mock(PlatformTransactionManager.class);
        when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
        return transactionManager;
    }
}
