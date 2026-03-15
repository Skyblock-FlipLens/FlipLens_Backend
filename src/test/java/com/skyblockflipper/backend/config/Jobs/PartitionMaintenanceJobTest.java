package com.skyblockflipper.backend.config.Jobs;

import com.skyblockflipper.backend.service.market.partitioning.PartitionLifecycleService;
import com.skyblockflipper.backend.service.market.partitioning.PartitionRetentionReport;
import com.skyblockflipper.backend.service.market.rollup.MarketBucketMaterializationService;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PartitionMaintenanceJobTest {

    @Test
    void ensureForwardPartitionsInvokesLifecycleService() {
        PartitionLifecycleService lifecycleService = mock(PartitionLifecycleService.class);
        PartitionMaintenanceJob job = new PartitionMaintenanceJob(lifecycleService);

        job.ensureForwardPartitions();

        verify(lifecycleService).ensureForwardPartitions(any(Instant.class));
    }

    @Test
    void ensureForwardPartitionsSwallowsLifecycleExceptions() {
        PartitionLifecycleService lifecycleService = mock(PartitionLifecycleService.class);
        doThrow(new RuntimeException("boom")).when(lifecycleService).ensureForwardPartitions(any(Instant.class));
        PartitionMaintenanceJob job = new PartitionMaintenanceJob(lifecycleService);

        job.ensureForwardPartitions();

        verify(lifecycleService).ensureForwardPartitions(any(Instant.class));
    }

    @Test
    void compactAggregatePartitionsMaterializesBucketsBeforeRetention() {
        PartitionLifecycleService lifecycleService = mock(PartitionLifecycleService.class);
        MarketBucketMaterializationService materializationService = mock(MarketBucketMaterializationService.class);
        PartitionMaintenanceJob job = new PartitionMaintenanceJob(lifecycleService);
        job.setMarketBucketMaterializationService(materializationService);
        when(materializationService.isEnabled()).thenReturn(true);
        when(materializationService.materializeDueBuckets(any(Instant.class)))
                .thenReturn(new MarketBucketMaterializationService.BucketMaterializationReport(4, 1, Map.of("AH:1h", 4)));
        when(lifecycleService.executeAggregateRetention(any(Instant.class)))
                .thenReturn(new PartitionRetentionReport(8, 2, 0, false, true, Map.of("ah", 2), Map.of(), List.of()));

        job.compactAggregatePartitions();

        verify(materializationService).materializeDueBuckets(any(Instant.class));
        verify(lifecycleService).executeAggregateRetention(any(Instant.class));
    }

    @Test
    void compactAggregatePartitionsSkipsMaterializationWhenUnavailableOrDisabled() {
        PartitionLifecycleService lifecycleService = mock(PartitionLifecycleService.class);
        PartitionMaintenanceJob job = new PartitionMaintenanceJob(lifecycleService);
        when(lifecycleService.executeAggregateRetention(any(Instant.class)))
                .thenReturn(PartitionRetentionReport.empty(false));

        job.compactAggregatePartitions();

        verify(lifecycleService).executeAggregateRetention(any(Instant.class));

        MarketBucketMaterializationService disabledService = mock(MarketBucketMaterializationService.class);
        when(disabledService.isEnabled()).thenReturn(false);
        job.setMarketBucketMaterializationService(disabledService);

        job.compactAggregatePartitions();

        verify(disabledService, never()).materializeDueBuckets(any(Instant.class));
    }

    @Test
    void compactAggregatePartitionsSwallowsRetentionExceptions() {
        PartitionLifecycleService lifecycleService = mock(PartitionLifecycleService.class);
        PartitionMaintenanceJob job = new PartitionMaintenanceJob(lifecycleService);
        doThrow(new RuntimeException("boom")).when(lifecycleService).executeAggregateRetention(any(Instant.class));

        job.compactAggregatePartitions();

        verify(lifecycleService).executeAggregateRetention(any(Instant.class));
    }
}
