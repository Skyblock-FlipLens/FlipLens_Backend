package com.skyblockflipper.backend.config.Jobs;

import com.skyblockflipper.backend.repository.AhItemSnapshotRepository;
import com.skyblockflipper.backend.repository.BzItemSnapshotRepository;
import com.skyblockflipper.backend.repository.PartitionAdminRepository;
import com.skyblockflipper.backend.service.flipping.FlipGenerationService;
import com.skyblockflipper.backend.service.item.NeuRepoIngestionService;
import com.skyblockflipper.backend.service.market.MarketDataProcessingService;
import com.skyblockflipper.backend.service.market.SnapshotRetentionProperties;
import com.skyblockflipper.backend.service.market.partitioning.PartitioningProperties;
import com.skyblockflipper.backend.service.market.polling.ElectionPollFreshnessService;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SourceJobsTest {

    @Test
    void compactAggregateSnapshotsSkipsRowDeletesWhenTablesArePartitioned() {
        AhItemSnapshotRepository ahRepository = mock(AhItemSnapshotRepository.class);
        BzItemSnapshotRepository bzRepository = mock(BzItemSnapshotRepository.class);
        PartitionAdminRepository partitionAdminRepository = mock(PartitionAdminRepository.class);
        PartitioningProperties partitioningProperties = new PartitioningProperties();

        when(partitionAdminRepository.isTablePartitioned("public", "ah_item_snapshot")).thenReturn(true);
        when(partitionAdminRepository.isTablePartitioned("public", "bz_item_snapshot")).thenReturn(true);

        SourceJobs jobs = new SourceJobs(
                mock(NeuRepoIngestionService.class),
                mock(MarketDataProcessingService.class),
                mock(FlipGenerationService.class),
                ahRepository,
                bzRepository,
                new SnapshotRetentionProperties(),
                mock(ElectionPollFreshnessService.class),
                partitionAdminRepository,
                partitioningProperties
        );

        jobs.compactAggregateSnapshots();

        verify(ahRepository, never()).deleteOlderThan(org.mockito.ArgumentMatchers.anyLong());
        verify(bzRepository, never()).deleteOlderThan(org.mockito.ArgumentMatchers.anyLong());
    }

    @Test
    void compactAggregateSnapshotsDeletesOnlyUnpartitionedTables() {
        AhItemSnapshotRepository ahRepository = mock(AhItemSnapshotRepository.class);
        BzItemSnapshotRepository bzRepository = mock(BzItemSnapshotRepository.class);
        PartitionAdminRepository partitionAdminRepository = mock(PartitionAdminRepository.class);
        PartitioningProperties partitioningProperties = new PartitioningProperties();

        when(partitionAdminRepository.isTablePartitioned("public", "ah_item_snapshot")).thenReturn(true);
        when(partitionAdminRepository.isTablePartitioned("public", "bz_item_snapshot")).thenReturn(false);

        SourceJobs jobs = new SourceJobs(
                mock(NeuRepoIngestionService.class),
                mock(MarketDataProcessingService.class),
                mock(FlipGenerationService.class),
                ahRepository,
                bzRepository,
                new SnapshotRetentionProperties(),
                mock(ElectionPollFreshnessService.class),
                partitionAdminRepository,
                partitioningProperties
        );

        jobs.compactAggregateSnapshots();

        verify(ahRepository, never()).deleteOlderThan(org.mockito.ArgumentMatchers.anyLong());
        verify(bzRepository).deleteOlderThan(org.mockito.ArgumentMatchers.anyLong());
    }
}
