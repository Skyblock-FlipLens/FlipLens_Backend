package com.skyblockflipper.backend.config.Jobs;

import com.skyblockflipper.backend.repository.AhItemSnapshotRepository;
import com.skyblockflipper.backend.repository.BzItemSnapshotRepository;
import com.skyblockflipper.backend.repository.PartitionAdminRepository;
import com.skyblockflipper.backend.service.flipping.FlipGenerationService;
import com.skyblockflipper.backend.service.item.NeuRepoIngestionService;
import com.skyblockflipper.backend.service.market.MarketDataProcessingService;
import com.skyblockflipper.backend.service.market.SnapshotRetentionProperties;
import com.skyblockflipper.backend.model.market.MarketSnapshot;
import com.skyblockflipper.backend.service.market.partitioning.PartitioningProperties;
import com.skyblockflipper.backend.service.market.polling.ElectionPollFreshnessService;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
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

    @Test
    void compactAggregateSnapshotsHandlesPartitionLookupFailure() {
        AhItemSnapshotRepository ahRepository = mock(AhItemSnapshotRepository.class);
        BzItemSnapshotRepository bzRepository = mock(BzItemSnapshotRepository.class);
        PartitionAdminRepository partitionAdminRepository = mock(PartitionAdminRepository.class);
        PartitioningProperties partitioningProperties = new PartitioningProperties();

        when(partitionAdminRepository.isTablePartitioned("public", "ah_item_snapshot"))
                .thenThrow(new RuntimeException("boom"));
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

        assertDoesNotThrow(jobs::compactAggregateSnapshots);

        verify(ahRepository).deleteOlderThan(org.mockito.ArgumentMatchers.anyLong());
        verify(bzRepository, never()).deleteOlderThan(org.mockito.ArgumentMatchers.anyLong());
    }

    @Test
    void refreshElectionPollSwallowsExceptionsFromFreshnessService() {
        ElectionPollFreshnessService electionPollFreshnessService = mock(ElectionPollFreshnessService.class);
        doThrow(new RuntimeException("boom")).when(electionPollFreshnessService).ensureRecentElectionPoll();
        SourceJobs jobs = new SourceJobs(
                mock(NeuRepoIngestionService.class),
                mock(MarketDataProcessingService.class),
                mock(FlipGenerationService.class),
                mock(AhItemSnapshotRepository.class),
                mock(BzItemSnapshotRepository.class),
                new SnapshotRetentionProperties(),
                electionPollFreshnessService,
                mock(PartitionAdminRepository.class),
                new PartitioningProperties()
        );

        assertDoesNotThrow(jobs::refreshElectionPoll);

        verify(electionPollFreshnessService).ensureRecentElectionPoll();
    }

    @Test
    void copyRepoDailyRefreshesNeuItemsAndRegeneratesLatestSnapshot() throws IOException, InterruptedException {
        NeuRepoIngestionService neuRepoIngestionService = mock(NeuRepoIngestionService.class);
        MarketDataProcessingService marketDataProcessingService = mock(MarketDataProcessingService.class);
        FlipGenerationService flipGenerationService = mock(FlipGenerationService.class);
        SourceJobs jobs = new SourceJobs(
                neuRepoIngestionService,
                marketDataProcessingService,
                flipGenerationService,
                mock(AhItemSnapshotRepository.class),
                mock(BzItemSnapshotRepository.class),
                new SnapshotRetentionProperties(),
                mock(ElectionPollFreshnessService.class),
                mock(PartitionAdminRepository.class),
                new PartitioningProperties()
        );
        Instant snapshotTimestamp = Instant.parse("2026-03-01T12:00:00Z");
        when(neuRepoIngestionService.ingestLatestFilteredItems()).thenReturn(17);
        when(marketDataProcessingService.latestMarketSnapshot()).thenReturn(Optional.of(
                new MarketSnapshot(snapshotTimestamp, List.of(), Map.of())
        ));
        when(flipGenerationService.regenerateForSnapshot(snapshotTimestamp))
                .thenReturn(new FlipGenerationService.GenerationResult(9, 2, false));

        jobs.copyRepoDaily();

        verify(neuRepoIngestionService).ingestLatestFilteredItems();
        verify(marketDataProcessingService).latestMarketSnapshot();
        verify(flipGenerationService).regenerateForSnapshot(snapshotTimestamp);
    }

    @Test
    void copyRepoDailyAsyncDelegatesToSameRefreshLogic() throws IOException, InterruptedException {
        NeuRepoIngestionService neuRepoIngestionService = mock(NeuRepoIngestionService.class);
        MarketDataProcessingService marketDataProcessingService = mock(MarketDataProcessingService.class);
        FlipGenerationService flipGenerationService = mock(FlipGenerationService.class);
        SourceJobs jobs = new SourceJobs(
                neuRepoIngestionService,
                marketDataProcessingService,
                flipGenerationService,
                mock(AhItemSnapshotRepository.class),
                mock(BzItemSnapshotRepository.class),
                new SnapshotRetentionProperties(),
                mock(ElectionPollFreshnessService.class),
                mock(PartitionAdminRepository.class),
                new PartitioningProperties()
        );
        when(neuRepoIngestionService.ingestLatestFilteredItems()).thenReturn(1);
        when(marketDataProcessingService.latestMarketSnapshot()).thenReturn(Optional.empty());

        jobs.copyRepoDailyAsync();

        verify(neuRepoIngestionService).ingestLatestFilteredItems();
        verify(marketDataProcessingService).latestMarketSnapshot();
        verifyNoInteractions(flipGenerationService);
    }
}
