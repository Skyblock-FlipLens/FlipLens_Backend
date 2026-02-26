package com.skyblockflipper.backend.config.Jobs;

import com.skyblockflipper.backend.service.market.MarketDataProcessingService;
import com.skyblockflipper.backend.service.market.MarketSnapshotPersistenceService;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RawSnapshotCompactionJobTest {

    @Test
    void compactSnapshotsInvokesCompactionService() {
        MarketDataProcessingService marketDataProcessingService = mock(MarketDataProcessingService.class);
        when(marketDataProcessingService.compactSnapshots())
                .thenReturn(new MarketSnapshotPersistenceService.SnapshotCompactionResult(12, 3, 9));
        RawSnapshotCompactionJob job = new RawSnapshotCompactionJob(marketDataProcessingService);

        job.compactSnapshots();

        verify(marketDataProcessingService, times(1)).compactSnapshots();
    }

    @Test
    void compactSnapshotsSwallowsExceptionAndContinues() {
        MarketDataProcessingService marketDataProcessingService = mock(MarketDataProcessingService.class);
        when(marketDataProcessingService.compactSnapshots()).thenThrow(new RuntimeException("boom"));
        RawSnapshotCompactionJob job = new RawSnapshotCompactionJob(marketDataProcessingService);

        assertDoesNotThrow(job::compactSnapshots);
        verify(marketDataProcessingService, times(1)).compactSnapshots();
    }
}
