package com.skyblockflipper.backend.config.Jobs;

import com.skyblockflipper.backend.service.market.rollup.MarketBucketMaterializationService;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BucketMaterializationJobTest {

    @Test
    void materializeDueBucketsInvokesServiceForSuccessfulReport() {
        MarketBucketMaterializationService service = mock(MarketBucketMaterializationService.class);
        when(service.materializeDueBuckets(any(Instant.class)))
                .thenReturn(new MarketBucketMaterializationService.BucketMaterializationReport(3, 0, Map.of("BZ:1h", 3)));
        BucketMaterializationJob job = new BucketMaterializationJob(service);

        job.materializeDueBuckets();

        verify(service).materializeDueBuckets(any(Instant.class));
    }

    @Test
    void materializeDueBucketsSwallowsExceptions() {
        MarketBucketMaterializationService service = mock(MarketBucketMaterializationService.class);
        doThrow(new RuntimeException("boom")).when(service).materializeDueBuckets(any(Instant.class));
        BucketMaterializationJob job = new BucketMaterializationJob(service);

        job.materializeDueBuckets();

        verify(service).materializeDueBuckets(any(Instant.class));
    }
}
