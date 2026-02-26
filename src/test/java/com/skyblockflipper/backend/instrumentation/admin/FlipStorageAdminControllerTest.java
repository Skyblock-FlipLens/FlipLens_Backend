package com.skyblockflipper.backend.instrumentation.admin;

import com.skyblockflipper.backend.repository.FlipRepository;
import com.skyblockflipper.backend.service.flipping.storage.FlipStorageBackfillService;
import com.skyblockflipper.backend.service.flipping.storage.FlipStorageParityService;
import com.skyblockflipper.backend.service.flipping.storage.FlipStorageProperties;
import com.skyblockflipper.backend.service.flipping.storage.UnifiedFlipCurrentReadService;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FlipStorageAdminControllerTest {

    @Test
    void configReturnsCurrentStorageConfiguration() {
        AdminAccessGuard adminAccessGuard = mock(AdminAccessGuard.class);
        FlipStorageProperties properties = new FlipStorageProperties();
        properties.setDualWriteEnabled(true);
        properties.setReadFromNew(false);
        properties.setLegacyWriteEnabled(true);
        properties.setTrendRelativeThreshold(0.12D);
        properties.setTrendScoreDeltaThreshold(2.5D);
        properties.setParitySampleSize(15);
        FlipStorageParityService parityService = mock(FlipStorageParityService.class);
        FlipStorageBackfillService backfillService = mock(FlipStorageBackfillService.class);
        FlipRepository flipRepository = mock(FlipRepository.class);
        UnifiedFlipCurrentReadService unifiedFlipCurrentReadService = mock(UnifiedFlipCurrentReadService.class);
        when(flipRepository.findMaxSnapshotTimestampEpochMillis()).thenReturn(Optional.of(111L));
        when(unifiedFlipCurrentReadService.latestSnapshotEpochMillis()).thenReturn(Optional.of(222L));
        FlipStorageAdminController controller = new FlipStorageAdminController(
                adminAccessGuard,
                properties,
                parityService,
                backfillService,
                flipRepository,
                unifiedFlipCurrentReadService
        );
        MockHttpServletRequest request = new MockHttpServletRequest();

        Map<String, Object> result = controller.config(request);

        verify(adminAccessGuard, times(1)).validate(request);
        assertEquals(true, result.get("dualWriteEnabled"));
        assertEquals(false, result.get("readFromNew"));
        assertEquals(true, result.get("legacyWriteEnabled"));
        assertEquals(0.12D, result.get("trendRelativeThreshold"));
        assertEquals(2.5D, result.get("trendScoreDeltaThreshold"));
        assertEquals(15, result.get("paritySampleSize"));
        assertEquals(111L, result.get("legacyLatestSnapshotEpochMillis"));
        assertEquals(222L, result.get("currentLatestSnapshotEpochMillis"));
    }

    @Test
    void latestParityDelegatesToService() {
        AdminAccessGuard adminAccessGuard = mock(AdminAccessGuard.class);
        FlipStorageProperties properties = new FlipStorageProperties();
        FlipStorageParityService parityService = mock(FlipStorageParityService.class);
        FlipStorageBackfillService backfillService = mock(FlipStorageBackfillService.class);
        FlipRepository flipRepository = mock(FlipRepository.class);
        UnifiedFlipCurrentReadService unifiedFlipCurrentReadService = mock(UnifiedFlipCurrentReadService.class);
        FlipStorageParityService.FlipStorageParityReport report = new FlipStorageParityService.FlipStorageParityReport(
                Instant.now(),
                new FlipStorageParityService.Flags(true, false, true, 0.05D, 3.0D, 20),
                1000L,
                1000L,
                1000L,
                true,
                10,
                10,
                10,
                0,
                0,
                0,
                true,
                Map.of(),
                Map.of(),
                List.of(),
                List.of(),
                List.of()
        );
        when(parityService.latestParityReport()).thenReturn(report);
        FlipStorageAdminController controller = new FlipStorageAdminController(
                adminAccessGuard,
                properties,
                parityService,
                backfillService,
                flipRepository,
                unifiedFlipCurrentReadService
        );
        MockHttpServletRequest request = new MockHttpServletRequest();

        FlipStorageParityService.FlipStorageParityReport result = controller.latestParity(request);

        verify(adminAccessGuard, times(1)).validate(request);
        assertEquals(report, result);
    }

    @Test
    void backfillEndpointsDelegateToService() {
        AdminAccessGuard adminAccessGuard = mock(AdminAccessGuard.class);
        FlipStorageProperties properties = new FlipStorageProperties();
        FlipStorageParityService parityService = mock(FlipStorageParityService.class);
        FlipStorageBackfillService backfillService = mock(FlipStorageBackfillService.class);
        FlipRepository flipRepository = mock(FlipRepository.class);
        UnifiedFlipCurrentReadService unifiedFlipCurrentReadService = mock(UnifiedFlipCurrentReadService.class);
        FlipStorageBackfillService.BackfillResult latestResult = new FlipStorageBackfillService.BackfillResult(2000L, 8, true, "ok");
        FlipStorageBackfillService.BackfillResult snapshotResult = new FlipStorageBackfillService.BackfillResult(3000L, 6, true, "ok");
        when(backfillService.backfillLatestLegacySnapshot()).thenReturn(latestResult);
        when(backfillService.backfillSnapshot(3000L)).thenReturn(snapshotResult);
        FlipStorageAdminController controller = new FlipStorageAdminController(
                adminAccessGuard,
                properties,
                parityService,
                backfillService,
                flipRepository,
                unifiedFlipCurrentReadService
        );
        MockHttpServletRequest request = new MockHttpServletRequest();

        FlipStorageBackfillService.BackfillResult latest = controller.backfillLatest(request);
        FlipStorageBackfillService.BackfillResult snapshot = controller.backfillSnapshot(request, 3000L);

        verify(adminAccessGuard, times(2)).validate(request);
        assertEquals(latestResult, latest);
        assertEquals(snapshotResult, snapshot);
    }
}
