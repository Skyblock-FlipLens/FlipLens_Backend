package com.skyblockflipper.backend.instrumentation.admin;

import com.skyblockflipper.backend.repository.FlipRepository;
import com.skyblockflipper.backend.service.flipping.storage.FlipStorageBackfillService;
import com.skyblockflipper.backend.service.flipping.storage.FlipStorageParityService;
import com.skyblockflipper.backend.service.flipping.storage.FlipStorageProperties;
import com.skyblockflipper.backend.service.flipping.storage.UnifiedFlipCurrentReadService;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;

import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FlipStorageAdminControllerTest {

    @Test
    void configIncludesOptionalHistoryToggleStates() {
        AdminAccessGuard adminAccessGuard = mock(AdminAccessGuard.class);
        FlipStorageProperties flipStorageProperties = new FlipStorageProperties();
        flipStorageProperties.setTopSnapshotMaterializationEnabled(true);
        flipStorageProperties.setSnapshotItemStateCaptureEnabled(false);
        FlipStorageParityService flipStorageParityService = mock(FlipStorageParityService.class);
        FlipStorageBackfillService flipStorageBackfillService = mock(FlipStorageBackfillService.class);
        FlipRepository flipRepository = mock(FlipRepository.class);
        UnifiedFlipCurrentReadService unifiedFlipCurrentReadService = mock(UnifiedFlipCurrentReadService.class);
        when(flipRepository.findMaxSnapshotTimestampEpochMillis()).thenReturn(Optional.of(111L));
        when(unifiedFlipCurrentReadService.latestSnapshotEpochMillis()).thenReturn(Optional.of(222L));
        FlipStorageAdminController controller = new FlipStorageAdminController(
                adminAccessGuard,
                flipStorageProperties,
                flipStorageParityService,
                flipStorageBackfillService,
                flipRepository,
                unifiedFlipCurrentReadService
        );
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRemoteAddr("127.0.0.1");

        Map<String, Object> response = controller.config(request);

        verify(adminAccessGuard).validate(request);
        assertTrue((Boolean) response.get("topSnapshotMaterializationEnabled"));
        assertFalse((Boolean) response.get("snapshotItemStateCaptureEnabled"));
        assertEquals(111L, response.get("legacyLatestSnapshotEpochMillis"));
        assertEquals(222L, response.get("currentLatestSnapshotEpochMillis"));
        assertNotNull(response.get("generatedAt"));
    }
}
