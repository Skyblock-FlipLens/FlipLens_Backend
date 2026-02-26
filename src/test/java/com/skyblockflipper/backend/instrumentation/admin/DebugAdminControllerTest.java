package com.skyblockflipper.backend.instrumentation.admin;

import com.skyblockflipper.backend.config.Jobs.SourceJobs;
import com.skyblockflipper.backend.service.market.CompactionRequestService;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DebugAdminControllerTest {

    @Test
    void triggerCompactSnapshotsValidatesAccessAndRequestsCompaction() {
        AdminAccessGuard adminAccessGuard = mock(AdminAccessGuard.class);
        SourceJobs sourceJobs = mock(SourceJobs.class);
        CompactionRequestService compactionRequestService = mock(CompactionRequestService.class);
        DebugAdminController controller = new DebugAdminController(adminAccessGuard, sourceJobs, compactionRequestService);
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRemoteAddr("127.0.0.1");
        when(compactionRequestService.request("api:127.0.0.1"))
                .thenReturn(Map.of("status", "requested", "requestedBy", "api:127.0.0.1", "requestedAtUtc", "2026-02-26T00:00:00Z"));

        Map<String, Object> response = controller.triggerCompactSnapshots(request);

        verify(adminAccessGuard, times(1)).validate(request);
        verify(compactionRequestService, times(1)).request("api:127.0.0.1");
        assertEquals("requested", response.get("status"));
        assertEquals("compactSnapshots", response.get("job"));
        assertNotNull(response.get("triggeredAtUtc"));
    }

    @Test
    void requestCompactionValidatesAccessAndRequestsCompaction() {
        AdminAccessGuard adminAccessGuard = mock(AdminAccessGuard.class);
        SourceJobs sourceJobs = mock(SourceJobs.class);
        CompactionRequestService compactionRequestService = mock(CompactionRequestService.class);
        DebugAdminController controller = new DebugAdminController(adminAccessGuard, sourceJobs, compactionRequestService);
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRemoteAddr("127.0.0.1");
        when(compactionRequestService.request("api:127.0.0.1"))
                .thenReturn(Map.of("status", "requested", "requestedBy", "api:127.0.0.1", "requestedAtUtc", "2026-02-26T00:00:00Z"));

        Map<String, Object> response = controller.requestCompaction(request);

        verify(adminAccessGuard, times(1)).validate(request);
        verify(compactionRequestService, times(1)).request("api:127.0.0.1");
        assertEquals("requested", response.get("status"));
        assertEquals("requestCompaction", response.get("job"));
        assertNotNull(response.get("triggeredAtUtc"));
    }

    @Test
    void triggerCopyRepoValidatesAccessAndInvokesJob() {
        AdminAccessGuard adminAccessGuard = mock(AdminAccessGuard.class);
        SourceJobs sourceJobs = mock(SourceJobs.class);
        CompactionRequestService compactionRequestService = mock(CompactionRequestService.class);
        DebugAdminController controller = new DebugAdminController(adminAccessGuard, sourceJobs, compactionRequestService);
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRemoteAddr("127.0.0.1");

        Map<String, Object> response = controller.triggerCopyRepo(request);

        verify(adminAccessGuard, times(1)).validate(request);
        verify(sourceJobs, times(1)).copyRepoDailyAsync();
        assertEquals("triggered", response.get("status"));
        assertEquals("copyRepoDaily", response.get("job"));
        assertNotNull(response.get("triggeredAtUtc"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void monitoringIncludesRuntimeAndLastManualTriggerTimestamps() {
        AdminAccessGuard adminAccessGuard = mock(AdminAccessGuard.class);
        SourceJobs sourceJobs = mock(SourceJobs.class);
        CompactionRequestService compactionRequestService = mock(CompactionRequestService.class);
        DebugAdminController controller = new DebugAdminController(adminAccessGuard, sourceJobs, compactionRequestService);
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRemoteAddr("127.0.0.1");
        when(compactionRequestService.request("api:127.0.0.1"))
                .thenReturn(Map.of("status", "requested", "requestedBy", "api:127.0.0.1", "requestedAtUtc", "2026-02-26T00:00:00Z"));

        controller.triggerCompactSnapshots(request);
        controller.triggerCopyRepo(request);
        Map<String, Object> response = controller.monitoring(request);

        verify(adminAccessGuard, times(3)).validate(request);
        assertNotNull(response.get("timestampUtc"));
        assertTrue((Long) response.get("heapUsedBytes") >= 0L);
        assertTrue((Long) response.get("heapMaxBytes") > 0L);
        assertTrue((Integer) response.get("threadCount") > 0);
        assertTrue((Integer) response.get("availableProcessors") > 0);

        Map<String, Object> triggerTimestamps = (Map<String, Object>) response.get("lastManualTriggerTimestampsUtc");
        assertNotNull(triggerTimestamps.get("compactSnapshots"));
        assertNotNull(triggerTimestamps.get("copyRepoDaily"));
    }
}
