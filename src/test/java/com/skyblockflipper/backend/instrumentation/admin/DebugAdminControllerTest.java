package com.skyblockflipper.backend.instrumentation.admin;

import com.skyblockflipper.backend.config.Jobs.SourceJobs;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class DebugAdminControllerTest {

    @Test
    void triggerCompactSnapshotsValidatesAccessAndInvokesJob() {
        AdminAccessGuard adminAccessGuard = mock(AdminAccessGuard.class);
        SourceJobs sourceJobs = mock(SourceJobs.class);
        DebugAdminController controller = new DebugAdminController(adminAccessGuard, sourceJobs);
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRemoteAddr("127.0.0.1");

        Map<String, Object> response = controller.triggerCompactSnapshots(request);

        verify(adminAccessGuard, times(1)).validate(request);
        verify(sourceJobs, times(1)).compactSnapshots();
        assertEquals("triggered", response.get("status"));
        assertEquals("compactSnapshots", response.get("job"));
        assertNotNull(response.get("triggeredAtUtc"));
    }

    @Test
    void triggerCopyRepoValidatesAccessAndInvokesJob() {
        AdminAccessGuard adminAccessGuard = mock(AdminAccessGuard.class);
        SourceJobs sourceJobs = mock(SourceJobs.class);
        DebugAdminController controller = new DebugAdminController(adminAccessGuard, sourceJobs);
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRemoteAddr("127.0.0.1");

        Map<String, Object> response = controller.triggerCopyRepo(request);

        verify(adminAccessGuard, times(1)).validate(request);
        verify(sourceJobs, times(1)).copyRepoDaily();
        assertEquals("triggered", response.get("status"));
        assertEquals("copyRepoDaily", response.get("job"));
        assertNotNull(response.get("triggeredAtUtc"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void monitoringIncludesRuntimeAndLastManualTriggerTimestamps() {
        AdminAccessGuard adminAccessGuard = mock(AdminAccessGuard.class);
        SourceJobs sourceJobs = mock(SourceJobs.class);
        DebugAdminController controller = new DebugAdminController(adminAccessGuard, sourceJobs);
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRemoteAddr("127.0.0.1");

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
