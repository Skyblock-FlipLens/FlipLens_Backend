package com.skyblockflipper.backend.instrumentation.admin;

import com.skyblockflipper.backend.instrumentation.InstrumentationProperties;
import com.skyblockflipper.backend.instrumentation.JfrBlockingReportService;
import com.skyblockflipper.backend.instrumentation.JfrRecordingManager;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.web.server.ResponseStatusException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class InstrumentationAdminControllerTest {

    @Test
    void dumpSnapshotDelegatesToRecordingManager() {
        AdminAccessGuard adminAccessGuard = mock(AdminAccessGuard.class);
        JfrRecordingManager jfrRecordingManager = mock(JfrRecordingManager.class);
        JfrBlockingReportService reportService = mock(JfrBlockingReportService.class);
        InstrumentationProperties properties = new InstrumentationProperties();
        Path snapshotPath = Path.of("var", "profiling", "jfr", "snapshot-test.jfr");
        when(jfrRecordingManager.dumpSnapshot()).thenReturn(snapshotPath);
        InstrumentationAdminController controller = new InstrumentationAdminController(
                adminAccessGuard,
                jfrRecordingManager,
                reportService,
                properties
        );
        MockHttpServletRequest request = new MockHttpServletRequest();

        Map<String, Object> result = controller.dumpSnapshot(request);

        verify(adminAccessGuard, times(1)).validate(request);
        verify(jfrRecordingManager, times(1)).dumpSnapshot();
        assertEquals(snapshotPath.toString(), result.get("snapshot"));
        assertNotNull(result.get("createdAt"));
    }

    @Test
    void latestReportDelegatesToReportService() {
        AdminAccessGuard adminAccessGuard = mock(AdminAccessGuard.class);
        JfrRecordingManager jfrRecordingManager = mock(JfrRecordingManager.class);
        JfrBlockingReportService reportService = mock(JfrBlockingReportService.class);
        InstrumentationProperties properties = new InstrumentationProperties();
        Path latestPath = Path.of("var", "profiling", "jfr", "continuous.jfr");
        Map<String, Object> summary = Map.of("status", "ok");
        when(jfrRecordingManager.latestRecordingFile()).thenReturn(latestPath);
        when(reportService.summarize(latestPath)).thenReturn(summary);
        InstrumentationAdminController controller = new InstrumentationAdminController(
                adminAccessGuard,
                jfrRecordingManager,
                reportService,
                properties
        );
        MockHttpServletRequest request = new MockHttpServletRequest();

        Map<String, Object> result = controller.latestReport(request);

        verify(adminAccessGuard, times(1)).validate(request);
        verify(reportService, times(1)).summarize(latestPath);
        assertEquals(summary, result);
    }

    @Test
    void runAsyncProfilerReturnsNotFoundWhenDisabled() {
        AdminAccessGuard adminAccessGuard = mock(AdminAccessGuard.class);
        JfrRecordingManager jfrRecordingManager = mock(JfrRecordingManager.class);
        JfrBlockingReportService reportService = mock(JfrBlockingReportService.class);
        InstrumentationProperties properties = new InstrumentationProperties();
        properties.getAsyncProfiler().setEnabled(false);
        InstrumentationAdminController controller = new InstrumentationAdminController(
                adminAccessGuard,
                jfrRecordingManager,
                reportService,
                properties
        );
        MockHttpServletRequest request = new MockHttpServletRequest();

        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () -> controller.runAsyncProfiler(request));

        verify(adminAccessGuard, times(1)).validate(request);
        assertEquals(HttpStatus.NOT_FOUND, exception.getStatusCode());
    }

    @Test
    void runAsyncProfilerReturnsServerErrorWhenScriptCannotStart() throws Exception {
        AdminAccessGuard adminAccessGuard = mock(AdminAccessGuard.class);
        JfrRecordingManager jfrRecordingManager = mock(JfrRecordingManager.class);
        JfrBlockingReportService reportService = mock(JfrBlockingReportService.class);
        InstrumentationProperties properties = new InstrumentationProperties();
        properties.getAsyncProfiler().setEnabled(true);
        properties.getAsyncProfiler().setOutputDir(Files.createTempDirectory("async-profiler-test"));
        properties.getAsyncProfiler().setScriptPath("this-command-does-not-exist-xyz");
        InstrumentationAdminController controller = new InstrumentationAdminController(
                adminAccessGuard,
                jfrRecordingManager,
                reportService,
                properties
        );
        MockHttpServletRequest request = new MockHttpServletRequest();

        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () -> controller.runAsyncProfiler(request));

        verify(adminAccessGuard, times(1)).validate(request);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, exception.getStatusCode());
    }
}
