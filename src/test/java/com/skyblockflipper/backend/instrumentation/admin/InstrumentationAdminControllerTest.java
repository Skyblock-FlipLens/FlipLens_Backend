package com.skyblockflipper.backend.instrumentation.admin;

import com.skyblockflipper.backend.instrumentation.InstrumentationProperties;
import com.skyblockflipper.backend.instrumentation.JfrBlockingReportService;
import com.skyblockflipper.backend.instrumentation.JfrRecordingManager;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;
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
        ApplicationLogFileService applicationLogFileService = mock(ApplicationLogFileService.class);
        InstrumentationProperties properties = new InstrumentationProperties();
        Path snapshotPath = Path.of("var", "profiling", "jfr", "snapshot-test.jfr");
        when(jfrRecordingManager.dumpSnapshot()).thenReturn(snapshotPath);
        InstrumentationAdminController controller = new InstrumentationAdminController(
                adminAccessGuard,
                jfrRecordingManager,
                reportService,
                applicationLogFileService,
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
        ApplicationLogFileService applicationLogFileService = mock(ApplicationLogFileService.class);
        InstrumentationProperties properties = new InstrumentationProperties();
        Path latestPath = Path.of("var", "profiling", "jfr", "continuous.jfr");
        Map<String, Object> summary = Map.of("status", "ok");
        when(jfrRecordingManager.latestRecordingFile()).thenReturn(latestPath);
        when(reportService.summarize(latestPath)).thenReturn(summary);
        InstrumentationAdminController controller = new InstrumentationAdminController(
                adminAccessGuard,
                jfrRecordingManager,
                reportService,
                applicationLogFileService,
                properties
        );
        MockHttpServletRequest request = new MockHttpServletRequest();

        Map<String, Object> result = controller.latestReport(request);

        verify(adminAccessGuard, times(1)).validate(request);
        verify(reportService, times(1)).summarize(latestPath);
        assertEquals(summary, result);
    }

    @Test
    void latestReportFallsBackToSnapshotWhenLatestParseFails() {
        AdminAccessGuard adminAccessGuard = mock(AdminAccessGuard.class);
        JfrRecordingManager jfrRecordingManager = mock(JfrRecordingManager.class);
        JfrBlockingReportService reportService = mock(JfrBlockingReportService.class);
        ApplicationLogFileService applicationLogFileService = mock(ApplicationLogFileService.class);
        InstrumentationProperties properties = new InstrumentationProperties();
        Path latestPath = Path.of("var", "profiling", "jfr", "continuous.jfr");
        Path snapshotPath = Path.of("var", "profiling", "jfr", "snapshot-1.jfr");
        Map<String, Object> first = Map.of("error", "Failed to parse JFR: Not a Flight Recorder file");
        Map<String, Object> fallback = Map.of("status", "ok");
        when(jfrRecordingManager.latestRecordingFile()).thenReturn(latestPath);
        when(reportService.summarize(latestPath)).thenReturn(first);
        when(jfrRecordingManager.dumpSnapshot()).thenReturn(snapshotPath);
        when(reportService.summarize(snapshotPath)).thenReturn(fallback);
        InstrumentationAdminController controller = new InstrumentationAdminController(
                adminAccessGuard,
                jfrRecordingManager,
                reportService,
                applicationLogFileService,
                properties
        );
        MockHttpServletRequest request = new MockHttpServletRequest();

        Map<String, Object> result = controller.latestReport(request);

        verify(adminAccessGuard, times(1)).validate(request);
        verify(jfrRecordingManager, times(1)).latestRecordingFile();
        verify(jfrRecordingManager, times(1)).dumpSnapshot();
        verify(reportService, times(1)).summarize(latestPath);
        verify(reportService, times(1)).summarize(snapshotPath);
        assertEquals(fallback, result);
    }

    @Test
    void runAsyncProfilerReturnsNotFoundWhenDisabled() {
        AdminAccessGuard adminAccessGuard = mock(AdminAccessGuard.class);
        JfrRecordingManager jfrRecordingManager = mock(JfrRecordingManager.class);
        JfrBlockingReportService reportService = mock(JfrBlockingReportService.class);
        ApplicationLogFileService applicationLogFileService = mock(ApplicationLogFileService.class);
        InstrumentationProperties properties = new InstrumentationProperties();
        properties.getAsyncProfiler().setEnabled(false);
        InstrumentationAdminController controller = new InstrumentationAdminController(
                adminAccessGuard,
                jfrRecordingManager,
                reportService,
                applicationLogFileService,
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
        ApplicationLogFileService applicationLogFileService = mock(ApplicationLogFileService.class);
        InstrumentationProperties properties = new InstrumentationProperties();
        properties.getAsyncProfiler().setEnabled(true);
        properties.getAsyncProfiler().setOutputDir(Files.createTempDirectory("async-profiler-test"));
        properties.getAsyncProfiler().setScriptPath("this-command-does-not-exist-xyz");
        InstrumentationAdminController controller = new InstrumentationAdminController(
                adminAccessGuard,
                jfrRecordingManager,
                reportService,
                applicationLogFileService,
                properties
        );
        MockHttpServletRequest request = new MockHttpServletRequest();

        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () -> controller.runAsyncProfiler(request));

        verify(adminAccessGuard, times(1)).validate(request);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, exception.getStatusCode());
    }

    @Test
    void latestReportReturnsPrimaryWhenFallbackAlsoFails() {
        AdminAccessGuard adminAccessGuard = mock(AdminAccessGuard.class);
        JfrRecordingManager jfrRecordingManager = mock(JfrRecordingManager.class);
        JfrBlockingReportService reportService = mock(JfrBlockingReportService.class);
        ApplicationLogFileService applicationLogFileService = mock(ApplicationLogFileService.class);
        InstrumentationProperties properties = new InstrumentationProperties();
        Path latestPath = Path.of("var", "profiling", "jfr", "continuous.jfr");
        Path snapshotPath = Path.of("var", "profiling", "jfr", "snapshot-1.jfr");
        Map<String, Object> primary = Map.of("error", "Primary parse failed");
        Map<String, Object> fallbackError = Map.of("error", "Fallback also failed");
        when(jfrRecordingManager.latestRecordingFile()).thenReturn(latestPath);
        when(reportService.summarize(latestPath)).thenReturn(primary);
        when(jfrRecordingManager.dumpSnapshot()).thenReturn(snapshotPath);
        when(reportService.summarize(snapshotPath)).thenReturn(fallbackError);
        InstrumentationAdminController controller = new InstrumentationAdminController(
                adminAccessGuard, jfrRecordingManager, reportService, applicationLogFileService, properties);
        MockHttpServletRequest request = new MockHttpServletRequest();

        Map<String, Object> result = controller.latestReport(request);

        assertEquals(primary, result);  // Should return primary when fallback also fails
    }

    @Test
    void appLogReturnsPlainTextTailAndHeaders() {
        AdminAccessGuard adminAccessGuard = mock(AdminAccessGuard.class);
        JfrRecordingManager jfrRecordingManager = mock(JfrRecordingManager.class);
        JfrBlockingReportService reportService = mock(JfrBlockingReportService.class);
        ApplicationLogFileService applicationLogFileService = mock(ApplicationLogFileService.class);
        InstrumentationProperties properties = new InstrumentationProperties();
        Path logFile = Path.of("var", "log", "application.log");
        when(applicationLogFileService.readTail(150)).thenReturn(new ApplicationLogFileService.ApplicationLogTail(
                true,
                logFile,
                1234L,
                java.time.Instant.parse("2026-03-18T12:00:00Z"),
                false,
                150,
                "line-1\nline-2",
                null
        ));
        InstrumentationAdminController controller = new InstrumentationAdminController(
                adminAccessGuard, jfrRecordingManager, reportService, applicationLogFileService, properties);
        MockHttpServletRequest request = new MockHttpServletRequest();

        ResponseEntity<String> response = controller.appLog(request, 150);

        verify(adminAccessGuard).validate(request);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals("line-1\nline-2", response.getBody());
        assertEquals(logFile.toString(), response.getHeaders().getFirst("X-Log-File-Path"));
        assertEquals("150", response.getHeaders().getFirst("X-Log-Line-Limit"));
    }

    @Test
    void appLogReturnsNotFoundWhenUnavailable() {
        AdminAccessGuard adminAccessGuard = mock(AdminAccessGuard.class);
        JfrRecordingManager jfrRecordingManager = mock(JfrRecordingManager.class);
        JfrBlockingReportService reportService = mock(JfrBlockingReportService.class);
        ApplicationLogFileService applicationLogFileService = mock(ApplicationLogFileService.class);
        InstrumentationProperties properties = new InstrumentationProperties();
        when(applicationLogFileService.readTail(200)).thenReturn(ApplicationLogFileService.ApplicationLogTail.unavailable("log_file_missing"));
        InstrumentationAdminController controller = new InstrumentationAdminController(
                adminAccessGuard, jfrRecordingManager, reportService, applicationLogFileService, properties);
        MockHttpServletRequest request = new MockHttpServletRequest();

        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () -> controller.appLog(request, 200));

        assertEquals(HttpStatus.NOT_FOUND, exception.getStatusCode());
    }
}
