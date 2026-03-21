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

    private TestFixture createFixture() {
        AdminAccessGuard adminAccessGuard = mock(AdminAccessGuard.class);
        JfrRecordingManager jfrRecordingManager = mock(JfrRecordingManager.class);
        JfrBlockingReportService reportService = mock(JfrBlockingReportService.class);
        ApplicationLogFileService applicationLogFileService = mock(ApplicationLogFileService.class);
        InstrumentationProperties properties = new InstrumentationProperties();
        InstrumentationAdminController controller = new InstrumentationAdminController(
                adminAccessGuard,
                jfrRecordingManager,
                reportService,
                applicationLogFileService,
                properties
        );
        return new TestFixture(
                adminAccessGuard,
                jfrRecordingManager,
                reportService,
                applicationLogFileService,
                properties,
                controller
        );
    }

    @Test
    void dumpSnapshotDelegatesToRecordingManager() {
        TestFixture fixture = createFixture();
        Path snapshotPath = Path.of("var", "profiling", "jfr", "snapshot-test.jfr");
        when(fixture.jfrRecordingManager().dumpSnapshot()).thenReturn(snapshotPath);
        MockHttpServletRequest request = new MockHttpServletRequest();

        Map<String, Object> result = fixture.controller().dumpSnapshot(request);

        verify(fixture.adminAccessGuard(), times(1)).validate(request);
        verify(fixture.jfrRecordingManager(), times(1)).dumpSnapshot();
        assertEquals(snapshotPath.toString(), result.get("snapshot"));
        assertNotNull(result.get("createdAt"));
    }

    @Test
    void latestReportDelegatesToReportService() {
        TestFixture fixture = createFixture();
        Path latestPath = Path.of("var", "profiling", "jfr", "continuous.jfr");
        Map<String, Object> summary = Map.of("status", "ok");
        when(fixture.jfrRecordingManager().latestRecordingFile()).thenReturn(latestPath);
        when(fixture.reportService().summarize(latestPath)).thenReturn(summary);
        MockHttpServletRequest request = new MockHttpServletRequest();

        Map<String, Object> result = fixture.controller().latestReport(request);

        verify(fixture.adminAccessGuard(), times(1)).validate(request);
        verify(fixture.reportService(), times(1)).summarize(latestPath);
        assertEquals(summary, result);
    }

    @Test
    void latestReportFallsBackToSnapshotWhenLatestParseFails() {
        TestFixture fixture = createFixture();
        Path latestPath = Path.of("var", "profiling", "jfr", "continuous.jfr");
        Path snapshotPath = Path.of("var", "profiling", "jfr", "snapshot-1.jfr");
        Map<String, Object> first = Map.of("error", "Failed to parse JFR: Not a Flight Recorder file");
        Map<String, Object> fallback = Map.of("status", "ok");
        when(fixture.jfrRecordingManager().latestRecordingFile()).thenReturn(latestPath);
        when(fixture.reportService().summarize(latestPath)).thenReturn(first);
        when(fixture.jfrRecordingManager().dumpSnapshot()).thenReturn(snapshotPath);
        when(fixture.reportService().summarize(snapshotPath)).thenReturn(fallback);
        MockHttpServletRequest request = new MockHttpServletRequest();

        Map<String, Object> result = fixture.controller().latestReport(request);

        verify(fixture.adminAccessGuard(), times(1)).validate(request);
        verify(fixture.jfrRecordingManager(), times(1)).latestRecordingFile();
        verify(fixture.jfrRecordingManager(), times(1)).dumpSnapshot();
        verify(fixture.reportService(), times(1)).summarize(latestPath);
        verify(fixture.reportService(), times(1)).summarize(snapshotPath);
        assertEquals(fallback, result);
    }

    @Test
    void runAsyncProfilerReturnsNotFoundWhenDisabled() {
        TestFixture fixture = createFixture();
        fixture.properties().getAsyncProfiler().setEnabled(false);
        MockHttpServletRequest request = new MockHttpServletRequest();

        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () -> fixture.controller().runAsyncProfiler(request));

        verify(fixture.adminAccessGuard(), times(1)).validate(request);
        assertEquals(HttpStatus.NOT_FOUND, exception.getStatusCode());
    }

    @Test
    void runAsyncProfilerReturnsServerErrorWhenScriptCannotStart() throws Exception {
        TestFixture fixture = createFixture();
        fixture.properties().getAsyncProfiler().setEnabled(true);
        fixture.properties().getAsyncProfiler().setOutputDir(Files.createTempDirectory("async-profiler-test"));
        fixture.properties().getAsyncProfiler().setScriptPath("this-command-does-not-exist-xyz");
        MockHttpServletRequest request = new MockHttpServletRequest();

        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () -> fixture.controller().runAsyncProfiler(request));

        verify(fixture.adminAccessGuard(), times(1)).validate(request);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, exception.getStatusCode());
    }

    @Test
    void latestReportReturnsPrimaryWhenFallbackAlsoFails() {
        TestFixture fixture = createFixture();
        Path latestPath = Path.of("var", "profiling", "jfr", "continuous.jfr");
        Path snapshotPath = Path.of("var", "profiling", "jfr", "snapshot-1.jfr");
        Map<String, Object> primary = Map.of("error", "Primary parse failed");
        Map<String, Object> fallbackError = Map.of("error", "Fallback also failed");
        when(fixture.jfrRecordingManager().latestRecordingFile()).thenReturn(latestPath);
        when(fixture.reportService().summarize(latestPath)).thenReturn(primary);
        when(fixture.jfrRecordingManager().dumpSnapshot()).thenReturn(snapshotPath);
        when(fixture.reportService().summarize(snapshotPath)).thenReturn(fallbackError);
        MockHttpServletRequest request = new MockHttpServletRequest();

        Map<String, Object> result = fixture.controller().latestReport(request);

        verify(fixture.adminAccessGuard(), times(1)).validate(request);
        assertEquals(primary, result);
    }

    @Test
    void appLogReturnsPlainTextTailAndHeaders() {
        TestFixture fixture = createFixture();
        Path logFile = Path.of("var", "log", "application.log");
        when(fixture.applicationLogFileService().readTail(150)).thenReturn(new ApplicationLogFileService.ApplicationLogTail(
                true,
                logFile,
                1234L,
                java.time.Instant.parse("2026-03-18T12:00:00Z"),
                false,
                150,
                "line-1\nline-2",
                null
        ));
        MockHttpServletRequest request = new MockHttpServletRequest();

        ResponseEntity<String> response = fixture.controller().appLog(request, 150);

        verify(fixture.adminAccessGuard()).validate(request);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals("line-1\nline-2", response.getBody());
        assertEquals(logFile.toString(), response.getHeaders().getFirst("X-Log-File-Path"));
        assertEquals("150", response.getHeaders().getFirst("X-Log-Line-Limit"));
    }

    @Test
    void appLogReturnsNotFoundWhenUnavailable() {
        TestFixture fixture = createFixture();
        when(fixture.applicationLogFileService().readTail(200)).thenReturn(ApplicationLogFileService.ApplicationLogTail.unavailable("log_file_missing"));
        MockHttpServletRequest request = new MockHttpServletRequest();

        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () -> fixture.controller().appLog(request, 200));

        verify(fixture.adminAccessGuard(), times(1)).validate(request);
        assertEquals(HttpStatus.NOT_FOUND, exception.getStatusCode());
    }

    private record TestFixture(
            AdminAccessGuard adminAccessGuard,
            JfrRecordingManager jfrRecordingManager,
            JfrBlockingReportService reportService,
            ApplicationLogFileService applicationLogFileService,
            InstrumentationProperties properties,
            InstrumentationAdminController controller
    ) {
    }
}
