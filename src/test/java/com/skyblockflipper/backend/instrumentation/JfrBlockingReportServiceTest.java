package com.skyblockflipper.backend.instrumentation;

import jdk.jfr.Recording;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JfrBlockingReportServiceTest {

    @Test
    void summarizeReturnsErrorWhenRecordingFileIsMissing() {
        InstrumentationProperties properties = new InstrumentationProperties();
        JfrBlockingReportService service = new JfrBlockingReportService(properties);

        Map<String, Object> result = service.summarize(null);

        assertEquals("No JFR recording file available", result.get("error"));
    }

    @Test
    void summarizeReturnsErrorWhenRecordingCannotBeRead() {
        InstrumentationProperties properties = new InstrumentationProperties();
        JfrBlockingReportService service = new JfrBlockingReportService(properties);

        Map<String, Object> result = service.summarize(Path.of("does-not-exist", "missing.jfr"));

        assertTrue(String.valueOf(result.get("error")).startsWith("Failed to parse JFR:"));
    }

    @Test
    void summarizeParsesEmptyRecordingAndReturnsCoreMetrics() throws Exception {
        InstrumentationProperties properties = new InstrumentationProperties();
        JfrBlockingReportService service = new JfrBlockingReportService(properties);
        Path file = Files.createTempFile("blocking-report", ".jfr");
        try (Recording recording = new Recording()) {
            recording.start();
            recording.stop();
            recording.dump(file);
        }

        Map<String, Object> result = service.summarize(file);

        assertEquals(file.toString(), result.get("recordingFile"));
        assertTrue(result.containsKey("topBlockingStacks"));
        assertTrue(result.containsKey("topIoWaitStacks"));
        assertTrue(result.containsKey("blockedMillis"));
        assertTrue(result.containsKey("cpuMillis"));
        assertTrue(result.containsKey("blockedToCpuRatio"));
    }
}
