package com.skyblockflipper.backend.instrumentation.actuator;

import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RequestTimingDiagnosticsServiceTest {

    @Test
    void collectSnapshotAggregatesRoutesAndFlagsThresholdBreaches() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        Timer.builder("http.server.requests")
                .tag("uri", "/items")
                .tag("method", "GET")
                .tag("status", "200")
                .tag("outcome", "SUCCESS")
                .register(registry)
                .record(Duration.ofMillis(6_800));
        Timer.builder("http.server.requests")
                .tag("uri", "/status")
                .tag("method", "GET")
                .tag("status", "200")
                .tag("outcome", "SUCCESS")
                .register(registry)
                .record(Duration.ofMillis(5));

        RequestTimingDiagnosticsProperties properties = new RequestTimingDiagnosticsProperties();
        properties.getOutput().setEnabled(false);
        CompactionReadinessProperties readinessProperties = new CompactionReadinessProperties();
        readinessProperties.getApi().setMaxP95HttpMs(250D);

        RequestTimingDiagnosticsService service = new RequestTimingDiagnosticsService(
                registry,
                properties,
                readinessProperties,
                new ObjectMapper()
        );

        RequestTimingDiagnosticsDto.Snapshot snapshot = service.collectSnapshot();

        assertEquals(2, snapshot.totalRoutes());
        assertEquals(1, snapshot.routesOverReadinessThreshold());
        assertEquals("/items", snapshot.routes().get(0).route());
        assertTrue(snapshot.routes().get(0).exceedsReadinessP95Threshold());
        assertFalse(snapshot.routes().get(1).exceedsReadinessP95Threshold());
    }

    @Test
    void readRecentSnapshotsReturnsNewestJsonlEntries() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RequestTimingDiagnosticsProperties properties = new RequestTimingDiagnosticsProperties();
        ObjectMapper objectMapper = new ObjectMapper();
        Path tempFile = Files.createTempDirectory("request-timing-diagnostics-test")
                .resolve("nested")
                .resolve("request-timings.jsonl");
        properties.getOutput().setEnabled(true);
        properties.getOutput().setFile(tempFile);
        properties.setHistoryReadLimitMax(2);

        RequestTimingDiagnosticsService service = new RequestTimingDiagnosticsService(
                registry,
                properties,
                new CompactionReadinessProperties(),
                objectMapper
        );

        List<RequestTimingDiagnosticsDto.Snapshot> storedSnapshots = List.of(
                new RequestTimingDiagnosticsDto.Snapshot(
                        Instant.parse("2026-03-18T11:00:00Z"),
                        250.0D,
                        1,
                        1,
                        List.of(),
                        List.of()
                ),
                new RequestTimingDiagnosticsDto.Snapshot(
                        Instant.parse("2026-03-18T11:01:00Z"),
                        250.0D,
                        2,
                        1,
                        List.of(),
                        List.of()
                ),
                new RequestTimingDiagnosticsDto.Snapshot(
                        Instant.parse("2026-03-18T11:02:00Z"),
                        250.0D,
                        3,
                        2,
                        List.of(),
                        List.of()
                )
        );
        Files.createDirectories(tempFile.getParent());
        StringBuilder content = new StringBuilder();
        for (RequestTimingDiagnosticsDto.Snapshot storedSnapshot : storedSnapshots) {
            content.append(objectMapper.writeValueAsString(storedSnapshot)).append(System.lineSeparator());
        }
        Files.writeString(tempFile, content.toString(), StandardCharsets.UTF_8);

        List<RequestTimingDiagnosticsDto.Snapshot> history = service.readRecentSnapshots(10);

        assertEquals(2, history.size());
        assertEquals(Instant.parse("2026-03-18T11:01:00Z"), history.get(0).timestampUtc());
        assertEquals(Instant.parse("2026-03-18T11:02:00Z"), history.get(1).timestampUtc());
    }

    @Test
    void readRecentSnapshotsSkipsMalformedJsonlLines() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RequestTimingDiagnosticsProperties properties = new RequestTimingDiagnosticsProperties();
        ObjectMapper objectMapper = new ObjectMapper();
        Path tempFile = Files.createTempDirectory("request-timing-diagnostics-malformed-test")
                .resolve("nested")
                .resolve("request-timings.jsonl");
        properties.getOutput().setEnabled(true);
        properties.getOutput().setFile(tempFile);
        properties.setHistoryReadLimitMax(5);

        RequestTimingDiagnosticsService service = new RequestTimingDiagnosticsService(
                registry,
                properties,
                new CompactionReadinessProperties(),
                objectMapper
        );

        RequestTimingDiagnosticsDto.Snapshot firstSnapshot = new RequestTimingDiagnosticsDto.Snapshot(
                Instant.parse("2026-03-18T11:00:00Z"),
                250.0D,
                1,
                0,
                List.of(),
                List.of()
        );
        RequestTimingDiagnosticsDto.Snapshot secondSnapshot = new RequestTimingDiagnosticsDto.Snapshot(
                Instant.parse("2026-03-18T11:02:00Z"),
                250.0D,
                3,
                1,
                List.of(),
                List.of()
        );
        Files.createDirectories(tempFile.getParent());
        String content = objectMapper.writeValueAsString(firstSnapshot)
                + System.lineSeparator()
                + "{not-json"
                + System.lineSeparator()
                + objectMapper.writeValueAsString(secondSnapshot)
                + System.lineSeparator();
        Files.writeString(tempFile, content, StandardCharsets.UTF_8);

        List<RequestTimingDiagnosticsDto.Snapshot> history = service.readRecentSnapshots(10);

        assertEquals(2, history.size());
        assertEquals(Instant.parse("2026-03-18T11:00:00Z"), history.get(0).timestampUtc());
        assertEquals(Instant.parse("2026-03-18T11:02:00Z"), history.get(1).timestampUtc());
    }
}
