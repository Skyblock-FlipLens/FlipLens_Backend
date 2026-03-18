package com.skyblockflipper.backend.instrumentation.actuator;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RequestTimingDiagnosticsEndpointTest {

    @Test
    @SuppressWarnings("unchecked")
    void requestTimingsReturnsNoDataMetadataWhenSnapshotMissing() {
        RequestTimingDiagnosticsService service = mock(RequestTimingDiagnosticsService.class);
        RequestTimingDiagnosticsProperties properties = new RequestTimingDiagnosticsProperties();
        RequestTimingDiagnosticsEndpoint endpoint = new RequestTimingDiagnosticsEndpoint(service, properties);

        Map<String, Object> response = (Map<String, Object>) endpoint.requestTimings(null);

        assertEquals("NO_DATA", response.get("status"));
        assertEquals(true, response.get("enabled"));
        assertEquals(true, response.get("fileOutputEnabled"));
        assertNull(response.get("latest"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void requestTimingsReturnsEnvelopeForLatestSnapshotWithoutHistory() {
        RequestTimingDiagnosticsService service = mock(RequestTimingDiagnosticsService.class);
        RequestTimingDiagnosticsProperties properties = new RequestTimingDiagnosticsProperties();
        RequestTimingDiagnosticsEndpoint endpoint = new RequestTimingDiagnosticsEndpoint(service, properties);
        RequestTimingDiagnosticsDto.Snapshot snapshot = new RequestTimingDiagnosticsDto.Snapshot(
                Instant.parse("2026-03-18T11:30:00Z"),
                250.0D,
                1,
                0,
                List.of(),
                List.of()
        );
        when(service.getLastSnapshot()).thenReturn(snapshot);

        Map<String, Object> response = (Map<String, Object>) endpoint.requestTimings(null);

        assertEquals("OK", response.get("status"));
        assertSame(snapshot, response.get("latest"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void requestTimingsReturnsLatestSnapshotAndHistoryWhenRequested() {
        RequestTimingDiagnosticsService service = mock(RequestTimingDiagnosticsService.class);
        RequestTimingDiagnosticsProperties properties = new RequestTimingDiagnosticsProperties();
        RequestTimingDiagnosticsEndpoint endpoint = new RequestTimingDiagnosticsEndpoint(service, properties);
        RequestTimingDiagnosticsDto.Snapshot snapshot = new RequestTimingDiagnosticsDto.Snapshot(
                Instant.parse("2026-03-18T11:30:00Z"),
                250.0D,
                1,
                1,
                List.of(),
                List.of()
        );
        List<RequestTimingDiagnosticsDto.Snapshot> history = List.of(snapshot);
        when(service.getLastSnapshot()).thenReturn(snapshot);
        when(service.readRecentSnapshots(5)).thenReturn(history);

        Map<String, Object> response = (Map<String, Object>) endpoint.requestTimings(5);

        assertEquals("OK", response.get("status"));
        assertEquals(5, response.get("historyLimit"));
        assertSame(snapshot, response.get("latest"));
        assertSame(history, response.get("history"));
    }
}
