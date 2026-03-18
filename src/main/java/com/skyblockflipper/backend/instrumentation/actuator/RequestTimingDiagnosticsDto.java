package com.skyblockflipper.backend.instrumentation.actuator;

import java.time.Instant;
import java.util.List;

public final class RequestTimingDiagnosticsDto {

    private RequestTimingDiagnosticsDto() {
    }

    public record Snapshot(Instant timestampUtc,
                           double readinessP95ThresholdMs,
                           int totalRoutes,
                           int routesOverReadinessThreshold,
                           List<RouteTiming> routes,
                           List<String> errors) {
    }

    public record RouteTiming(String route,
                              List<String> methods,
                              List<String> statuses,
                              List<String> outcomes,
                              long requestCount,
                              int timerSeriesCount,
                              double meanMs,
                              Double p95Ms,
                              Double p99Ms,
                              double maxMs,
                              boolean exceedsReadinessP95Threshold) {
    }
}
