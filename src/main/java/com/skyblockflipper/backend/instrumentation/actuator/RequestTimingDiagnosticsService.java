package com.skyblockflipper.backend.instrumentation.actuator;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Component
@Profile("!compactor")
@RequiredArgsConstructor
@Slf4j
public class RequestTimingDiagnosticsService implements SmartLifecycle {

    private final MeterRegistry meterRegistry;
    private final RequestTimingDiagnosticsProperties properties;
    private final CompactionReadinessProperties readinessProperties;
    private final ObjectMapper objectMapper;
    private final AtomicReference<RequestTimingDiagnosticsDto.Snapshot> lastSnapshot = new AtomicReference<>();

    private volatile boolean running;
    private ScheduledExecutorService scheduler;

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        if (!properties.isEnabled()) {
            log.info("Request timing diagnostics disabled");
            return;
        }
        running = true;
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "request-timing-diagnostics");
            thread.setDaemon(true);
            return thread;
        });
        long intervalMillis = Math.max(1_000L, properties.getInterval().toMillis());
        scheduler.scheduleWithFixedDelay(this::runCollectionSafely, 0L, intervalMillis, TimeUnit.MILLISECONDS);
        log.info("Request timing diagnostics started (intervalMs={}, fileOutputEnabled={})",
                intervalMillis,
                properties.getOutput().isEnabled());
    }

    @Override
    public synchronized void stop() {
        running = false;
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
        log.info("Request timing diagnostics stopped");
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 20;
    }

    public RequestTimingDiagnosticsDto.Snapshot getLastSnapshot() {
        return lastSnapshot.get();
    }

    public List<RequestTimingDiagnosticsDto.Snapshot> readRecentSnapshots(int limit) {
        Path file = properties.getOutput().getFile();
        if (!properties.getOutput().isEnabled() || file == null || !Files.exists(file)) {
            return List.of();
        }
        int sanitizedLimit = sanitizeHistoryLimit(limit);
        try {
            List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
            int fromIndex = Math.max(0, lines.size() - sanitizedLimit);
            List<RequestTimingDiagnosticsDto.Snapshot> snapshots = new ArrayList<>();
            for (String line : lines.subList(fromIndex, lines.size())) {
                String trimmed = line == null ? "" : line.trim();
                if (trimmed.isEmpty()) {
                    continue;
                }
                snapshots.add(objectMapper.readValue(trimmed, RequestTimingDiagnosticsDto.Snapshot.class));
            }
            return snapshots;
        } catch (Exception exception) {
            log.warn("Failed to read request timing history from {}: {}", file, summarize(exception), exception);
            return List.of();
        }
    }

    public Path getOutputFile() {
        return properties.getOutput().getFile();
    }

    RequestTimingDiagnosticsDto.Snapshot collectSnapshot() {
        Instant now = Instant.now();
        List<String> errors = new ArrayList<>();
        double readinessThresholdMs = readinessProperties.getApi().getMaxP95HttpMs();

        Collection<Timer> timers = meterRegistry.find("http.server.requests").timers();
        LinkedHashMap<String, RouteAccumulator> byRoute = new LinkedHashMap<>();
        for (Timer timer : timers) {
            try {
                String route = normalizeTag(timer.getId().getTag("uri"), "UNKNOWN");
                RouteAccumulator accumulator = byRoute.computeIfAbsent(route, RouteAccumulator::new);
                accumulator.seriesCount++;
                accumulator.methods.add(normalizeTag(timer.getId().getTag("method"), "UNKNOWN"));
                accumulator.statuses.add(normalizeTag(timer.getId().getTag("status"), "UNKNOWN"));
                accumulator.outcomes.add(normalizeTag(timer.getId().getTag("outcome"), "UNKNOWN"));

                long count = timer.count();
                accumulator.requestCount += count;

                double meanMs = timer.mean(TimeUnit.MILLISECONDS);
                if (Double.isFinite(meanMs) && count > 0L) {
                    accumulator.weightedMeanMs += meanMs * count;
                }

                double maxMs = timer.max(TimeUnit.MILLISECONDS);
                if (Double.isFinite(maxMs)) {
                    accumulator.maxMs = Math.max(accumulator.maxMs, maxMs);
                }

                ValueAtPercentile[] percentiles = timer.takeSnapshot().percentileValues();
                accumulator.p95Ms = maxNullable(accumulator.p95Ms, readPercentileMillis(percentiles, 0.95D, meanMs));
                accumulator.p99Ms = maxNullable(accumulator.p99Ms, readPercentileMillis(percentiles, 0.99D, meanMs));
            } catch (Exception exception) {
                errors.add("timer_read_failed:" + summarize(exception));
            }
        }

        List<RequestTimingDiagnosticsDto.RouteTiming> routes = byRoute.values().stream()
                .map(accumulator -> accumulator.toRouteTiming(readinessThresholdMs))
                .sorted(Comparator
                        .comparing((RequestTimingDiagnosticsDto.RouteTiming route) -> nullSafe(route.p95Ms()))
                        .reversed()
                        .thenComparing(RequestTimingDiagnosticsDto.RouteTiming::meanMs, Comparator.reverseOrder())
                        .thenComparing(RequestTimingDiagnosticsDto.RouteTiming::maxMs, Comparator.reverseOrder())
                        .thenComparing(RequestTimingDiagnosticsDto.RouteTiming::route))
                .toList();

        int routesOverThreshold = (int) routes.stream()
                .filter(RequestTimingDiagnosticsDto.RouteTiming::exceedsReadinessP95Threshold)
                .count();

        return new RequestTimingDiagnosticsDto.Snapshot(
                now,
                round(readinessThresholdMs),
                routes.size(),
                routesOverThreshold,
                routes,
                errors
        );
    }

    private void runCollectionSafely() {
        if (!running || !properties.isEnabled()) {
            return;
        }
        try {
            RequestTimingDiagnosticsDto.Snapshot snapshot = collectSnapshot();
            lastSnapshot.set(snapshot);
            String serialized = objectMapper.writeValueAsString(snapshot);
            log.info("request_timing_summary={}", serialized);
            writeSnapshotToFile(serialized);
        } catch (Exception exception) {
            log.warn("Request timing diagnostics collection failed: {}", summarize(exception), exception);
        }
    }

    private void writeSnapshotToFile(String serializedSnapshot) {
        if (!properties.getOutput().isEnabled()) {
            return;
        }
        Path file = properties.getOutput().getFile();
        if (file == null) {
            log.warn("Request timing diagnostics file output enabled but no file path configured");
            return;
        }
        try {
            Path parent = file.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Files.writeString(
                    file,
                    serializedSnapshot + System.lineSeparator(),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND
            );
        } catch (Exception exception) {
            log.warn("Failed to append request timing diagnostics snapshot to {}: {}",
                    file,
                    summarize(exception),
                    exception);
        }
    }

    private int sanitizeHistoryLimit(int limit) {
        int configuredMax = Math.max(1, properties.getHistoryReadLimitMax());
        return Math.min(configuredMax, Math.max(1, limit));
    }

    private String normalizeTag(String value, String fallback) {
        return value == null || value.isBlank() ? fallback : value;
    }

    private Double readPercentileMillis(ValueAtPercentile[] percentiles, double percentile, double fallbackMeanMs) {
        if (percentiles != null) {
            for (ValueAtPercentile value : percentiles) {
                if (Math.abs(value.percentile() - percentile) < 0.0001D) {
                    double measured = value.value(TimeUnit.MILLISECONDS);
                    return Double.isFinite(measured) ? round(measured) : null;
                }
            }
        }
        return Double.isFinite(fallbackMeanMs) ? round(fallbackMeanMs) : null;
    }

    private Double maxNullable(Double left, Double right) {
        if (left == null) {
            return right;
        }
        if (right == null) {
            return left;
        }
        return Math.max(left, right);
    }

    private double nullSafe(Double value) {
        return value == null ? Double.NEGATIVE_INFINITY : value;
    }

    private double round(double value) {
        return Math.round(value * 100.0D) / 100.0D;
    }

    private String summarize(Exception exception) {
        String message = exception.getMessage();
        if (message == null || message.isBlank()) {
            return exception.getClass().getSimpleName();
        }
        return message.length() > 160 ? message.substring(0, 160) : message;
    }

    private final class RouteAccumulator {
        private final String route;
        private final Set<String> methods = new TreeSet<>();
        private final Set<String> statuses = new TreeSet<>();
        private final Set<String> outcomes = new TreeSet<>();
        private long requestCount;
        private int seriesCount;
        private double weightedMeanMs;
        private double maxMs;
        private Double p95Ms;
        private Double p99Ms;

        private RouteAccumulator(String route) {
            this.route = route;
        }

        private RequestTimingDiagnosticsDto.RouteTiming toRouteTiming(double readinessThresholdMs) {
            double meanMs = requestCount <= 0L ? 0.0D : weightedMeanMs / requestCount;
            Double effectiveP95 = p95Ms != null ? p95Ms : (requestCount > 0L ? round(meanMs) : null);
            Double effectiveP99 = p99Ms != null ? p99Ms : (requestCount > 0L ? round(meanMs) : null);
            return new RequestTimingDiagnosticsDto.RouteTiming(
                    route,
                    List.copyOf(methods),
                    List.copyOf(statuses),
                    List.copyOf(outcomes),
                    requestCount,
                    seriesCount,
                    round(meanMs),
                    effectiveP95,
                    effectiveP99,
                    round(maxMs),
                    effectiveP95 != null && effectiveP95 > readinessThresholdMs
            );
        }
    }
}
