package com.skyblockflipper.backend.instrumentation.actuator;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Component
@Endpoint(id = "compactionReadiness")
@Profile("!compactor")
@RequiredArgsConstructor
public class CompactionReadinessEndpoint {

    private static final Duration READINESS_CACHE_TTL = Duration.ofSeconds(1);

    private final io.micrometer.core.instrument.MeterRegistry meterRegistry;
    private final CompactionReadinessProperties properties;
    private final Object readinessCacheLock = new Object();
    private volatile CachedReadiness cachedReadiness;

    @ReadOperation
    public Map<String, Object> readiness() {
        Instant now = Instant.now();
        CachedReadiness localCache = cachedReadiness;
        if (localCache != null && !localCache.isExpired(now)) {
            return localCache.response();
        }
        synchronized (readinessCacheLock) {
            localCache = cachedReadiness;
            if (localCache != null && !localCache.isExpired(now)) {
                return localCache.response();
            }
            Map<String, Object> response = buildReadinessResponse(now);
            cachedReadiness = new CachedReadiness(now, response);
            return response;
        }
    }

    private Map<String, Object> buildReadinessResponse(Instant now) {
        Double processCpu = readGauge("process.cpu.usage");
        Double systemCpu = readGauge("system.cpu.usage");
        Double p95HttpMs = readHttpP95Millis();
        Double tomcatBusy = sumGauge("tomcat.threads.busy");
        Double tomcatMax = sumGauge("tomcat.threads.config.max");
        Double tomcatBusyRatio = ratio(tomcatBusy, tomcatMax);
        Double dbActive = sumGauge("hikaricp.connections.active");
        Double dbIdle = sumGauge("hikaricp.connections.idle");
        Double dbPending = sumGauge("hikaricp.connections.pending");
        Double pipelineQueueSize = sumGauge("pipeline.queue.size");
        Double pipelineLastCycleMs = readGauge("pipeline.lastCycleMs");

        List<String> blockers = new ArrayList<>();
        if (properties.isEnabled()) {
            if (p95HttpMs != null && p95HttpMs > properties.getApi().getMaxP95HttpMs()) {
                blockers.add("http_p95_high");
            }
            if (processCpu != null && processCpu > properties.getApi().getMaxProcessCpu()) {
                blockers.add("process_cpu_high");
            }
            if (tomcatBusyRatio != null && tomcatBusyRatio > properties.getApi().getMaxTomcatBusyRatio()) {
                blockers.add("tomcat_busy_high");
            }
            if (dbPending != null && dbPending > properties.getDbPool().getMaxPending()) {
                blockers.add("db_pool_pending");
            }
            if (pipelineQueueSize != null && pipelineQueueSize > properties.getPipeline().getMaxQueueSize()) {
                blockers.add("pipeline_queue_high");
            }
        }

        boolean ok = properties.isEnabled() && blockers.isEmpty();
        String reason = !properties.isEnabled()
                ? "readiness_disabled"
                : ok ? "low_load" : String.join(",", blockers);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("ok", ok);
        response.put("reason", reason);
        response.put("timestampUtc", now.toString());
        response.put("api", apiMap(processCpu, systemCpu, p95HttpMs, tomcatBusy, tomcatMax, tomcatBusyRatio));
        response.put("dbPool", dbPoolMap(dbActive, dbIdle, dbPending));
        response.put("pipeline", pipelineMap(pipelineLastCycleMs, pipelineQueueSize));
        return response;
    }

    private Map<String, Object> apiMap(Double processCpu,
                                       Double systemCpu,
                                       Double p95HttpMs,
                                       Double tomcatBusy,
                                       Double tomcatMax,
                                       Double tomcatBusyRatio) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("cpuProcess", processCpu);
        map.put("cpuSystem", systemCpu);
        map.put("p95HttpMs", p95HttpMs);
        map.put("tomcatBusy", tomcatBusy);
        map.put("tomcatMax", tomcatMax);
        map.put("tomcatBusyRatio", tomcatBusyRatio);
        return map;
    }

    private Map<String, Object> dbPoolMap(Double active, Double idle, Double pending) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("active", active);
        map.put("idle", idle);
        map.put("pending", pending);
        return map;
    }

    private Map<String, Object> pipelineMap(Double lastCycleMs, Double queueSize) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("lastCycleMs", lastCycleMs);
        map.put("queueSize", queueSize);
        return map;
    }

    private Double readGauge(String meterName) {
        Gauge gauge = meterRegistry.find(meterName).gauge();
        if (gauge == null) {
            return null;
        }
        double value = gauge.value();
        return Double.isFinite(value) ? value : null;
    }

    private Double sumGauge(String meterName) {
        Collection<Gauge> gauges = meterRegistry.find(meterName).gauges();
        if (gauges == null || gauges.isEmpty()) {
            return null;
        }
        double sum = 0.0D;
        boolean seen = false;
        for (Gauge gauge : gauges) {
            if (gauge == null) {
                continue;
            }
            double value = gauge.value();
            if (!Double.isFinite(value)) {
                continue;
            }
            sum += value;
            seen = true;
        }
        return seen ? sum : null;
    }

    private Double ratio(Double numerator, Double denominator) {
        if (numerator == null || denominator == null || denominator <= 0.0D) {
            return null;
        }
        return numerator / denominator;
    }

    private Double readHttpP95Millis() {
        Collection<Timer> timers = meterRegistry.find("http.server.requests").timers();
        if (timers == null || timers.isEmpty()) {
            return null;
        }

        double maxP95Millis = Double.NEGATIVE_INFINITY;
        boolean seenPercentile = false;
        for (Timer timer : timers) {
            if (timer == null) {
                continue;
            }
            ValueAtPercentile[] percentiles = timer.takeSnapshot().percentileValues();
            if (percentiles == null || percentiles.length == 0) {
                continue;
            }
            for (ValueAtPercentile percentile : percentiles) {
                if (Math.abs(percentile.percentile() - 0.95D) < 0.0001D) {
                    maxP95Millis = Math.max(maxP95Millis, percentile.value(TimeUnit.MILLISECONDS));
                    seenPercentile = true;
                }
            }
        }
        if (seenPercentile && Double.isFinite(maxP95Millis)) {
            return maxP95Millis;
        }

        // Fallback when percentile histograms are not available.
        double maxMeanMillis = Double.NEGATIVE_INFINITY;
        boolean seenMean = false;
        for (Timer timer : timers) {
            if (timer == null || timer.count() <= 0L) {
                continue;
            }
            maxMeanMillis = Math.max(maxMeanMillis, timer.mean(TimeUnit.MILLISECONDS));
            seenMean = true;
        }
        return seenMean && Double.isFinite(maxMeanMillis) ? maxMeanMillis : null;
    }

    private record CachedReadiness(Instant computedAt, Map<String, Object> response) {
        private boolean isExpired(Instant now) {
            return computedAt.plus(READINESS_CACHE_TTL).isBefore(now);
        }
    }
}
