package com.skyblockflipper.backend.instrumentation.actuator;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CompactionReadinessEndpointTest {

    @Test
    void readinessIsOkWhenLoadSignalsAreLow() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        registerGauge(registry, "process.cpu.usage", 0.20D);
        registerGauge(registry, "system.cpu.usage", 0.30D);
        registerGauge(registry, "tomcat.threads.busy", 5D);
        registerGauge(registry, "tomcat.threads.config.max", 200D);
        registerGauge(registry, "hikaricp.connections.active", 3D);
        registerGauge(registry, "hikaricp.connections.idle", 7D);
        registerGauge(registry, "hikaricp.connections.pending", 0D);
        Timer.builder("http.server.requests").register(registry).record(Duration.ofMillis(120));

        CompactionReadinessProperties properties = new CompactionReadinessProperties();
        CompactionReadinessEndpoint endpoint = new CompactionReadinessEndpoint(registry, properties);

        Map<String, Object> response = endpoint.readiness();

        assertEquals(true, response.get("ok"));
        assertEquals("low_load", response.get("reason"));
    }

    @Test
    void readinessReturnsBlockersWhenThresholdsExceeded() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        registerGauge(registry, "process.cpu.usage", 0.95D);
        registerGauge(registry, "tomcat.threads.busy", 180D);
        registerGauge(registry, "tomcat.threads.config.max", 200D);
        registerGauge(registry, "hikaricp.connections.pending", 3D);
        Timer.builder("http.server.requests").register(registry).record(Duration.ofMillis(800));

        CompactionReadinessProperties properties = new CompactionReadinessProperties();
        properties.getApi().setMaxP95HttpMs(250D);
        properties.getApi().setMaxProcessCpu(0.70D);
        properties.getApi().setMaxTomcatBusyRatio(0.70D);
        properties.getDbPool().setMaxPending(0D);
        CompactionReadinessEndpoint endpoint = new CompactionReadinessEndpoint(registry, properties);

        Map<String, Object> response = endpoint.readiness();

        assertEquals(false, response.get("ok"));
        String reason = (String) response.get("reason");
        assertTrue(reason.contains("http_p95_high"));
        assertTrue(reason.contains("process_cpu_high"));
        assertTrue(reason.contains("tomcat_busy_high"));
        assertTrue(reason.contains("db_pool_pending"));
    }

    @Test
    void readinessReturnsDisabledReasonWhenFeatureOff() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        CompactionReadinessProperties properties = new CompactionReadinessProperties();
        properties.setEnabled(false);
        CompactionReadinessEndpoint endpoint = new CompactionReadinessEndpoint(registry, properties);

        Map<String, Object> response = endpoint.readiness();

        assertEquals(false, response.get("ok"));
        assertEquals("readiness_disabled", response.get("reason"));
    }

    private void registerGauge(SimpleMeterRegistry registry, String name, double value) {
        AtomicReference<Double> holder = new AtomicReference<>(value);
        Gauge.builder(name, holder, AtomicReference::get).register(registry);
    }
}
