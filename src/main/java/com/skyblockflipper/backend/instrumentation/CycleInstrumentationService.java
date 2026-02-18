package com.skyblockflipper.backend.instrumentation;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Component
@Slf4j
public class CycleInstrumentationService {

    private static final String METRIC_NAME = "skyblock.polling.phase";

    private final AtomicLong cycleCounter = new AtomicLong();
    private final MeterRegistry meterRegistry;
    private final BlockingTimeTracker blockingTimeTracker;

    public CycleInstrumentationService(MeterRegistry meterRegistry, BlockingTimeTracker blockingTimeTracker) {
        this.meterRegistry = meterRegistry;
        this.blockingTimeTracker = blockingTimeTracker;
    }

    public CycleContext startCycle() {
        long next = cycleCounter.incrementAndGet();
        String cycleId = next + "-" + Instant.now().toEpochMilli();
        CycleContext context = new CycleContext(cycleId, Instant.now());
        CycleContextHolder.set(context);
        return context;
    }

    public long startPhase() {
        return System.nanoTime();
    }

    public void endPhase(String phase, long phaseStartNanos, boolean success, long payloadBytes) {
        CycleContext context = CycleContextHolder.get();
        if (context == null) {
            return;
        }
        long phaseMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - phaseStartNanos);
        context.getPhaseMillis().put(phase, phaseMillis);
        context.setPayloadBytes(payloadBytes);
        Timer.builder(METRIC_NAME)
                .tag("phase", phase)
                .tag("cycleId", bucketCycle(context.getCycleId()))
                .tag("outcome", success ? "success" : "failure")
                .tag("payload_size_bucket", bucketPayload(payloadBytes))
                .register(meterRegistry)
                .record(phaseMillis, TimeUnit.MILLISECONDS);
    }

    public void finishCycle(boolean success) {
        CycleContext context = CycleContextHolder.get();
        if (context == null) {
            return;
        }
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("cycleId", context.getCycleId());
        summary.put("payloadBytes", context.getPayloadBytes());
        summary.put("totalCycleMillis", context.getPhaseMillis().getOrDefault("total_cycle", 0L));
        summary.put("perPhaseMillis", context.getPhaseMillis());
        summary.put("slowBlockingPoints", blockingTimeTracker.topBlockingPoints(context, 5));
        summary.put("outcome", success ? "success" : "failure");
        log.info("cycle_blocking_summary={}", summary);
        CycleContextHolder.clear();
    }

    private String bucketCycle(String cycleId) {
        int dash = cycleId.indexOf('-');
        if (dash <= 0) {
            return "0-99";
        }
        try {
            long counter = Long.parseLong(cycleId.substring(0, dash));
            long bucket = (counter / 100) * 100;
            return bucket + "-" + (bucket + 99);
        } catch (NumberFormatException ignored) {
            return "0-99";
        }
    }

    private String bucketPayload(long payloadBytes) {
        if (payloadBytes < 100_000) return "lt_100kb";
        if (payloadBytes < 1_000_000) return "100kb_1mb";
        if (payloadBytes < 10_000_000) return "1mb_10mb";
        return "gte_10mb";
    }
}
