package com.skyblockflipper.backend.instrumentation;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class CycleContext {
    private final String cycleId;
    private final Instant startedAt;
    private final Map<String, Long> phaseMillis = new LinkedHashMap<>();
    private final List<BlockingTimeTracker.BlockingPoint> blockingPoints = new ArrayList<>();
    private long payloadBytes;

    public CycleContext(String cycleId, Instant startedAt) {
        this.cycleId = cycleId;
        this.startedAt = startedAt;
    }

    public String getCycleId() { return cycleId; }
    public Instant getStartedAt() { return startedAt; }
    public Map<String, Long> getPhaseMillis() { return phaseMillis; }
    public List<BlockingTimeTracker.BlockingPoint> getBlockingPoints() { return blockingPoints; }
    public long getPayloadBytes() { return payloadBytes; }
    public void setPayloadBytes(long payloadBytes) { this.payloadBytes = payloadBytes; }
}
