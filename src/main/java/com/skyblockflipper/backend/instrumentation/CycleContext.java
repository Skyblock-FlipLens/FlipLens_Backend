package com.skyblockflipper.backend.instrumentation;

import lombok.Getter;
import lombok.Setter;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Getter
public class CycleContext {
    private final String cycleId;
    private final Instant startedAt;
    private final Map<String, Long> phaseMillis = new LinkedHashMap<>();
    private final List<BlockingTimeTracker.BlockingPoint> blockingPoints = new ArrayList<>();
    @Setter
    private long payloadBytes;

    public CycleContext(String cycleId, Instant startedAt) {
        this.cycleId = cycleId;
        this.startedAt = startedAt;
    }

}
