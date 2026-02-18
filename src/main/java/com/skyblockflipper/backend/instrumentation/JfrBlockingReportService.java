package com.skyblockflipper.backend.instrumentation;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordedStackTrace;
import jdk.jfr.consumer.RecordingFile;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class JfrBlockingReportService {

    private static final int STACK_DEPTH = 12;

    private final InstrumentationProperties properties;


    public Map<String, Object> summarize(Path recordingFile) {
        if (recordingFile == null) {
            return Map.of("error", "No JFR recording file available");
        }
        Map<String, Long> blockingStacks = new LinkedHashMap<>();
        Map<String, Long> ioStacks = new LinkedHashMap<>();
        long blockedNanos = 0L;
        long executionSampleCount = 0L;

        try (RecordingFile file = new RecordingFile(recordingFile)) {
            while (file.hasMoreEvents()) {
                RecordedEvent event = file.readEvent();
                String eventName = event.getEventType().getName();
                long durationNanos = durationNanos(event);
                switch (eventName) {
                    case "jdk.ThreadPark", "jdk.JavaMonitorBlocked", "jdk.JavaMonitorWait" -> {
                        blockedNanos += durationNanos;
                        blockingStacks.merge(stackKey(event.getStackTrace()), durationNanos, Long::sum);
                    }
                    case "jdk.SocketRead", "jdk.SocketWrite", "jdk.FileRead", "jdk.FileWrite" ->
                            ioStacks.merge(stackKey(event.getStackTrace()), durationNanos, Long::sum);
                    case "jdk.ExecutionSample" -> executionSampleCount++;
                    default -> {
                    }
                }
            }
        } catch (IOException exception) {
            return Map.of("error", "Failed to parse JFR: " + exception.getMessage());
        }

        long samplingPeriodNanos = properties.getJfr().getExecutionSamplePeriod().toNanos();
        long cpuNanos = executionSampleCount * samplingPeriodNanos;

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("recordingFile", recordingFile.toString());
        result.put("topBlockingStacks", topN(blockingStacks, 10));
        result.put("topIoWaitStacks", topN(ioStacks, 10));
        result.put("blockedMillis", blockedNanos / 1_000_000L);
        result.put("cpuMillis", cpuNanos / 1_000_000L);
        result.put("blockedToCpuRatio", cpuNanos == 0L ? "n/a" : (double) blockedNanos / (double) cpuNanos);
        return result;
    }

    private long durationNanos(RecordedEvent event) {
        Duration duration = event.getDuration();
        return duration == null ? 0L : duration.toNanos();
    }

    private String stackKey(RecordedStackTrace stackTrace) {
        if (stackTrace == null || stackTrace.getFrames().isEmpty()) {
            return "<no-stack>";
        }
        return stackTrace.getFrames().stream()
                .limit(STACK_DEPTH)
                .map(frame -> frame.getMethod().getType().getName() + "." + frame.getMethod().getName())
                .reduce((left, right) -> left + " <- " + right)
                .orElse("<unknown>");
    }

    private Map<String, Long> topN(Map<String, Long> source, int n) {
        return source.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .limit(n)
                .collect(LinkedHashMap::new, (map, entry) -> map.put(entry.getKey(), entry.getValue() / 1_000_000L), Map::putAll);
    }
}
