package com.skyblockflipper.backend.instrumentation;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import jdk.jfr.Configuration;
import jdk.jfr.Recording;
import jdk.jfr.RecordingState;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

@Component
@Slf4j
@RequiredArgsConstructor
public class JfrRecordingManager {

    private Recording continuousRecording;
    private Recording snapshotRingRecording;
    private final InstrumentationProperties properties;


    @PostConstruct
    public synchronized void start() {
        if (!properties.getJfr().isEnabled()) {
            return;
        }
        try {
            System.setProperty("jdk.jfr.stackdepth", Integer.toString(properties.getJfr().getStackDepth()));
            Path outputDir = ensureOutputDir();
            Configuration profile = Configuration.getConfiguration("profile");

            Recording initializedContinuousRecording = getRecording(profile, outputDir);
            configureBlockingEvents(initializedContinuousRecording);
            initializedContinuousRecording.start();

            Recording initializedSnapshotRingRecording = getRecording(profile);
            configureBlockingEvents(initializedSnapshotRingRecording);
            initializedSnapshotRingRecording.start();

            continuousRecording = initializedContinuousRecording;
            snapshotRingRecording = initializedSnapshotRingRecording;

            log.info("JFR started. dir={} retention={} snapshotWindow={}",
                    outputDir,
                    properties.getJfr().getRetention(),
                    properties.getJfr().getSnapshotWindow());
        } catch (Exception exception) {
            log.warn("Failed to start JFR instrumentation: {}", exception.getMessage(), exception);
        }
    }

    private Recording getRecording(Configuration profile) {
        Recording initializedSnapshotRingRecording = new Recording(profile);
        initializedSnapshotRingRecording.setName("skyblock-blocking-snapshot-ring");
        initializedSnapshotRingRecording.setToDisk(true);
        initializedSnapshotRingRecording.setDumpOnExit(false);
        initializedSnapshotRingRecording.setMaxAge(properties.getJfr().getSnapshotWindow());
        initializedSnapshotRingRecording.setMaxSize(Math.max(32L, properties.getJfr().getMaxSizeMb() / 4) * 1024L * 1024L);
        return initializedSnapshotRingRecording;
    }

    private Recording getRecording(Configuration profile, Path outputDir) throws IOException {
        Recording initializedContinuousRecording = new Recording(profile);
        initializedContinuousRecording.setName("skyblock-blocking-continuous");
        initializedContinuousRecording.setToDisk(true);
        initializedContinuousRecording.setDumpOnExit(true);
        initializedContinuousRecording.setDestination(outputDir.resolve("continuous.jfr"));
        initializedContinuousRecording.setMaxAge(properties.getJfr().getRetention());
        initializedContinuousRecording.setMaxSize(properties.getJfr().getMaxSizeMb() * 1024L * 1024L);
        return initializedContinuousRecording;
    }

    public synchronized Path dumpSnapshot() {
        if (snapshotRingRecording == null) {
            throw new IllegalStateException("JFR snapshot recording is not running");
        }
        try {
            Path outputDir = ensureOutputDir();
            Path output = outputDir.resolve("snapshot-" + Instant.now().toEpochMilli() + ".jfr");
            try (Recording copy = snapshotRingRecording.copy(true)) {
                copy.dump(output);
            }
            return output;
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to dump JFR snapshot", exception);
        }
    }

    public synchronized Path latestRecordingFile() {
        try {
            Path outputDir = ensureOutputDir();
            List<Path> files;
            try (Stream<Path> stream = Files.list(outputDir)) {
                files = stream
                        .filter(path -> path.getFileName().toString().endsWith(".jfr"))
                        .sorted(Comparator.comparingLong((Path path) -> path.toFile().lastModified()).reversed())
                        .toList();
            }
            return files.isEmpty() ? null : files.getFirst();
        } catch (IOException exception) {
            return null;
        }
    }

    @Scheduled(fixedDelayString = "PT10M")
    public void cleanupOldRecordings() {
        if (!properties.getJfr().isEnabled()) {
            return;
        }
        try {
            Path outputDir = ensureOutputDir();
            Instant threshold = Instant.now().minus(properties.getJfr().getRetention());
            try (Stream<Path> stream = Files.list(outputDir)) {
                stream
                        .filter(path -> path.getFileName().toString().endsWith(".jfr"))
                        .filter(path -> path.toFile().lastModified() < threshold.toEpochMilli())
                        .forEach(path -> {
                            try {
                                Files.deleteIfExists(path);
                            } catch (IOException ignored) {
                            }
                        });
            }
        } catch (IOException ignored) {
        }
    }

    private void configureBlockingEvents(Recording recording) {
        recording.enable("jdk.JavaMonitorBlocked").withStackTrace();
        recording.enable("jdk.ThreadPark").withStackTrace();
        recording.enable("jdk.JavaMonitorWait").withStackTrace();
        recording.enable("jdk.SocketRead").withStackTrace();
        recording.enable("jdk.SocketWrite").withStackTrace();
        recording.enable("jdk.FileRead").withStackTrace();
        recording.enable("jdk.FileWrite").withStackTrace();
        recording.enable("jdk.GarbageCollection").withStackTrace();
        recording.enable("jdk.CPULoad");
        recording.enable("jdk.ExecutionSample")
                .withStackTrace()
                .withPeriod(properties.getJfr().getExecutionSamplePeriod());
    }

    private Path ensureOutputDir() throws IOException {
        Path outputDir = properties.getJfr().getOutputDir();
        Files.createDirectories(outputDir);
        return outputDir;
    }

    @PreDestroy
    public synchronized void stop() {
        stopAndClose(continuousRecording);
        stopAndClose(snapshotRingRecording);
    }

    private void stopAndClose(Recording recording) {
        if (recording == null) {
            return;
        }
        try {
            if (recording.getState() == RecordingState.RUNNING) {
                recording.stop();
            }
        } catch (IllegalStateException ignored) {
        }

        try {
            if (recording.getState() != RecordingState.CLOSED) {
                recording.close();
            }
        } catch (IllegalStateException ignored) {
        }
    }
}
