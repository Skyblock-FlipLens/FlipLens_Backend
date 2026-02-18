package com.skyblockflipper.backend.instrumentation;

import org.springframework.boot.context.properties.ConfigurationProperties;

import jakarta.annotation.PostConstruct;

import java.nio.file.Path;
import java.time.Duration;

@ConfigurationProperties(prefix = "instrumentation")
public class InstrumentationProperties {

    private final Jfr jfr = new Jfr();
    private final Blocking blocking = new Blocking();
    private final Admin admin = new Admin();
    private final AsyncProfiler asyncProfiler = new AsyncProfiler();

    public Jfr getJfr() { return jfr; }
    public Blocking getBlocking() { return blocking; }
    public Admin getAdmin() { return admin; }
    public AsyncProfiler getAsyncProfiler() { return asyncProfiler; }

    @PostConstruct
    void validate() {
        if (!admin.isLocalOnly() && (admin.getToken() == null || admin.getToken().isBlank())) {
            throw new IllegalStateException(
                    "instrumentation.admin.token must be set when instrumentation.admin.local-only=false"
            );
        }
    }

    public static class Jfr {
        private boolean enabled = true;
        private Path outputDir = Path.of("var", "profiling", "jfr");
        private Duration retention = Duration.ofHours(2);
        private long maxSizeMb = 512;
        private int stackDepth = 256;
        private Duration snapshotWindow = Duration.ofMinutes(2);
        private Duration executionSamplePeriod = Duration.ofMillis(20);

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public Path getOutputDir() { return outputDir; }
        public void setOutputDir(Path outputDir) { this.outputDir = outputDir; }
        public Duration getRetention() { return retention; }
        public void setRetention(Duration retention) { this.retention = retention; }
        public long getMaxSizeMb() { return maxSizeMb; }
        public void setMaxSizeMb(long maxSizeMb) { this.maxSizeMb = maxSizeMb; }
        public int getStackDepth() { return stackDepth; }
        public void setStackDepth(int stackDepth) { this.stackDepth = stackDepth; }
        public Duration getSnapshotWindow() { return snapshotWindow; }
        public void setSnapshotWindow(Duration snapshotWindow) { this.snapshotWindow = snapshotWindow; }
        public Duration getExecutionSamplePeriod() { return executionSamplePeriod; }
        public void setExecutionSamplePeriod(Duration executionSamplePeriod) {
            this.executionSamplePeriod = executionSamplePeriod;
        }
    }

    public static class Blocking {
        private long slowThresholdMillis = 100;
        private double stackSampleRate = 0.01;
        private Duration stackLogRateLimit = Duration.ofSeconds(30);

        public long getSlowThresholdMillis() { return slowThresholdMillis; }
        public void setSlowThresholdMillis(long slowThresholdMillis) { this.slowThresholdMillis = slowThresholdMillis; }
        public double getStackSampleRate() { return stackSampleRate; }
        public void setStackSampleRate(double stackSampleRate) { this.stackSampleRate = stackSampleRate; }
        public Duration getStackLogRateLimit() { return stackLogRateLimit; }
        public void setStackLogRateLimit(Duration stackLogRateLimit) { this.stackLogRateLimit = stackLogRateLimit; }
    }

    public static class Admin {
        private boolean localOnly = true;
        private String token = "";

        public boolean isLocalOnly() { return localOnly; }
        public void setLocalOnly(boolean localOnly) { this.localOnly = localOnly; }
        public String getToken() { return token; }
        public void setToken(String token) { this.token = token; }
    }

    public static class AsyncProfiler {
        private boolean enabled = false;
        private Path outputDir = Path.of("var", "profiling", "async-profiler");
        private String scriptPath = "scripts/run_async_profiler.sh";

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public Path getOutputDir() { return outputDir; }
        public void setOutputDir(Path outputDir) { this.outputDir = outputDir; }
        public String getScriptPath() { return scriptPath; }
        public void setScriptPath(String scriptPath) { this.scriptPath = scriptPath; }
    }
}
