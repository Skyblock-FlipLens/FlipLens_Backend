package com.skyblockflipper.backend.instrumentation;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import jakarta.annotation.PostConstruct;

import java.nio.file.Path;
import java.time.Duration;

@Getter
@ConfigurationProperties(prefix = "instrumentation")
public class InstrumentationProperties {

    private final Jfr jfr = new Jfr();
    private final Blocking blocking = new Blocking();
    private final Admin admin = new Admin();
    private final AsyncProfiler asyncProfiler = new AsyncProfiler();

    @PostConstruct
    void validate() {
        if (!admin.isLocalOnly() && (admin.getToken() == null || admin.getToken().isBlank())) {
            throw new IllegalStateException(
                    "instrumentation.admin.token must be set when instrumentation.admin.local-only=false"
            );
        }
    }

    @Setter
    @Getter
    public static class Jfr {
        private boolean enabled = true;
        private Path outputDir = Path.of("var", "profiling", "jfr");
        private Duration retention = Duration.ofHours(2);
        private long maxSizeMb = 512;
        private int stackDepth = 256;
        private Duration snapshotWindow = Duration.ofMinutes(2);
        private Duration executionSamplePeriod = Duration.ofMillis(20);

    }

    @Setter
    @Getter
    public static class Blocking {
        private long slowThresholdMillis = 100;
        private double stackSampleRate = 0.01;
        private Duration stackLogRateLimit = Duration.ofSeconds(30);

    }

    @Setter
    @Getter
    public static class Admin {
        private boolean localOnly = true;
        private String token = "";
    }

    @Setter
    @Getter
    public static class AsyncProfiler {
        private boolean enabled = false;
        private Path outputDir = Path.of("var", "profiling", "async-profiler");
        private String scriptPath = "scripts/run_async_profiler.sh";

    }
}
