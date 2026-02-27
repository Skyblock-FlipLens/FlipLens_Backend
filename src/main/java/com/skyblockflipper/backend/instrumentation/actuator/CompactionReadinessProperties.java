package com.skyblockflipper.backend.instrumentation.actuator;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "config.compaction.readiness")
public class CompactionReadinessProperties {

    private boolean enabled = true;
    private final Api api = new Api();
    private final DbPool dbPool = new DbPool();
    private final Pipeline pipeline = new Pipeline();

    @Getter
    @Setter
    public static class Api {
        private double maxP95HttpMs = 250.0D;
        private double maxProcessCpu = 0.70D;
        private double maxTomcatBusyRatio = 0.70D;
    }

    @Getter
    @Setter
    public static class DbPool {
        private double maxPending = 0.0D;
    }

    @Getter
    @Setter
    public static class Pipeline {
        private double maxQueueSize = 0.0D;
    }
}
